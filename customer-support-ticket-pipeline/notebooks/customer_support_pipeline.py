

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Widgets & Configuration
# MAGIC Parameterised so this notebook can be triggered from a Databricks Job without editing source.

# COMMAND ----------

dbutils.widgets.text("adls_base_path", "abfss://landing@<storage_account>.dfs.core.windows.net/customer_support")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "customer_support_review")

ADLS_BASE = dbutils.widgets.get("adls_base_path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

DAY1_PATH = f"{ADLS_BASE}/day1_tickets.csv"
DAY2_PATH = f"{ADLS_BASE}/day2_tickets.csv"
PROFILES_PATH = f"{ADLS_BASE}/agent_profiles.csv"

IN_SCOPE_LEADS = [f"TL0{i}" for i in range(1, 9)]  # TL01-TL08

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Day1 source     : {DAY1_PATH}")
print(f"Day2 source     : {DAY2_PATH}")
print(f"Profiles source : {PROFILES_PATH}")
print(f"Gold target      : {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bronze Layer — Ingest
# MAGIC Read both day files, tag each row with its source day, and drop hard-invalid rows
# MAGIC (null/blank `ticket_id`, `agent_id`, `status`, or `resolution_time`).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def read_ticket_csv(path, day_label):
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(path))
    return df.withColumn("day", F.lit(day_label))

day1_raw = read_ticket_csv(DAY1_PATH, "Day1")
day2_raw = read_ticket_csv(DAY2_PATH, "Day2")
bronze = day1_raw.unionByName(day2_raw)

bronze_count_before = bronze.count()

# R5 - drop null / blank required fields
required_cols = ["ticket_id", "agent_id", "status", "resolution_time"]
bronze_clean = bronze.dropna(subset=required_cols)
for c in required_cols:
    bronze_clean = bronze_clean.filter(F.trim(F.col(c)) != "")

print(f"[BRONZE] {bronze_count_before} raw rows -> {bronze_clean.count()} after null/blank filter")
bronze_clean.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_tickets")
display(bronze_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Silver Layer — Transform
# MAGIC - **R1/R2**: parse `"Xh Xm Xs"` into decimal minutes, rounding up when seconds >= 30
# MAGIC - **R4**: join agent profiles, keep only agents under TL01-TL08 (scope filter)
# MAGIC - **R3**: flag `is_successful` = status is Resolved AND resolved_minutes > 15

# COMMAND ----------

import re

def parse_resolution_time(value):
    """'0h 22m 45s' -> 23.0 decimal minutes, applying the 30-second round-up rule."""
    if value is None:
        return None
    match = re.match(r"(\d+)h\s*(\d+)m\s*(\d+)s", value.strip())
    if not match:
        return None
    hours, minutes, seconds = (int(x) for x in match.groups())
    total = hours * 60 + minutes
    if seconds >= 30:
        total += 1
    return float(total)

parse_time_udf = F.udf(parse_resolution_time, DoubleType())

profiles = (spark.read.option("header", True).option("inferSchema", True)
            .csv(PROFILES_PATH))
profiles.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.agent_profiles")

silver = bronze_clean.withColumn("resolved_minutes", parse_time_udf(F.col("resolution_time")))
silver = silver.filter(F.col("resolved_minutes").isNotNull())

# R4 - inner join enriches with profile data AND enforces scope (drops agents with no profile match)
before_scope = silver.count()
silver = silver.join(profiles, on="agent_id", how="inner")
silver = silver.filter(F.col("team_lead_id").isin(IN_SCOPE_LEADS))
print(f"[SILVER] {before_scope} rows after time parsing -> {silver.count()} after TL01-TL08 scope filter")

# R3 - quality threshold
silver = silver.withColumn(
    "is_successful",
    (F.lower(F.trim(F.col("status"))) == "resolved") & (F.col("resolved_minutes") > 15)
)

silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_tickets")
display(silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Day 2 Carry-over Rule (R6)
# MAGIC Agents who already succeeded on Day 1 are excluded from Day 2 results, so they are not
# MAGIC double-counted.

# COMMAND ----------

day1_success_agents = (silver.filter((F.col("day") == "Day1") & (F.col("is_successful")))
                        .select("agent_id").distinct())

day2 = silver.filter(F.col("day") == "Day2")
day2_excluded = day2.join(day1_success_agents, on="agent_id", how="left_semi")
day2_kept = day2.join(day1_success_agents, on="agent_id", how="left_anti")

print(f"[SILVER] Day2 carry-over rule: {day2_excluded.select('agent_id').distinct().count()} "
      f"agent(s) / {day2_excluded.count()} row(s) excluded (already succeeded Day1)")

day1 = silver.filter(F.col("day") == "Day1")
combined = day1.unionByName(day2_kept)
combined.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_combined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Gold Layer — Leadership KPIs (Q1-Q4)

# COMMAND ----------

successful = combined.filter(F.col("is_successful"))

# Q1 - resolution rates per Team Lead
gold_q1 = (successful.groupBy("team_lead_id")
           .agg(F.count("ticket_id").alias("total_resolved"),
                F.countDistinct("agent_id").alias("contributing_agents"))
           .orderBy("team_lead_id"))
gold_q1.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_q1_team_lead_rates")

# Q2 - per-agent, per-day performance
gold_q2 = (successful.groupBy("agent_id", "agent_name", "team_lead_id")
           .pivot("day", ["Day1", "Day2"])
           .agg(F.count("ticket_id"))
           .na.fill(0)
           .withColumnRenamed("Day1", "day1_resolved")
           .withColumnRenamed("Day2", "day2_resolved")
           .orderBy("agent_id"))
gold_q2.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_q2_agent_daily_performance")

# Q3 - compliance rate per Team Lead (successful / all Resolved-status attempts)
attempted = combined.filter(F.lower(F.trim(F.col("status"))) == "resolved")
gold_q3 = (attempted.groupBy("team_lead_id")
           .agg(F.count("ticket_id").alias("attempted_resolutions"))
           .join(gold_q1.select("team_lead_id", "total_resolved"), on="team_lead_id", how="left")
           .na.fill(0)
           .withColumn("compliance_rate_pct",
                       F.round(F.col("total_resolved") / F.col("attempted_resolutions") * 100, 1))
           .orderBy("team_lead_id"))
gold_q3.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_q3_compliance_by_team_lead")

# Q4 - Day1 successful agents excluded from Day2
gold_q4 = (day1_success_agents
           .join(profiles, on="agent_id", how="inner")
           .select("agent_id", "agent_name", "team_lead_id")
           .withColumn("day1_status", F.lit("Succeeded"))
           .withColumn("day2_records", F.lit("Excluded (carry-over rule)"))
           .orderBy("agent_id"))
gold_q4.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_q4_day2_carryover_agents")

display(gold_q1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

total_qualifying = successful.count()
unique_agents_q = successful.select("agent_id").distinct().count()
unique_tls_q = successful.select("team_lead_id").distinct().count()
carryover_count = gold_q4.count()

print("=" * 55)
print("  PIPELINE EXECUTION SUMMARY")
print("=" * 55)
print(f"  Qualifying resolved tickets  : {total_qualifying}")
print(f"  Contributing agents          : {unique_agents_q}")
print(f"  Team Leads in scope          : {unique_tls_q}  (TL01-TL08)")
print(f"  Day 2 carry-over agents      : {carryover_count}")
print("=" * 55)
