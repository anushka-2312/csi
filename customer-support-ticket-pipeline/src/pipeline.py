"""
pipeline.py
-----------
Customer Support Ticket Resolution Pipeline — Bronze / Silver / Gold.

This is a pandas implementation of the pipeline so it can be run and verified
without a Spark cluster. The equivalent PySpark/Databricks notebook
(notebooks/customer_support_pipeline.py) implements the exact same
transformations using the DataFrame API for submission/execution on Databricks.

Business rules implemented (see docs/business_rules.md for the full spec):
    R1  Resolution time text ("Xh Xm Xs") -> decimal minutes
    R2  Rounding: seconds >= 30 round up to the next whole minute
    R3  Successful resolution = status == "Resolved" AND resolved_minutes > 15
    R4  Scope filter: only agents whose team_lead_id is in TL01-TL08
    R5  Drop rows with null/missing ticket_id, agent_id, status, or resolution_time
    R6  Day 2 carry-over rule: agents who succeeded on Day 1 are excluded from Day 2
"""

import os
import re
import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
os.makedirs(GOLD_DIR, exist_ok=True)

IN_SCOPE_LEADS = {f"TL0{i}" for i in range(1, 9)}
TIME_RE = re.compile(r"(\d+)h\s*(\d+)m\s*(\d+)s")


# ---------------------------------------------------------------------------
# R1 / R2 — resolution-time parsing + rounding
# ---------------------------------------------------------------------------
def parse_resolution_time(value):
    """'0h 22m 45s' -> 23.0  (decimal minutes, rounded per R2). Returns None if unparseable."""
    if not isinstance(value, str) or not value.strip():
        return None
    m = TIME_RE.match(value.strip())
    if not m:
        return None
    hours, minutes, seconds = (int(x) for x in m.groups())
    total_minutes = hours * 60 + minutes
    if seconds >= 30:
        total_minutes += 1
    return float(total_minutes)


# ---------------------------------------------------------------------------
# BRONZE — raw ingest, tag with source day, drop hard-invalid rows (R5)
# ---------------------------------------------------------------------------
def bronze_layer():
    day1 = pd.read_csv(os.path.join(RAW_DIR, "day1_tickets.csv"))
    day2 = pd.read_csv(os.path.join(RAW_DIR, "day2_tickets.csv"))
    day1["day"] = "Day1"
    day2["day"] = "Day2"

    bronze = pd.concat([day1, day2], ignore_index=True)

    before = len(bronze)
    bronze = bronze.dropna(subset=["ticket_id", "agent_id", "status", "resolution_time"])
    bronze = bronze[(bronze["agent_id"].astype(str).str.strip() != "") &
                     (bronze["status"].astype(str).str.strip() != "") &
                     (bronze["resolution_time"].astype(str).str.strip() != "")]
    dropped = before - len(bronze)
    print(f"[BRONZE] {before} raw rows -> {len(bronze)} after dropping {dropped} null/blank rows")
    return bronze


# ---------------------------------------------------------------------------
# SILVER — parse time, join agent profiles, apply scope filter + quality threshold
# ---------------------------------------------------------------------------
def silver_layer(bronze):
    profiles = pd.read_csv(os.path.join(RAW_DIR, "agent_profiles.csv"))

    silver = bronze.copy()
    silver["resolved_minutes"] = silver["resolution_time"].apply(parse_resolution_time)
    silver = silver.dropna(subset=["resolved_minutes"])

    # R4 — enrich with agent profile, then scope filter (inner join drops out-of-scope agents
    # and any ticket referencing an agent_id not present in the profile table)
    silver = silver.merge(profiles, on="agent_id", how="inner")
    before_scope = len(silver)
    silver = silver[silver["team_lead_id"].isin(IN_SCOPE_LEADS)]
    print(f"[SILVER] {before_scope} rows after profile join -> {len(silver)} after TL01-TL08 scope filter")

    # R3 — quality threshold: Resolved AND > 15 minutes
    silver["is_successful"] = (silver["status"].str.strip().str.lower() == "resolved") & \
                               (silver["resolved_minutes"] > 15)

    return silver


# ---------------------------------------------------------------------------
# R6 — Day 2 carry-over exclusion, applied between Silver and Gold
# ---------------------------------------------------------------------------
def apply_carryover_rule(silver):
    day1_success_agents = set(
        silver.loc[(silver["day"] == "Day1") & (silver["is_successful"]), "agent_id"]
    )
    day2 = silver[silver["day"] == "Day2"]
    excluded = day2[day2["agent_id"].isin(day1_success_agents)]
    day2_kept = day2[~day2["agent_id"].isin(day1_success_agents)]

    print(f"[SILVER] Day2 carry-over rule: {excluded['agent_id'].nunique()} agent(s) / "
          f"{len(excluded)} row(s) excluded (already succeeded Day1)")

    day1 = silver[silver["day"] == "Day1"]
    combined = pd.concat([day1, day2_kept], ignore_index=True)
    return combined, sorted(day1_success_agents)


# ---------------------------------------------------------------------------
# GOLD — the four leadership KPI tables
# ---------------------------------------------------------------------------
def gold_layer(combined, day1_success_agents):
    successful = combined[combined["is_successful"]]

    # Q1 — resolution counts per Team Lead
    q1 = (successful.groupby("team_lead_id")
                     .agg(total_resolved=("ticket_id", "count"),
                          contributing_agents=("agent_id", "nunique"))
                     .reset_index()
                     .sort_values("team_lead_id"))

    # Q2 — per-agent, per-day performance
    q2 = (successful.groupby(["agent_id", "agent_name", "team_lead_id", "day"])
                     .agg(resolved_tickets=("ticket_id", "count"))
                     .reset_index()
                     .pivot_table(index=["agent_id", "agent_name", "team_lead_id"],
                                  columns="day", values="resolved_tickets", fill_value=0)
                     .reset_index())
    for col in ("Day1", "Day2"):
        if col not in q2.columns:
            q2[col] = 0
    q2 = q2.rename(columns={"Day1": "day1_resolved", "Day2": "day2_resolved"})

    # Q3 — compliance rate per Team Lead (successful / all valid resolved-status attempts)
    attempted = combined[combined["status"].str.strip().str.lower() == "resolved"]
    q3 = (attempted.groupby("team_lead_id")
                    .agg(attempted_resolutions=("ticket_id", "count"))
                    .reset_index()
                    .merge(q1[["team_lead_id", "total_resolved"]], on="team_lead_id", how="left")
                    .fillna(0))
    q3["total_resolved"] = q3["total_resolved"].astype(int)
    q3["compliance_rate_pct"] = (q3["total_resolved"] / q3["attempted_resolutions"] * 100).round(1)

    # Q4 — Day1 successful agents who also show up (and are excluded) on Day 2
    profiles = pd.read_csv(os.path.join(RAW_DIR, "agent_profiles.csv"))
    q4 = profiles[profiles["agent_id"].isin(day1_success_agents)][
        ["agent_id", "agent_name", "team_lead_id"]
    ].sort_values("agent_id").reset_index(drop=True)
    q4["day1_status"] = "Succeeded"
    q4["day2_records"] = "Excluded (carry-over rule)"

    return q1, q2, q3, q4


def run():
    bronze = bronze_layer()
    silver = silver_layer(bronze)
    combined, day1_success_agents = apply_carryover_rule(silver)
    q1, q2, q3, q4 = gold_layer(combined, day1_success_agents)

    q1.to_csv(os.path.join(GOLD_DIR, "q1_team_lead_resolution_rates.csv"), index=False)
    q2.to_csv(os.path.join(GOLD_DIR, "q2_per_agent_day1_vs_day2.csv"), index=False)
    q3.to_csv(os.path.join(GOLD_DIR, "q3_compliance_by_team_lead.csv"), index=False)
    q4.to_csv(os.path.join(GOLD_DIR, "q4_day2_carryover_agents.csv"), index=False)

    print("\n" + "=" * 55)
    print("  PIPELINE EXECUTION SUMMARY")
    print("=" * 55)
    print(f"  Qualifying resolved tickets : {int((combined['is_successful']).sum())}")
    print(f"  Contributing agents         : {combined.loc[combined['is_successful'], 'agent_id'].nunique()}")
    print(f"  Team Leads in scope         : {combined['team_lead_id'].nunique()}")
    print(f"  Day 2 carry-over agents     : {len(day1_success_agents)}")
    print("=" * 55)
    print("\nGold outputs written to data/gold/")


if __name__ == "__main__":
    run()
