"""
generate_sample_data.py
------------------------
Creates the three raw source files described in the problem statement:

    data/raw/agent_profiles.csv   - agent -> name / role / team_lead mapping
    data/raw/day1_tickets.csv     - Day 1 ticket log (agents report to TL01-TL08 + a couple out of scope)
    data/raw/day2_tickets.csv     - Day 2 ticket log

Deliberately injects the messiness the pipeline is supposed to clean up:
    - a few null / missing resolution_time or status values
    - some tickets with status "Pending" / "Escalated" (not resolved)
    - a couple of agents belonging to team leads outside TL01-TL08 (out of scope)
    - resolution times that straddle the 15-minute threshold and the 30-second rounding boundary
    - some Day 1 "successful" agents deliberately reappearing on Day 2, to exercise the
      carry-over exclusion rule
"""

import random
import csv
import os

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

IN_SCOPE_LEADS = [f"TL0{i}" for i in range(1, 9)]          # TL01-TL08
OUT_OF_SCOPE_LEADS = ["TL09", "TL10"]

ROLES = ["Support Agent", "Senior Support Agent"]
FIRST_NAMES = ["Riya", "Arjun", "Sneha", "Kunal", "Priya", "Rahul", "Aditi", "Vikram",
               "Neha", "Sameer", "Pooja", "Manish", "Kavya", "Rohit", "Ishaan", "Divya",
               "Tarun", "Meera", "Yash", "Simran", "Aman", "Nisha", "Karan", "Ritu",
               "Varun", "Anjali", "Deepak", "Shreya", "Gaurav", "Payal", "Ankit", "Swati",
               "Harsh", "Komal", "Nikhil", "Bhavna", "Suresh", "Alok", "Preeti", "Vivek",
               "Sonal", "Ravi"]
LAST_NAMES = ["Sharma", "Verma", "Patel", "Iyer", "Reddy", "Nair", "Gupta", "Singh",
              "Rao", "Mehta"]

def make_agent_profiles():
    rows = []
    agent_num = 1
    all_leads = IN_SCOPE_LEADS + OUT_OF_SCOPE_LEADS
    for lead in all_leads:
        n_agents = random.randint(4, 6) if lead in IN_SCOPE_LEADS else random.randint(1, 2)
        for _ in range(n_agents):
            agent_id = f"A{agent_num:03d}"
            name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
            role = random.choice(ROLES)
            rows.append([agent_id, name, role, lead])
            agent_num += 1
    return rows

def random_resolution_time(force_bucket=None):
    """Returns a string like '0h 22m 45s'. force_bucket in {'short','borderline','long', None}."""
    if force_bucket == "short":          # clearly <=15 min
        m, s = random.randint(1, 14), random.randint(0, 59)
        h = 0
    elif force_bucket == "borderline":   # straddles the 15-min / 30s rounding edge
        options = [(0, 14, 20), (0, 15, 0), (0, 14, 45), (0, 15, 29), (0, 14, 30)]
        h, m, s = random.choice(options)
    else:                                # clearly > 15 min
        h = 0 if random.random() < 0.85 else 1
        m = random.randint(15, 59) if h == 0 else random.randint(0, 59)
        s = random.randint(0, 59)
        if h == 0 and m == 15 and s == 0:
            m = 16
    return f"{h}h {m:02d}m {s:02d}s"

def make_tickets(day_label, agent_pool, n_tickets, carryover_agents=None):
    rows = []
    ticket_num = 1 if day_label == "Day1" else 2000
    statuses = ["Resolved", "Resolved", "Resolved", "Pending", "Escalated"]

    for _ in range(n_tickets):
        agent_id = random.choice(agent_pool)
        status = random.choice(statuses)
        bucket = random.choices(["long", "short", "borderline"], weights=[0.55, 0.3, 0.15])[0]
        res_time = random_resolution_time(bucket)

        # inject occasional missing/bad data
        if random.random() < 0.03:
            res_time = ""
        if random.random() < 0.02:
            status = ""
        if random.random() < 0.015:
            agent_id = ""

        ticket_id = f"{day_label[-1]}-T{ticket_num:04d}"
        rows.append([ticket_id, agent_id, status, res_time])
        ticket_num += 1

    # Force a handful of carry-over cases: agents who succeeded Day 1 also appear on Day 2
    if carryover_agents:
        for agent_id in carryover_agents:
            rows.append([f"{day_label[-1]}-T{ticket_num:04d}", agent_id, "Resolved",
                         random_resolution_time("long")])
            ticket_num += 1

    return rows

def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

if __name__ == "__main__":
    profiles = make_agent_profiles()
    write_csv(os.path.join(OUT_DIR, "agent_profiles.csv"),
              ["agent_id", "agent_name", "role", "team_lead_id"], profiles)

    in_scope_agents = [r[0] for r in profiles if r[3] in IN_SCOPE_LEADS]
    out_scope_agents = [r[0] for r in profiles if r[3] not in IN_SCOPE_LEADS]
    all_agents_pool = in_scope_agents + out_scope_agents  # tickets can reference out-of-scope agents too

    day1_rows = make_tickets("Day1", all_agents_pool, n_tickets=230)
    write_csv(os.path.join(OUT_DIR, "day1_tickets.csv"),
              ["ticket_id", "agent_id", "status", "resolution_time"], day1_rows)

    # Pick a few agents that succeeded on Day 1 (status Resolved, time > 15m) to reuse for carry-over test
    day1_success_agents = list({r[1] for r in day1_rows if r[2] == "Resolved" and r[1] in in_scope_agents})
    carryover_sample = random.sample(day1_success_agents, k=min(5, len(day1_success_agents)))

    day2_rows = make_tickets("Day2", all_agents_pool, n_tickets=180,
                              carryover_agents=carryover_sample)
    write_csv(os.path.join(OUT_DIR, "day2_tickets.csv"),
              ["ticket_id", "agent_id", "status", "resolution_time"], day2_rows)

    print(f"agent_profiles.csv : {len(profiles)} rows  "
          f"({len(in_scope_agents)} in-scope, {len(out_scope_agents)} out-of-scope)")
    print(f"day1_tickets.csv   : {len(day1_rows)} rows")
    print(f"day2_tickets.csv   : {len(day2_rows)} rows")
    print(f"Forced carry-over agents (succeeded Day1, also present Day2): {carryover_sample}")
