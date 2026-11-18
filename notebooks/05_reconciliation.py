# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Reconciliation (Bronze -> Silver outcomes)
# MAGIC
# MAGIC **Purpose:** prove that every Bronze row has **exactly one** Silver outcome, for orders and transactions independently.
# MAGIC
# MAGIC ```
# MAGIC Bronze count = accepted Silver + quarantined + duplicate losers
# MAGIC ```
# MAGIC
# MAGIC **Totals alone are not proof.** Two overlapping buckets plus a missing row can produce matching totals.
# MAGIC So this notebook also tests at **row level** with the preserved Bronze `_row_id`:
# MAGIC no Bronze row is missing, no row appears in two buckets, and nothing appears that Bronze never had.
# MAGIC A sensitivity test then shows these checks really do catch a "matching totals but wrong rows" failure.
# MAGIC
# MAGIC **This notebook writes nothing to the data tables.** It fails (raises an error) if any assertion fails,
# MAGIC so in a Lakeflow Job the Gold tasks will not run on unreconciled data.
# MAGIC
# MAGIC **Outputs to save**
# MAGIC - Section 3 count table + section 5 PASS/FAIL table + section 8 verdict -> `evidence/04_silver/`
# MAGIC - Section 9 printed SQL -> `sql/02_silver_quality_checks.sql`
# MAGIC - Section 7 examples (quarantined records with reasons, duplicate losers with winners)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

import datetime

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
dbutils.widgets.text("volume", "raw_files", "Volume")
dbutils.widgets.dropdown("save_results_table", "no", ["no", "yes"], "Save results to a Delta table (reconciliation_results)?")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()
SAVE_RESULTS = dbutils.widgets.get("save_results_table") == "yes"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

RUN_AT = datetime.datetime.now(datetime.timezone.utc).isoformat()


def fq(name):
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


# Everything the checks need, per dataset. "key" is the business key.
DATASETS = {
    "orders": {
        "key": "order_id",
        "source_file": "orders.csv",
        "bronze": fq("bronze_orders"),
        "silver": fq("silver_orders"),
        "quarantine": fq("quarantine_orders"),
        "losers": fq("duplicate_losers_orders"),
    },
    "transactions": {
        "key": "txn_id",
        "source_file": "transactions.csv",
        "bronze": fq("bronze_transactions"),
        "silver": fq("silver_transactions"),
        "quarantine": fq("quarantine_transactions"),
        "losers": fq("duplicate_losers_transactions"),
    },
}

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print("Reconciliation run at (UTC):", RUN_AT)
for name, d in DATASETS.items():
    print(f"{name:13s} key={d['key']:9s} {d['bronze']} -> {d['silver']} | {d['quarantine']} | {d['losers']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. All tables must exist and hold data
# MAGIC A missing table would otherwise show up as a confusing error later.

# COMMAND ----------

missing_tables = []
for name, d in DATASETS.items():
    for role in ["bronze", "silver", "quarantine", "losers"]:
        table = d[role]
        if not spark.catalog.tableExists(table.replace("`", "")):
            missing_tables.append(table)

assert not missing_tables, f"These tables do not exist - run the earlier notebooks first: {missing_tables}"
print("All 8 tables exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row counts per outcome
# MAGIC The count invariant, with the difference shown explicitly.

# COMMAND ----------

count_rows = []
counts = {}

for name, d in DATASETS.items():
    bronze_n = spark.table(d["bronze"]).count()
    accepted_n = spark.table(d["silver"]).count()
    quarantine_n = spark.table(d["quarantine"]).count()
    losers_n = spark.table(d["losers"]).count()
    outcome_sum = accepted_n + quarantine_n + losers_n
    diff = bronze_n - outcome_sum

    counts[name] = dict(bronze=bronze_n, accepted=accepted_n, quarantine=quarantine_n,
                        losers=losers_n, outcome_sum=outcome_sum, diff=diff)
    count_rows.append((
        name, bronze_n, accepted_n, quarantine_n, losers_n, outcome_sum, diff,
        round(accepted_n / bronze_n * 100, 4) if bronze_n else None,
        round(quarantine_n / bronze_n * 100, 4) if bronze_n else None,
        round(losers_n / bronze_n * 100, 4) if bronze_n else None,
        diff == 0,
    ))

counts_df = spark.createDataFrame(
    count_rows,
    "dataset STRING, bronze_rows LONG, accepted_rows LONG, quarantine_rows LONG, duplicate_loser_rows LONG, "
    "outcome_sum LONG, bronze_minus_outcome_sum LONG, accepted_pct DOUBLE, quarantine_pct DOUBLE, "
    "duplicate_loser_pct DOUBLE, totals_match BOOLEAN"
)
display(counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Upstream cross-check: source CSV rows = Bronze rows
# MAGIC A fresh read of each CSV, independent of the Bronze notebook. Catches a Bronze load that dropped or doubled rows.

# COMMAND ----------

source_checks = {}
for name, d in DATASETS.items():
    path = f"{VOLUME_PATH}/{d['source_file']}"
    src_n = (
        spark.read.option("header", "true").option("inferSchema", "false").csv(path).count()
    )
    source_checks[name] = (src_n, counts[name]["bronze"])
    print(f"{name:13s} source CSV rows: {src_n:,}   Bronze rows: {counts[name]['bronze']:,}   "
          f"-> {'PASS' if src_n == counts[name]['bronze'] else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Row-level assertions (SQL)
# MAGIC Each assertion is a query that returns a **violation count**. The expected value is **0** for all of them.
# MAGIC The same SQL is printed in section 9 for `sql/02_silver_quality_checks.sql`.
# MAGIC
# MAGIC | Assertion | What a non-zero value means |
# MAGIC |---|---|
# MAGIC | `count_invariant` | Bronze != accepted + quarantine + losers |
# MAGIC | `bronze_row_id_not_unique` | Bronze cannot identify rows uniquely, so row-level proof is impossible |
# MAGIC | `bronze_row_missing_from_outcomes` | a source row vanished |
# MAGIC | `outcome_row_not_in_bronze` | an outcome row has no Bronze parent |
# MAGIC | `row_in_multiple_outcomes` | a source row landed in more than one bucket |
# MAGIC | `overlap_*` | the same, shown pairwise for the three bucket pairs |
# MAGIC | `outcome_null_row_id` | an outcome row lost its audit identifier |
# MAGIC | `outcome_batch_mismatch` | an outcome row came from a different Bronze snapshot |
# MAGIC | `accepted_key_not_unique` | duplicate business key in the accepted table |
# MAGIC | `loser_without_valid_winner` | a duplicate loser does not point to an accepted winner of the same key |
# MAGIC | `quarantine_without_reason` | a quarantined row has no `rejection_reason` |
# MAGIC | `accepted_txn_orphan_order` | an accepted transaction references an order that is not accepted |

# COMMAND ----------

def outcomes_cte(d):
    """All Silver outcome rows with their Bronze identity, tagged by bucket."""
    return f"""WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM {d['silver']}
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM {d['quarantine']}
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM {d['losers']}
)"""


def build_assertions(name, d):
    """Return [(assertion_name, description, sql)] for one dataset. Each SQL returns ONE number that must be 0."""
    key = d["key"]
    cte = outcomes_cte(d)
    return [
        ("count_invariant",
         "Bronze count must equal accepted + quarantine + duplicate losers",
         f"""SELECT ABS(
    (SELECT COUNT(*) FROM {d['bronze']})
  - ((SELECT COUNT(*) FROM {d['silver']}) + (SELECT COUNT(*) FROM {d['quarantine']}) + (SELECT COUNT(*) FROM {d['losers']}))
) AS violations"""),

        ("bronze_row_id_not_unique",
         "Every Bronze row must have a unique _row_id (needed for row-level proof)",
         f"SELECT COUNT(*) - COUNT(DISTINCT _row_id) AS violations FROM {d['bronze']}"),

        ("bronze_row_missing_from_outcomes",
         "Every Bronze row must appear in some outcome table",
         f"""{cte}
SELECT COUNT(*) AS violations
FROM {d['bronze']} b
LEFT ANTI JOIN outcomes o ON b._row_id = o._row_id"""),

        ("outcome_row_not_in_bronze",
         "Every outcome row must come from a Bronze row",
         f"""{cte}
SELECT COUNT(*) AS violations
FROM outcomes o
LEFT ANTI JOIN {d['bronze']} b ON b._row_id = o._row_id"""),

        ("row_in_multiple_outcomes",
         "No Bronze row may appear more than once across all outcome tables",
         f"""{cte}
SELECT COUNT(*) AS violations
FROM (SELECT _row_id FROM outcomes GROUP BY _row_id HAVING COUNT(*) > 1)"""),

        ("overlap_accepted_quarantine",
         "No row in both accepted and quarantine",
         f"""SELECT COUNT(*) AS violations
FROM {d['silver']} a JOIN {d['quarantine']} q ON a._row_id = q._row_id"""),

        ("overlap_accepted_losers",
         "No row in both accepted and duplicate losers",
         f"""SELECT COUNT(*) AS violations
FROM {d['silver']} a JOIN {d['losers']} l ON a._row_id = l._row_id"""),

        ("overlap_quarantine_losers",
         "No row in both quarantine and duplicate losers",
         f"""SELECT COUNT(*) AS violations
FROM {d['quarantine']} q JOIN {d['losers']} l ON q._row_id = l._row_id"""),

        ("outcome_null_row_id",
         "No outcome row may have a NULL _row_id",
         f"""{cte}
SELECT COUNT(*) AS violations FROM outcomes WHERE _row_id IS NULL"""),

        ("outcome_batch_mismatch",
         "Every outcome row must come from the current Bronze snapshot (_batch_id)",
         f"""{cte}
SELECT COUNT(*) AS violations
FROM outcomes
WHERE _batch_id NOT IN (SELECT DISTINCT _batch_id FROM {d['bronze']})"""),

        ("accepted_key_not_unique",
         f"Accepted table must have one row per {key}",
         f"SELECT COUNT(*) - COUNT(DISTINCT {key}) AS violations FROM {d['silver']}"),

        ("loser_without_valid_winner",
         f"Every duplicate loser must point (winner_row_id) to an accepted row with the same {key}",
         f"""SELECT COUNT(*) AS violations
FROM {d['losers']} l
LEFT JOIN {d['silver']} s ON l.winner_row_id = s._row_id
WHERE s._row_id IS NULL OR l.{key} <> s.{key}"""),

        ("quarantine_without_reason",
         "Every quarantined row must have a non-empty rejection_reason",
         f"""SELECT COUNT(*) AS violations
FROM {d['quarantine']}
WHERE rejection_reason IS NULL OR trim(rejection_reason) = ''"""),
    ]


assertions = []   # (dataset, name, description, sql)
for name, d in DATASETS.items():
    for a_name, desc, sql in build_assertions(name, d):
        assertions.append((name, a_name, desc, sql))

# Cross-dataset assertion: accepted transactions must reference accepted orders
t, o = DATASETS["transactions"], DATASETS["orders"]
assertions.append((
    "cross", "accepted_txn_orphan_order",
    "Every accepted transaction must reference an order that exists in accepted silver_orders",
    f"""SELECT COUNT(*) AS violations
FROM {t['silver']} t
LEFT ANTI JOIN {o['silver']} s ON t.order_id = s.order_id"""
))

print(f"{len(assertions)} assertions defined.")

# COMMAND ----------

# Run every assertion; expected result is 0 violations
results = []
for dataset, a_name, desc, sql in assertions:
    violations = int(spark.sql(sql).collect()[0]["violations"])
    results.append((dataset, a_name, desc, violations, violations == 0))

results_df = spark.createDataFrame(
    results, "dataset STRING, assertion STRING, description STRING, violations LONG, passed BOOLEAN"
)
display(results_df.orderBy("dataset", "assertion"))

# COMMAND ----------

# Plain-text view (easy to screenshot)
for dataset, a_name, desc, violations, passed in results:
    print(f"[{'PASS' if passed else 'FAIL'}] {dataset:13s} {a_name:36s} violations = {violations:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sensitivity test: do the row-level checks catch what totals cannot?
# MAGIC This builds a **deliberately broken copy** of the outcomes in a temp view (nothing real is changed):
# MAGIC one row is removed and a different row is duplicated. The **total stays identical**, so a count-only check would pass.
# MAGIC The row-level checks must flag it.

# COMMAND ----------

sensitivity_results = []

for name, d in DATASETS.items():
    cte = outcomes_cte(d)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW tampered_outcomes_{name} AS
    {cte},
    bounds AS (SELECT MIN(_row_id) AS lo, MAX(_row_id) AS hi FROM outcomes)
    SELECT _row_id FROM outcomes WHERE _row_id <> (SELECT lo FROM bounds)      -- drop one row
    UNION ALL
    SELECT _row_id FROM outcomes WHERE _row_id = (SELECT hi FROM bounds)       -- duplicate another row
    """)

    real_total = spark.sql(f"{cte} SELECT COUNT(*) AS n FROM outcomes").collect()[0]["n"]
    tampered_total = spark.sql(f"SELECT COUNT(*) AS n FROM tampered_outcomes_{name}").collect()[0]["n"]

    missing = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {d['bronze']} b
        LEFT ANTI JOIN tampered_outcomes_{name} o ON b._row_id = o._row_id
    """).collect()[0]["n"]

    multi = spark.sql(f"""
        SELECT COUNT(*) AS n FROM (SELECT _row_id FROM tampered_outcomes_{name} GROUP BY _row_id HAVING COUNT(*) > 1)
    """).collect()[0]["n"]

    totals_fooled = real_total == tampered_total
    caught = missing >= 1 and multi >= 1
    sensitivity_results.append((name, real_total, tampered_total, missing, multi, totals_fooled, caught))

    print(f"{name:13s} real total={real_total:,}  tampered total={tampered_total:,}  "
          f"(count-only check would {'PASS - fooled' if totals_fooled else 'FAIL'})")
    print(f"{'':13s} row-level: missing rows={missing}, rows in >1 bucket={multi}  "
          f"-> {'PASS: tampering detected' if caught else 'FAIL: tampering NOT detected'}")

display(spark.createDataFrame(
    sensitivity_results,
    "dataset STRING, real_outcome_rows LONG, tampered_outcome_rows LONG, missing_rows_detected LONG, "
    "duplicate_rows_detected LONG, count_only_check_fooled BOOLEAN, row_level_check_caught_it BOOLEAN"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Examples (evidence)
# MAGIC The guide asks for example quarantined records with reasons and example duplicate losers with their winners.

# COMMAND ----------

# Rejection reasons per dataset. A row with several reasons counts once per reason, so totals can exceed the quarantine row count.
display(spark.sql(f"""
    SELECT 'orders' AS dataset, reason AS rejection_reason, COUNT(*) AS rows_with_this_reason
    FROM {DATASETS['orders']['quarantine']}
    LATERAL VIEW explode(rejection_reasons) r AS reason
    GROUP BY reason
    UNION ALL
    SELECT 'transactions', reason, COUNT(*)
    FROM {DATASETS['transactions']['quarantine']}
    LATERAL VIEW explode(rejection_reasons) r AS reason
    GROUP BY reason
    ORDER BY dataset, rows_with_this_reason DESC
"""))

# COMMAND ----------

# Example quarantined orders (raw values + reasons)
display(spark.sql(f"""
    SELECT order_id, customer_id, order_date, quantity, unit_price, order_status, country, rejection_reason, _row_id
    FROM {DATASETS['orders']['quarantine']}
    ORDER BY rejection_count DESC
    LIMIT 15
"""))

# COMMAND ----------

# Example quarantined transactions (raw values + reasons)
display(spark.sql(f"""
    SELECT txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts, rejection_reason, _row_id
    FROM {DATASETS['transactions']['quarantine']}
    ORDER BY rejection_count DESC
    LIMIT 15
"""))

# COMMAND ----------

# Example orphan transactions (rejected because the order is not in accepted silver_orders)
display(spark.sql(f"""
    SELECT txn_id, order_id, amount, txn_status, rejection_reason, _row_id
    FROM {DATASETS['transactions']['quarantine']}
    WHERE array_contains(rejection_reasons, 'order_id_not_in_silver_orders')
    LIMIT 15
"""))

# COMMAND ----------

# Example duplicate order losers next to their winners
display(spark.sql(f"""
    SELECT l.order_id, l.duplicate_rank,
           l.order_date AS loser_order_date, w.order_date AS winner_order_date,
           l._row_id AS loser_row_id, w._row_id AS winner_row_id, l.winner_rule
    FROM {DATASETS['orders']['losers']} l
    JOIN {DATASETS['orders']['silver']} w ON l.winner_row_id = w._row_id
    ORDER BY l.order_id, l.duplicate_rank
    LIMIT 15
"""))

# COMMAND ----------

# Example duplicate transaction losers next to their winners
display(spark.sql(f"""
    SELECT l.txn_id, l.duplicate_rank,
           l.txn_ts AS loser_txn_ts, w.txn_ts AS winner_txn_ts,
           l._row_id AS loser_row_id, w._row_id AS winner_row_id, l.winner_rule
    FROM {DATASETS['transactions']['losers']} l
    JOIN {DATASETS['transactions']['silver']} w ON l.winner_row_id = w._row_id
    ORDER BY l.txn_id, l.duplicate_rank
    LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final verdict
# MAGIC Raises an error if anything failed, so a Lakeflow Job stops before Gold.

# COMMAND ----------

all_assertions_pass = all(passed for *_, passed in results)
all_source_ok = all(s == b for s, b in source_checks.values())
all_sensitivity_ok = all(caught for *_, caught in sensitivity_results)

print("=" * 72)
print("RECONCILIATION VERDICT")
print("=" * 72)
for name, c in counts.items():
    print(f"{name:13s} Bronze {c['bronze']:>10,} = accepted {c['accepted']:>10,} + quarantine {c['quarantine']:>9,} "
          f"+ losers {c['losers']:>9,}   (difference {c['diff']:,})")
print("-" * 72)
print(f"[{'PASS' if all_source_ok else 'FAIL'}] source CSV rows equal Bronze rows (both datasets)")
print(f"[{'PASS' if all_assertions_pass else 'FAIL'}] all {len(results)} row-level assertions have 0 violations")
print(f"[{'PASS' if all_sensitivity_ok else 'FAIL'}] row-level checks detect a 'matching totals, wrong rows' failure")
print("=" * 72)

OVERALL_PASS = all_assertions_pass and all_source_ok and all_sensitivity_ok
print("OVERALL:", "RECONCILED - safe to build Gold" if OVERALL_PASS else "NOT RECONCILED - do not build Gold")

# COMMAND ----------

# Optional: keep the results as a Delta table (off by default to avoid creating extra objects)
if SAVE_RESULTS:
    out = results_df.selectExpr(f"'{RUN_AT}' AS run_at_utc", "*")
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("reconciliation_results"))
    print("Saved:", fq("reconciliation_results"))
else:
    print("Skipped saving results table (save_results_table = no).")

# COMMAND ----------

assert OVERALL_PASS, "Reconciliation failed - see the FAIL lines above. Do not build Gold until this passes."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. SQL for `sql/02_silver_quality_checks.sql`
# MAGIC Copy the printed text into that file. Every statement should return `violations = 0`.

# COMMAND ----------

sql_lines = [
    "-- sql/02_silver_quality_checks.sql",
    f"-- Generated by notebooks/05_reconciliation.py on {RUN_AT}",
    "-- Every statement returns a single number `violations`. The expected value is 0.",
    "",
]
for dataset, a_name, desc, sql in assertions:
    sql_lines.append(f"-- [{dataset}] {a_name}: {desc}")
    sql_lines.append(sql.strip() + ";")
    sql_lines.append("")

print("\n".join(sql_lines))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. README text: reconciliation result
# MAGIC Built from this run's real numbers.

# COMMAND ----------

lines = [
    "## Reconciliation result",
    "",
    f"Run at (UTC): {RUN_AT}",
    "",
    "**Invariant (per dataset):** Bronze count = accepted Silver + quarantined + duplicate losers.",
    "",
    "| Dataset | Bronze | Accepted | Quarantined | Duplicate losers | Outcome sum | Difference |",
    "|---|---|---|---|---|---|---|",
]
for name, c in counts.items():
    lines.append(
        f"| {name} | {c['bronze']:,} | {c['accepted']:,} | {c['quarantine']:,} | {c['losers']:,} | {c['outcome_sum']:,} | {c['diff']:,} |"
    )
lines += [
    "",
    "**Overlap / missing-row proof:** performed at row level using the Bronze `_row_id` "
    f"({len(results)} SQL assertions, all expected to return 0 violations; see `sql/02_silver_quality_checks.sql`).",
    "A sensitivity test removed one row and duplicated another in a temporary copy of the outcomes: totals stayed equal "
    "while the row-level checks detected both problems.",
    "",
    f"**Result:** {'PASS' if OVERALL_PASS else 'FAIL'}",
]
print("\n".join(lines))

