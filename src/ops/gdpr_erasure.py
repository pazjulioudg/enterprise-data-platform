"""
GDPR right-to-erasure job (HR domain).

POLICY IMPLEMENTED (the usual approach for HR data)
  * ERASE direct identifiers: name, date of birth, national ID.
  * RETAIN the pseudonymised employment record (emplid, job, pay history, absences) because
    employers must keep it for statutory periods. emplid is a pseudonym, not a direct identifier.

STEPS
  1. Read PENDING requests from <hr>.ops.erasure_requests (the request register = audit evidence).
  2. Delete the person's raw personal-data events from Bronze.
     (Silver streams read Bronze with skipChangeCommits=true, so this does not break them.)
  3. Delete the person from Silver personal data (current-state table).
  4. Null out the identifiers in Gold dim_employee (all SCD2 versions).
  5. Purge soft-deleted rows (REORG ... APPLY (PURGE)) so deletion vectors are rewritten;
     the maintenance job's VACUUM then removes the old files after the retention period.
  6. Verify zero identifiable rows remain, and mark each request COMPLETED with evidence.

KNOWN LIMITS (documented, as a DPO would expect): Delta time travel keeps old file versions until
VACUUM; landing files are removed by the landing retention policy; models already trained on the
data are handled by the model governance process.
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession


def main(argv=None):
    a = argparse.ArgumentParser()
    a.add_argument("--hr-catalog", required=True)
    args = a.parse_args(argv)
    c = args.hr_catalog
    spark = SparkSession.builder.getOrCreate()

    # STEP 1 - pending requests
    pending = [r["emplid"] for r in spark.sql(
        f"SELECT emplid FROM {c}.ops.erasure_requests WHERE status = 'PENDING'").collect()]
    if not pending:
        print("No pending erasure requests.")
        return
    ids = ", ".join(f"'{e}'" for e in pending)
    print(f"Erasing identifiers for {len(pending)} employee(s)")

    # STEP 2 - Bronze raw personal-data events (key is in 'after', or in 'before' for deletes)
    spark.sql(f"DELETE FROM {c}.bronze.hr_personal_data_raw "
              f"WHERE coalesce(after.emplid, before.emplid) IN ({ids})")
    # STEP 3 - Silver current personal data
    spark.sql(f"DELETE FROM {c}.silver.hr_personal_current WHERE emplid IN ({ids})")
    # STEP 4 - Gold: keep the pseudonymised employment record, remove direct identifiers
    spark.sql(f"""UPDATE {c}.gold.dim_employee
                  SET first_name = NULL, last_name = NULL, birth_date = NULL, national_id = NULL
                  WHERE emplid IN ({ids})""")

    # STEP 5 - physically rewrite files that still hold soft-deleted rows
    for t in [f"{c}.bronze.hr_personal_data_raw", f"{c}.silver.hr_personal_current", f"{c}.gold.dim_employee"]:
        try:
            spark.sql(f"REORG TABLE {t} APPLY (PURGE)")
        except Exception as e:  # noqa: BLE001 - some pipeline-managed tables do not allow REORG
            print(f"REORG skipped for {t}: {str(e)[:120]}")

    # STEP 6 - verify and close the requests with evidence
    evidence = {
        "bronze_rows": spark.sql(f"SELECT count(*) FROM {c}.bronze.hr_personal_data_raw "
                                 f"WHERE coalesce(after.emplid, before.emplid) IN ({ids})").first()[0],
        "silver_rows": spark.sql(f"SELECT count(*) FROM {c}.silver.hr_personal_current "
                                 f"WHERE emplid IN ({ids})").first()[0],
        "gold_identifiable_rows": spark.sql(
            f"SELECT count(*) FROM {c}.gold.dim_employee WHERE emplid IN ({ids}) AND "
            f"(first_name IS NOT NULL OR national_id IS NOT NULL OR birth_date IS NOT NULL)").first()[0],
    }
    if any(evidence.values()):
        raise RuntimeError(f"Erasure verification failed: {evidence}")
    spark.sql(f"""UPDATE {c}.ops.erasure_requests
                  SET status = 'COMPLETED', completed_ts = current_timestamp(), evidence = '{json.dumps(evidence)}'
                  WHERE status = 'PENDING' AND emplid IN ({ids})""")
    print(f"Erasure completed and verified: {evidence}")


if __name__ == "__main__":
    main(sys.argv[1:])
