"""Unit tests for the synthetic CDC generator (format and determinism of national IDs)."""
import re

import generate_hr_cdc as gen

PATTERNS = {
    "SSN": r"^[0-9]{3}-[0-9]{2}-[0-9]{4}$",
    "NINO": r"^[A-Z]{2}[0-9]{6}[A-D]$",
    "PAN": r"^[A-Z]{5}[0-9]{4}[A-Z]$",
    "AADHAAR": r"^[0-9]{4}-[0-9]{4}-[0-9]{4}$",
}


def test_national_ids_match_contract_formats(spark):
    rows = gen.employees_base(spark, 1, 400, 42).select("country_code", "national_id_type", "national_id").collect()
    assert {r.national_id_type for r in rows} == set(PATTERNS)
    for r in rows:
        assert re.match(PATTERNS[r.national_id_type], r.national_id), r


def test_generator_is_deterministic(spark):
    a = gen.employees_base(spark, 1, 50, 42).select("emplid", "national_id", "annual_rt").collect()
    b = gen.employees_base(spark, 1, 50, 42).select("emplid", "national_id", "annual_rt").collect()
    assert a == b


def test_department_skew_is_present(spark):
    df = gen.employees_base(spark, 1, 2000, 42)
    share = df.where("dept_id = 'D001'").count() / 2000
    assert 0.35 < share < 0.55
