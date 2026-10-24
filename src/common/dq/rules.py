"""
Load and validate data-quality rule files (YAML "data contracts").

A rule file describes one quality GATE (e.g. hr_gold) and the rules for each table in it.
Each rule has:
    name          unique, readable name (appears in results and alerts)
    type          not_null | unique | in_set | between | expression | foreign_key
    severity      error (blocks promotion) | warn (recorded, never blocks)
    max_fail_pct  optional tolerance: % of rows allowed to fail before the rule fails (default 0)
    where         optional SQL filter: the rule only applies to matching rows (e.g. "is_current")
plus the arguments of its type (column / columns / values / min / max / expression / ref_*).
"""
import os

import yaml

REQUIRED_ARGS = {
    "not_null": ["column"],
    "unique": ["columns"],
    "in_set": ["column", "values"],
    "between": ["column"],
    "expression": ["expression"],
    "foreign_key": ["column", "ref_table", "ref_column"],
}
ROW_LEVEL = {"not_null", "in_set", "between", "expression"}   # can identify the failing rows


def rules_path(gate: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules", f"{gate}.yml")


def load_gate(gate: str) -> dict:
    """Read rules/<gate>.yml and check every rule is well-formed (fail fast on typos)."""
    with open(rules_path(gate)) as f:
        spec = yaml.safe_load(f)
    names = set()
    for table, t in spec["tables"].items():
        for r in t["rules"]:
            assert r["type"] in REQUIRED_ARGS, f"{gate}.{table}.{r.get('name')}: unknown type {r['type']}"
            for arg in REQUIRED_ARGS[r["type"]]:
                assert arg in r, f"{gate}.{table}.{r['name']}: missing '{arg}'"
            assert r.get("severity", "error") in ("error", "warn"), f"{r['name']}: severity must be error|warn"
            assert r["name"] not in names, f"duplicate rule name {r['name']}"
            names.add(r["name"])
            r.setdefault("severity", "error")
            r.setdefault("max_fail_pct", 0.0)
    return spec


def failing_rows_condition(rule: dict) -> str:
    """SQL condition that is TRUE for rows breaking a row-level rule (used for quarantine)."""
    t = rule["type"]
    if t == "not_null":
        cond = f"{rule['column']} IS NULL"
    elif t == "in_set":
        vals = ", ".join(f"'{v}'" for v in rule["values"])
        cond = f"{rule['column']} IS NOT NULL AND {rule['column']} NOT IN ({vals})"
    elif t == "between":
        parts = []
        if "min" in rule:
            parts.append(f"{rule['column']} < {rule['min']}")
        if "max" in rule:
            parts.append(f"{rule['column']} > {rule['max']}")
        cond = " OR ".join(parts) or "FALSE"
    elif t == "expression":
        cond = f"NOT coalesce(({rule['expression']}), false)"
    else:
        raise ValueError(f"{t} is not a row-level rule")
    return f"({rule['where']}) AND ({cond})" if rule.get("where") else f"({cond})"
