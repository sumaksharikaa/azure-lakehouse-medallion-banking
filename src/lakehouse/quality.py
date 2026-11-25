"""
Declarative data-quality framework.

Rules come from the YAML config. Each rule adds its name to an array column
`_dq_errors` when a row fails it, so one pass evaluates every rule and every
failure reason is kept. Rows with an empty array are valid; the rest go to
quarantine with their reasons attached.

A null value passes value-level rules (regex, accepted_values, range,
foreign_key) so that a missing value is reported once, by its not_null rule,
instead of several times.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DQ_ERRORS = "_dq_errors"


def _rule_failed(rule: dict[str, Any]) -> Column:
    rtype = rule["type"]
    if rtype == "expression":
        return ~F.coalesce(F.expr(rule["expr"]), F.lit(False))

    c = F.col(rule["column"])
    if rtype == "not_null":
        return c.isNull() | (F.trim(c.cast("string")) == "")
    if rtype == "is_null":
        return c.isNotNull()
    if rtype == "regex":
        return c.isNotNull() & ~c.rlike(rule["pattern"])
    if rtype == "accepted_values":
        return c.isNotNull() & ~c.isin(rule["values"])
    if rtype == "range":
        cond = F.lit(False)
        if "min" in rule:
            cond = cond | (c < F.lit(rule["min"]))
        if "max" in rule:
            cond = cond | (c > F.lit(rule["max"]))
        return c.isNotNull() & cond
    raise ValueError(f"Unsupported rule type: {rtype}")


def apply_rules(
    df: DataFrame,
    rules: list[dict[str, Any]],
    references: dict[str, DataFrame] | None = None,
) -> DataFrame:
    """Return `df` with an `_dq_errors` array<string> column listing failed rule names."""
    references = references or {}
    checks: list[Column] = []
    helper_cols: list[str] = []

    for i, rule in enumerate(rules):
        if rule["type"] == "foreign_key":
            ref_name = rule["reference"]
            if ref_name not in references:
                raise KeyError(f"Rule {rule['name']} needs reference DataFrame '{ref_name}'")
            flag = f"__fk_{i}"
            ref_keys = (
                references[ref_name]
                .select(F.col(rule["reference_column"]).alias(f"__fk_key_{i}"))
                .distinct()
                .withColumn(flag, F.lit(True))
            )
            # Reference/dimension tables are small: broadcast to avoid a shuffle.
            df = df.join(
                F.broadcast(ref_keys),
                df[rule["column"]] == ref_keys[f"__fk_key_{i}"],
                "left",
            ).drop(f"__fk_key_{i}")
            helper_cols.append(flag)
            failed = F.col(rule["column"]).isNotNull() & F.col(flag).isNull()
        else:
            failed = _rule_failed(rule)
        checks.append(F.when(failed, F.lit(rule["name"])))

    errors = (
        F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array().cast("array<string>")
    )
    return df.withColumn(DQ_ERRORS, errors).drop(*helper_cols)


def split_valid_invalid(df_checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a checked DataFrame into (valid rows, quarantined rows)."""
    is_valid = F.size(F.col(DQ_ERRORS)) == 0
    valid = df_checked.filter(is_valid).drop(DQ_ERRORS)
    invalid = df_checked.filter(~is_valid).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


def failure_summary(df_invalid: DataFrame) -> dict[str, int]:
    """Count of failures per rule, for the audit log."""
    rows = df_invalid.select(F.explode(DQ_ERRORS).alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: int(r["count"]) for r in rows}
