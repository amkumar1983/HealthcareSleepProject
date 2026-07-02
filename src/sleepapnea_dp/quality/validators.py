"""
Lightweight, dependency-free data quality framework (rule -> DataFrame split into
pass/fail). Swap this for Great Expectations or Databricks DLT expectations if the team
wants a heavier framework — the rule definitions here are deliberately simple so the
*pattern* (quarantine bad rows, never silently drop or silently pass) is the takeaway.
"""
from dataclasses import dataclass
from typing import Callable, List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class Rule:
    name: str
    condition: Callable[[DataFrame], Column]  # returns a boolean Column; True = passes
    severity: str = "error"  # "error" -> quarantine, "warn" -> flag but keep


DEVICE_SESSION_RULES: List[Rule] = [
    Rule("non_null_patient_id", lambda df: F.col("patient_id").isNotNull()),
    Rule("non_null_device_id", lambda df: F.col("device_id").isNotNull()),
    Rule(
        "session_end_after_start",
        lambda df: F.to_timestamp("session_end_utc") > F.to_timestamp("session_start_utc"),
    ),
    Rule("ahi_non_negative", lambda df: (F.col("ahi") >= 0) | F.col("ahi").isNull()),
    Rule(
        "spo2_in_range",
        lambda df: F.col("avg_spo2_pct").between(50, 100) | F.col("avg_spo2_pct").isNull(),
        severity="warn",
    ),
    Rule(
        "duration_plausible",
        lambda df: F.col("duration_minutes").between(1, 1440) | F.col("duration_minutes").isNull(),
    ),
]


def apply_rules(df: DataFrame, rules: List[Rule]) -> DataFrame:
    """Adds one boolean column per rule plus `_dq_passed` (AND of all 'error' rules)."""
    error_conditions = []
    for rule in rules:
        col_name = f"_dq_{rule.name}"
        df = df.withColumn(col_name, rule.condition(df))
        if rule.severity == "error":
            error_conditions.append(F.col(col_name))

    overall = error_conditions[0]
    for c in error_conditions[1:]:
        overall = overall & c
    return df.withColumn("_dq_passed", overall)


def split_clean_and_quarantine(df: DataFrame, rules: List[Rule]) -> Tuple[DataFrame, DataFrame]:
    """Returns (clean_df, quarantined_df). Quarantined rows retain all _dq_* flag columns
    so engineers can see exactly why a record was rejected — required for audit/debugging
    in a clinical context, rather than a generic 'bad row' bucket."""
    checked = apply_rules(df, rules)
    clean = checked.filter(F.col("_dq_passed")).drop(*[f"_dq_{r.name}" for r in rules], "_dq_passed")
    quarantined = checked.filter(~F.col("_dq_passed"))
    return clean, quarantined
