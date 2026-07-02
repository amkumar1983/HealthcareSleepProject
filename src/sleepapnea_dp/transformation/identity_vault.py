"""
Identity vault: the ONLY place the reversible patient_id <-> patient_key mapping is stored.
This table must live in a separate Unity Catalog schema with much tighter grants (e.g., only
a 'phi_admin' group), separate from the Silver/Gold analytical schemas that the rest of the
org (analysts, data scientists) can query. Re-identification should require an explicit,
audited join against this table — never a default-accessible column.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sleepapnea_dp.utils.phi import pseudonymize_column


def build_identity_vault(raw_df: DataFrame, salt: str) -> DataFrame:
    return (
        raw_df.select("patient_id")
        .distinct()
        .withColumn("patient_key", pseudonymize_column(F.col("patient_id"), salt))
        .withColumn("_created_at_utc", F.current_timestamp())
    )
