"""
PHI pseudonymization utilities. Patient identifiers are salted-hashed before they enter
Silver/Gold analytical tables. The salt is a secret (Key Vault-backed), never hardcoded.
The original patient_id <-> pseudonym mapping is kept ONLY in a restricted-access table
(see transformation/identity_vault.py) with its own tighter Unity Catalog grants.
"""
import hashlib

from pyspark.sql import Column
from pyspark.sql import functions as F


def pseudonymize_column(col: Column, salt: str) -> Column:
    """Vectorized (Spark-native) salted SHA-256 hash — safe to run at scale, no Python UDF."""
    return F.sha2(F.concat(col.cast("string"), F.lit(salt)), 256)


def pseudonymize_value(value: str, salt: str) -> str:
    """Single-value helper (e.g., for tests or building the identity vault) matching the
    exact same algorithm as pseudonymize_column so values are joinable."""
    return hashlib.sha256(f"{value}{salt}".encode("utf-8")).hexdigest()
