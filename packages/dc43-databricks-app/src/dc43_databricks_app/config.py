"""Runtime configuration for the dc43 Databricks App.

Three contract sources, resolved in order:

1. ``DC43_CONTRACTS_URL``  -> remote dc43 service backend over HTTP
   (optionally ``DC43_CONTRACTS_TOKEN`` for bearer auth)
2. ``DC43_CONTRACT_PATH``  -> :class:`FSContractStore` on a UC volume / DBFS / local path
3. neither                 -> bundled demo contracts

System-table panels go live when ``DATABRICKS_WAREHOUSE_ID`` is set (bound as
an app resource in ``app.yaml``); otherwise deterministic demo data is shown.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
DEMO_CONTRACTS_DIR = PACKAGE_ROOT / "demo_contracts"


@dataclass(frozen=True)
class Settings:
    warehouse_id: str | None
    contracts_url: str | None
    contracts_token: str | None
    contract_path: str | None
    lookback_days: int = 30

    @property
    def live(self) -> bool:
        """True when a SQL warehouse is configured (live workspace mode)."""
        return bool(self.warehouse_id)

    @property
    def contract_source_label(self) -> str:
        if self.contracts_url:
            return self.contracts_url
        if self.contract_path:
            return self.contract_path
        return "bundled demo contracts"


def load_settings() -> Settings:
    return Settings(
        warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID") or None,
        contracts_url=os.getenv("DC43_CONTRACTS_URL") or None,
        contracts_token=os.getenv("DC43_CONTRACTS_TOKEN") or None,
        contract_path=os.getenv("DC43_CONTRACT_PATH") or None,
        lookback_days=int(os.getenv("DC43_APP_LOOKBACK_DAYS", "30")),
    )
