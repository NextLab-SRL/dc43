"""Small illustrative demand forecasting model used by the retail demo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping


_DEFAULT_COEFFICIENTS = (
    2.1312907625235793,
    320.120408927532,
    26.322528757817366,
    0.3712405692253157,
    -83.67820742168792,
    -31.09664836960005,
)

_DEFAULT_CONFIDENCE_COEFFICIENTS = (
    -0.10249123549771372,
    -37.24070900784901,
    -4.1972329412247005,
    -0.044747555161350656,
    10.454641540050691,
    4.727787840470743,
)


@dataclass(slots=True)
class DemandForecaster:
    """Trivial linear model calibrated on the bundled retail fixtures."""

    coefficients: tuple[float, float, float, float, float, float] = _DEFAULT_COEFFICIENTS
    confidence_coefficients: tuple[float, float, float, float, float, float] = (
        _DEFAULT_CONFIDENCE_COEFFICIENTS
    )
    model_version: str = "v0.1.0"

    def _features(self, record: Mapping[str, float]) -> tuple[float, float, float, float, float, float]:
        units = float(record.get("units_sold", 0.0))
        sell_through = float(record.get("sell_through_rate", 0.0))
        margin_pct = float(record.get("gross_margin_pct", 0.0))
        inventory = float(record.get("inventory_on_hand", 0.0))
        interaction = units * sell_through
        return units, sell_through, margin_pct, inventory, interaction, 1.0

    def predict(
        self,
        rows: Iterable[Mapping[str, float]],
        *,
        prediction_ts: datetime | None = None,
    ) -> list[dict[str, float | str]]:
        """Return scored demand forecasts for ``rows``.

        The model is intentionally simple: it uses a linear combination of
        features derived from the daily sales mart. The coefficients are solved
        via ordinary least squares against the bundled fixture so that the
        generated values line up with the static dataset shipped with the demo.
        """

        timestamp = prediction_ts or datetime.now(timezone.utc)
        iso_ts = timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        predictions: list[dict[str, float | str]] = []
        for row in rows:
            features = self._features(row)
            forecast = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            confidence = sum(
                coef * feat for coef, feat in zip(self.confidence_coefficients, features)
            )
            predictions.append(
                {
                    "date_key": str(row.get("date_key", "")),
                    "business_date": str(row.get("business_date", "")),
                    "store_key": str(row.get("store_key", "")),
                    "store_id": str(row.get("store_id", "")),
                    "category": str(row.get("category", "")),
                    "forecast_units": round(float(forecast), 1),
                    "confidence": round(float(confidence), 2),
                    "model_version": self.model_version,
                    "feature_version": str(row.get("feature_version", "")),
                    "prediction_ts": iso_ts,
                }
            )
        return predictions


__all__ = ["DemandForecaster"]
