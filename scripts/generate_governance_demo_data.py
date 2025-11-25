"""Populate governance demo data with rich sample contracts and products.

This helper drives the governance HTTP API via ``dc43_service_clients`` to seed
multiple contracts, datasets, runs, and optional data product bindings for
screenshots or ad-hoc demos. It intentionally creates a busy interface with
several stages, statuses, and metrics so history tables and graphs feel alive
without manual setup.
"""
from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

from faker import Faker

from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_products.client.remote import RemoteDataProductServiceClient
from dc43_service_clients.data_products.models import (
    DataProductInputBinding,
    DataProductOutputBinding,
)
from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
)
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
    PipelineContext,
)


@dataclass(frozen=True)
class DemoRun:
    dataset_version: str
    status: str
    metrics: Mapping[str, object]
    reason: str
    stage: str


@dataclass(frozen=True)
class DemoContract:
    contract_id: str
    contract_version: str
    dataset_id: str
    dataset_format: str
    display_name: str
    server_path: str
    schema_version: str
    fields: Sequence[tuple[str, str, bool]]  # (name, type, required)
    data_product_id: str | None
    data_product_version: str | None
    port_name: str | None
    runs: Sequence[DemoRun]


def _build_contract(spec: DemoContract) -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version=spec.contract_version,
        kind="DatasetContract",
        apiVersion="3.0.2",
        id=spec.contract_id,
        name=spec.display_name,
        schema=[
            SchemaObject(
                name=spec.display_name.lower().replace(" ", "_"),
                properties=[
                    SchemaProperty(name=name, physicalType=ptype, required=required)
                    for name, ptype, required in spec.fields
                ],
            )
        ],
        servers=[
            Server(server="s3", type="s3", path=spec.server_path, format=spec.dataset_format)
        ],
    )


def _schema_payload(spec: DemoContract) -> Mapping[str, Mapping[str, object]]:
    return {
        spec.display_name.lower().replace(" ", "_"): {
            **{name: {"type": ptype, "required": required} for name, ptype, required in spec.fields},
            "_schema_version": spec.schema_version,
        }
    }


def _observation(spec: DemoContract, run: DemoRun) -> ObservationPayload:
    return ObservationPayload(metrics=dict(run.metrics), schema=_schema_payload(spec))


def _pipeline_context(label: str, stage: str) -> Mapping[str, object]:
    return PipelineContext(
        pipeline="governance-demo",
        label=label,
        metadata={"stage": stage, "run_ts": datetime.now(tz=timezone.utc).isoformat()},
    ).as_dict()


def _binding(spec: DemoContract) -> tuple[DataProductInputBinding | None, DataProductOutputBinding | None]:
    if not spec.data_product_id or not spec.port_name:
        return None, None

    input_binding = DataProductInputBinding(
        data_product=spec.data_product_id,
        port_name=spec.port_name,
        custom_properties={"demo": True, "port": spec.port_name},
        data_product_version=spec.data_product_version,
    )
    output_binding = DataProductOutputBinding(
        data_product=spec.data_product_id,
        port_name=spec.port_name,
        custom_properties={"demo": True, "port": spec.port_name},
        data_product_version=spec.data_product_version,
    )
    return input_binding, output_binding


def _draft_contract(client: RemoteGovernanceServiceClient, spec: DemoContract) -> None:
    seed_run = spec.runs[0]
    client.draft_contract(
        dataset=PipelineContext(
            label=spec.dataset_id,
            metadata={"dataset_id": spec.dataset_id, "dataset_format": spec.dataset_format},
        ),
        validation=ValidationResult(metrics=dict(seed_run.metrics), status=seed_run.status, reason=seed_run.reason),
        observation=_observation(spec, seed_run),
        contract=_build_contract(spec),
    )


def _register_write(
    client: RemoteGovernanceServiceClient,
    spec: DemoContract,
    run: DemoRun,
    output_binding: DataProductOutputBinding | None,
) -> None:
    plan = client.resolve_write_context(
        context=GovernanceWriteContext(
            contract=ContractReference(contract_id=spec.contract_id, contract_version=spec.contract_version),
            output_binding=output_binding,
            dataset_id=spec.dataset_id,
            dataset_version=run.dataset_version,
            dataset_format=spec.dataset_format,
            pipeline_context=_pipeline_context(label=f"write::{run.dataset_version}", stage=run.stage),
            draft_on_violation=True,
            data_product_status_failure_message="Demo write draft",
        )
    )
    assessment = client.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(metrics=dict(run.metrics), status=run.status, reason=run.reason),
        observations=lambda: _observation(spec, run),
    )
    client.register_write_activity(plan=plan, assessment=assessment)
    client.link_dataset_contract(
        dataset_id=plan.dataset_id,
        dataset_version=plan.dataset_version,
        contract_id=plan.contract_id,
        contract_version=plan.contract_version,
    )


def _register_read(
    client: RemoteGovernanceServiceClient,
    spec: DemoContract,
    run: DemoRun,
    input_binding: DataProductInputBinding | None,
) -> None:
    plan = client.resolve_read_context(
        context=GovernanceReadContext(
            contract=ContractReference(contract_id=spec.contract_id, contract_version=spec.contract_version),
            input_binding=input_binding,
            dataset_id=spec.dataset_id,
            dataset_version=run.dataset_version,
            dataset_format=spec.dataset_format,
            pipeline_context=_pipeline_context(label=f"read::{run.dataset_version}", stage=run.stage),
            draft_on_violation=run.status != "ok",
        )
    )
    assessment = client.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(metrics=dict(run.metrics), status=run.status, reason=run.reason),
        observations=lambda: _observation(spec, run),
    )
    client.register_read_activity(plan=plan, assessment=assessment)


def _seed_contract(client: RemoteGovernanceServiceClient, spec: DemoContract) -> None:
    input_binding, output_binding = _binding(spec)
    _draft_contract(client, spec)
    for run in spec.runs:
        _register_write(client, spec, run, output_binding)
        _register_read(client, spec, run, input_binding)


def _semver_variant(base_version: str, offset: int) -> str:
    major, minor, patch = (int(part) for part in base_version.split("."))
    patch += offset
    minor += patch // 10
    patch %= 10
    return f"{major}.{minor}.{patch}"


def _make_products(fake: Faker, num_products: int) -> Sequence[str]:
    return [f"demo.{fake.word()}-{idx:02d}" for idx in range(num_products)]


def _random_fields(fake: Faker) -> Sequence[tuple[str, str, bool]]:
    field_types = ("integer", "string", "number", "boolean")
    count = random.randint(4, 8)
    names = fake.words(nb=count, unique=True)
    return tuple((name.replace("-", "_"), random.choice(field_types), bool(random.getrandbits(1))) for name in names)


def _random_metrics(fake: Faker, base_rows: int) -> Mapping[str, object]:
    quality_metric = random.choice(
        [
            ("null_rate", lambda: round(random.random() * 0.02, 4)),
            ("late_records", lambda: random.randint(0, 25)),
            ("duplicate_ids", lambda: random.randint(0, 5)),
            ("schema_violations", lambda: random.randint(0, 3)),
        ]
    )
    secondary_metric = random.choice(
        [
            ("freshness_minutes", lambda: random.randint(5, 720)),
            ("new_columns", lambda: random.randint(0, 4)),
            ("removed_columns", lambda: random.randint(0, 3)),
        ]
    )
    return {
        "row_count": max(0, base_rows + random.randint(-50, 150)),
        quality_metric[0]: quality_metric[1](),
        secondary_metric[0]: secondary_metric[1](),
        "notes": fake.sentence(nb_words=6),
    }


def _generate_runs(
    count: int,
    contract_idx: int,
    fake: Faker,
    start_day: date,
    run_seed: str,
) -> Sequence[DemoRun]:
    stages = ("bronze", "silver", "gold", "platinum")
    statuses = ("ok", "ok", "warn", "warn", "block")
    runs: list[DemoRun] = []
    base_rows = random.randint(500, 5000) + contract_idx * 5
    for idx in range(count):
        run_day = start_day - timedelta(days=idx + contract_idx)
        metrics = _random_metrics(fake, base_rows + idx * random.randint(5, 15))
        runs.append(
            DemoRun(
                dataset_version=f"{run_day:%Y-%m-%d}-{run_seed}-r{idx:03d}",
                status=random.choice(statuses),
                metrics=metrics,
                reason=f"{fake.bs().capitalize()} — {fake.catch_phrase()}",
                stage=stages[idx % len(stages)],
            )
        )
    return runs


def _demo_contracts(
    num_contracts: int,
    num_products: int,
    runs_per_contract: int,
    fake: Faker,
    start_day: date,
) -> Iterable[DemoContract]:
    products = _make_products(fake, num_products)
    for idx in range(num_contracts):
        base_version = f"{(idx % 4) + 1}.{(idx % 7)}.{fake.random_int(min=0, max=9)}"
        descriptor = fake.word().replace("_", "-")
        contract_root = fake.word().replace("_", "-")
        dataset_root = fake.word().replace("_", "-")
        product_id = products[idx % len(products)]
        product_version = f"{(idx % 3) + 1}.0.{fake.random_int(min=0, max=9)}"
        dataset_format = random.choice(["delta", "parquet", "csv"])
        port_name = f"port-{fake.word()}" if idx % 5 else None
        run_seed = f"c{idx:03d}"
        yield DemoContract(
            contract_id=f"demo.{contract_root}.{descriptor}.{idx + 1:03d}",
            contract_version=_semver_variant(base_version, idx % 10),
            dataset_id=f"{dataset_root}.demo.{descriptor}-{idx + 1:03d}",
            dataset_format=dataset_format,
            display_name=f"{fake.catch_phrase()} #{idx + 1}",
            server_path=f"datalake/demo/{dataset_root}/{descriptor}/{idx + 1:03d}",
            schema_version=f"{start_day:%Y-%m}-{(idx % 5) + 1}",
            fields=_random_fields(fake),
            data_product_id=product_id if idx % 3 else None,
            data_product_version=product_version if idx % 3 else None,
            port_name=port_name,
            runs=_generate_runs(runs_per_contract, idx + 1, fake, start_day, run_seed),
        )


def _data_products(
    contracts: Sequence[DemoContract],
    fake: Faker,
) -> Sequence[OpenDataProductStandard]:
    products: dict[tuple[str, str], OpenDataProductStandard] = {}
    for spec in contracts:
        if not spec.data_product_id or not spec.port_name or not spec.data_product_version:
            continue
        key = (spec.data_product_id, spec.data_product_version)
        product = products.get(key)
        if product is None:
            product = OpenDataProductStandard(
                id=spec.data_product_id,
                status="active",
                version=spec.data_product_version,
                name=fake.catch_phrase(),
                description={
                    "en": f"Demo data product {spec.data_product_id} for UI screenshots",
                },
                tags=[spec.dataset_format, "demo"],
            )
            products[key] = product

        if not any(port.name == spec.port_name for port in product.output_ports):
            product.output_ports.append(
                DataProductOutputPort(
                    name=spec.port_name,
                    version=spec.contract_version,
                    contract_id=spec.contract_id,
                    description=fake.bs(),
                    type="dataset",
                )
            )

        if not any(port.name == spec.port_name for port in product.input_ports):
            product.input_ports.append(
                DataProductInputPort(
                    name=spec.port_name,
                    version=spec.contract_version,
                    contract_id=spec.contract_id,
                    custom_properties=[{"source": "demo"}],
                )
            )

    return list(products.values())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="Governance service base URL")
    parser.add_argument("--token", default=None, help="Optional bearer token")
    parser.add_argument("--token-header", default="Authorization", help="Header used for tokens")
    parser.add_argument("--token-scheme", default="Bearer", help="Token scheme prefix")
    parser.add_argument(
        "--contracts",
        type=int,
        default=75,
        help="Number of contracts to generate (variants of the base scenarios)",
    )
    parser.add_argument(
        "--products",
        type=int,
        default=10,
        help="Number of data products to cycle through when binding ports",
    )
    parser.add_argument(
        "--total-runs",
        type=int,
        default=500,
        help="Approximate total run statuses to create across contracts",
    )
    parser.add_argument(
        "--runs-per-contract",
        type=int,
        default=None,
        help="Override runs per contract; defaults to a total-runs driven value",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed for reproducible demo data")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    fake = Faker()
    runs_per_contract = args.runs_per_contract or max(1, math.ceil(args.total_runs / args.contracts))
    client = RemoteGovernanceServiceClient(
        base_url=args.base_url,
        token=args.token,
        token_header=args.token_header,
        token_scheme=args.token_scheme,
    )
    contract_client = RemoteContractServiceClient(
        base_url=args.base_url,
        token=args.token,
        token_header=args.token_header,
        token_scheme=args.token_scheme,
    )
    data_product_client = RemoteDataProductServiceClient(
        base_url=args.base_url,
        token=args.token,
        token_header=args.token_header,
        token_scheme=args.token_scheme,
    )

    contracts = list(
        _demo_contracts(
            num_contracts=args.contracts,
            num_products=args.products,
            runs_per_contract=runs_per_contract,
            fake=fake,
            start_day=datetime.now(tz=timezone.utc).date(),
        )
    )

    for product in _data_products(contracts, fake):
        data_product_client.put(product)

    for spec in contracts:
        contract_client.put(_build_contract(spec))
        _seed_contract(client, spec)

    print("Seeded demo contracts:")
    for spec in contracts:
        print(f"- {spec.contract_id}:{spec.contract_version} -> {spec.dataset_id}")


if __name__ == "__main__":
    main()
