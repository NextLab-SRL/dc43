import json

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Description,
)

from dc43.storage.fs import FSContractStore


# helper to create a simple contract

def make_contract(cid: str) -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id=cid,
        name="Test",
        description=Description(usage="test"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="id", physicalType="bigint", required=True),
                    SchemaProperty(name="val", physicalType="string", required=True),
                ],
            )
        ],
    )


def test_pipeline_creates_output_contract(spark, tmp_path, monkeypatch):
    from dc43.demo_app import pipeline

    store_dir = tmp_path / "contracts"
    store = FSContractStore(str(store_dir))
    in_contract = make_contract("in.ds")
    store.put(in_contract)

    dataset_file = tmp_path / "datasets.json"
    dataset_file.write_text("[]", encoding="utf-8")

    def load_records():
        raw = json.loads(dataset_file.read_text())
        return [pipeline.DatasetRecord(**r) for r in raw]

    def save_records(records):
        dataset_file.write_text(json.dumps([r.__dict__ for r in records]), encoding="utf-8")

    monkeypatch.setattr(pipeline, "store", store)
    monkeypatch.setattr(pipeline, "DATASETS_FILE", dataset_file)
    monkeypatch.setattr(pipeline, "load_records", load_records)
    monkeypatch.setattr(pipeline, "save_records", save_records)

    input_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    input_path = str(tmp_path / "input")
    input_df.write.mode("overwrite").format("json").save(input_path)
    output_path = str(tmp_path / "out")

    pipeline.run_pipeline(
        input_contract_id=in_contract.id,
        input_contract_version=in_contract.version,
        output_contract_id="out.ds",
        output_contract_version="1.0.0",
        input_path=input_path,
        output_path=output_path,
        dataset_version="v1",
    )

    # contract for output should now exist and be draft
    out_contract = store.get("out.ds", "1.0.0")
    assert out_contract.status == "draft"

    records = load_records()
    assert len(records) == 1
    rec = records[0]
    assert rec.contract_id == "out.ds"
    assert rec.status == "ok"


def test_pipeline_blocks_on_dq_violation(spark, tmp_path, monkeypatch):
    from dc43.demo_app import pipeline

    store_dir = tmp_path / "contracts"
    store = FSContractStore(str(store_dir))
    in_contract = make_contract("in.ds")
    # output contract requires unique id to trigger violation
    out_contract = make_contract("out.ds")
    out_contract.schema_[0].properties[0].unique = True  # type: ignore[attr-defined]
    store.put(in_contract)
    store.put(out_contract)

    dataset_file = tmp_path / "datasets.json"
    dataset_file.write_text("[]", encoding="utf-8")

    def load_records():
        raw = json.loads(dataset_file.read_text())
        return [pipeline.DatasetRecord(**r) for r in raw]

    def save_records(records):
        dataset_file.write_text(json.dumps([r.__dict__ for r in records]), encoding="utf-8")

    monkeypatch.setattr(pipeline, "store", store)
    monkeypatch.setattr(pipeline, "DATASETS_FILE", dataset_file)
    monkeypatch.setattr(pipeline, "load_records", load_records)
    monkeypatch.setattr(pipeline, "save_records", save_records)

    # duplicate id violates unique rule
    input_df = spark.createDataFrame([(1, "a"), (1, "b")], ["id", "val"])
    input_path = str(tmp_path / "input")
    input_df.write.mode("overwrite").format("json").save(input_path)
    output_path = str(tmp_path / "out")

    import pytest

    with pytest.raises(ValueError):
        pipeline.run_pipeline(
            input_contract_id=in_contract.id,
            input_contract_version=in_contract.version,
            output_contract_id=out_contract.id,
            output_contract_version=out_contract.version,
            input_path=input_path,
            output_path=output_path,
            dataset_version="v1",
        )

    # No dataset record should be written on block
    records = load_records()
    assert records == []
