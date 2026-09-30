"""Focused tests for the Automation run-local data contract."""

import base64
import json

import pytest

import semoss_automation_runtime as runtime


OWNER = {"projectId": "project", "runId": "run", "userId": "user"}


def data_service(
    *, max_value_bytes: int = 1_024, max_owner_bytes: int = 4_096,
    max_owner_values: int = 4
) -> runtime.AutomationDataService:
    return runtime.create_data_service(
        {}, max_value_bytes, max_owner_bytes, max_owner_values
    )


def test_small_values_keep_their_existing_shape():
    value = {"status": "ok", "count": 2}

    prepared = runtime._prepare_node_value(
        value, 1_024, "value", 512, OWNER, data_service()
    )

    assert prepared == value
    assert runtime._data_reference_metadata(prepared) is None


def test_large_value_is_referenced_and_transparently_resolved():
    value = [{"id": index, "name": f"row-{index}"} for index in range(20)]
    service = data_service()

    reference = runtime._prepare_node_value(
        value, 1_024, "dataset", 64, OWNER, service
    )
    scope = runtime.AutomationScope({"rows": reference}, OWNER, service)
    page = runtime.read_data_page(reference, OWNER, 5, 4, 1_024, service)

    assert scope["rows"] is value
    assert page["kind"] == "table"
    assert page["headers"] == ["id", "name"]
    assert page["rows"][0] == [5, "row-5"]
    assert page["count"] == 4
    assert page["total"] == 20
    assert page["hasMore"] is True


def test_referenced_object_keeps_nested_scope_access_contract():
    value = {
        "data_element": "match_id",
        "metadata": {"label": "IPL data", "count": 400},
        "padding": "x" * 128,
    }
    service = data_service()
    reference = runtime._prepare_node_value(
        value, 1_024, "value", 32, OWNER, service
    )
    scope = runtime.AutomationScope({"ipl_data": reference}, OWNER, service)

    assert scope["ipl_data"]["data_element"] == "match_id"
    assert scope.get("ipl_data", {}).get("data_element") == "match_id"
    assert scope.resolve("${ipl_data.data_element}") == "match_id"
    assert scope.resolve("Rows use ${ipl_data.metadata.count} records") == (
        "Rows use 400 records"
    )


def test_inline_and_referenced_values_resolve_with_the_same_scope_shape():
    value = {"data_element": {"name": "winner", "type": "string"}}
    service = data_service()
    reference = service.store(OWNER, value, "value", 128)
    inline_scope = runtime.AutomationScope({"ipl_data": value}, OWNER, service)
    retained_scope = runtime.AutomationScope(
        {"ipl_data": reference}, OWNER, service
    )

    assert retained_scope["ipl_data"] == inline_scope["ipl_data"]
    assert retained_scope.resolve("${ipl_data.data_element.name}") == (
        inline_scope.resolve("${ipl_data.data_element.name}")
    )


def test_generated_node_envelope_retains_only_its_business_value():
    service = data_service()
    envelope = {
        runtime._INTERNAL_RESULT_VALUE: {
            "data_element": "winner",
            "padding": "x" * 128,
        },
        runtime._INTERNAL_RESULT_METADATA: {"messageId": "message-1"},
    }

    prepared = runtime._node_result(
        envelope, 1_024, "value", 32, OWNER, service
    )
    value = prepared[runtime._INTERNAL_RESULT_VALUE]

    assert runtime._data_reference_metadata(value) is not None
    assert prepared[runtime._INTERNAL_RESULT_METADATA] == {
        "messageId": "message-1"
    }
    assert runtime.AutomationScope({"result": value}, OWNER, service).resolve(
        "${result.data_element}"
    ) == "winner"


def test_trigger_globals_use_the_same_retained_value_contract():
    service = data_service()
    encoded_scope = base64.urlsafe_b64encode(b"{}").decode("ascii")
    source = (
        "def run(scope):\n"
        "    return {'ipl_data': {'data_element': 'venue', "
        "'padding': 'x' * 128}}\n"
    )
    encoded_source = base64.urlsafe_b64encode(source.encode("utf-8")).decode(
        "ascii"
    )

    globals_result = runtime.execute_trigger(
        encoded_scope, encoded_source, 1_024, 32, OWNER, service
    )
    scope = runtime.AutomationScope(globals_result, OWNER, service)

    assert runtime._data_reference_metadata(globals_result["ipl_data"]) is not None
    assert scope.resolve("${ipl_data.data_element}") == "venue"


def test_java_completed_node_result_uses_the_same_scope_contract():
    service = data_service()
    value = {
        runtime._INTERNAL_RESULT_VALUE: {
            "data_element": "agent_summary",
            "padding": "x" * 128,
        },
        runtime._INTERNAL_RESULT_METADATA: {"status": "COMPLETED"},
    }
    encoded_value = base64.urlsafe_b64encode(
        json.dumps(value).encode("utf-8")
    ).decode("ascii")

    prepared = runtime.prepare_node_result(
        encoded_value, 1_024, "value", 32, OWNER, service
    )
    scope = runtime.AutomationScope(
        {"agent_result": prepared[runtime._INTERNAL_RESULT_VALUE]}, OWNER, service
    )

    assert scope.resolve("${agent_result.data_element}") == "agent_summary"
    assert prepared[runtime._INTERNAL_RESULT_METADATA]["status"] == "COMPLETED"


def test_reference_cannot_be_replayed_by_another_owner():
    service = data_service()
    reference = runtime._prepare_node_value(
        ["large-value"], 1_024, "collection", 4, OWNER, service
    )
    other_owner = {**OWNER, "runId": "another-run"}

    with pytest.raises(PermissionError):
        service.resolve(reference, other_owner)


def test_provider_enforces_single_value_and_run_quotas():
    value_limited = data_service(max_value_bytes=32)
    with pytest.raises(ValueError, match="retained-value maximum"):
        runtime._prepare_node_value(
            "x" * 64, 128, "value", 4, OWNER, value_limited
        )

    run_limited = data_service(
        max_value_bytes=128, max_owner_bytes=67, max_owner_values=4
    )
    runtime._prepare_node_value("a" * 32, 128, "value", 4, OWNER, run_limited)
    with pytest.raises(ValueError, match="retained-data maximum"):
        runtime._prepare_node_value("b" * 32, 128, "value", 4, OWNER, run_limited)


def test_task_dataset_pages_rows_without_exposing_the_reference():
    reference = runtime._data_reference("task-reference", "dataset")

    class StubDataset(runtime.AutomationDataset):
        def __init__(self):
            super().__init__(reference, OWNER, page_size=2)
            self.page_calls = []

        def _read_page(self, offset: int, limit: int):
            self.page_calls.append((offset, limit))
            rows = [[1, "one"], [2, "two"], [3, "three"]]
            selected = rows[offset : offset + limit]
            return {
                "available": True,
                "kind": "table",
                "valueType": "dataset",
                "offset": offset,
                "limit": limit,
                "count": len(selected),
                "total": len(rows),
                "hasMore": offset + len(selected) < len(rows),
                "headers": ["id", "name"],
                "rows": selected,
            }

    dataset = StubDataset()

    assert list(dataset) == [
        {"id": 1, "name": "one"},
        {"id": 2, "name": "two"},
        {"id": 3, "name": "three"},
    ]
    assert dataset.page_calls == [(0, 1), (1, 2)]
    assert len(dataset) == 3
    assert dataset[1] == {"id": 2, "name": "two"}
    assert dataset[:2] == [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]
    assert data_service().reference_for(dataset, OWNER) == reference


def test_scope_resolves_task_reference_to_lazy_dataset():
    reference = runtime._data_reference("task-reference", "dataset")
    service = data_service()

    value = runtime.AutomationScope({"rows": reference}, OWNER, service)["rows"]

    assert isinstance(value, runtime.AutomationDataset)
    assert value.reference == reference
    assert value.owner == OWNER
