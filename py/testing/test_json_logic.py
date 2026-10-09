import json

from utils.json_logic import evaluate_json_with_reason


def evaluate(rule, data=None):
    response = evaluate_json_with_reason(json.dumps(rule), json.dumps(data) if data is not None else None)
    return json.loads(response)


# ---------------------------------------------------------------------------
# Passing rules — no reason attached
# ---------------------------------------------------------------------------

def test_passing_comparison_has_no_reason():
    result = evaluate({">=": [{"var": "age"}, 21]}, {"age": 25})
    assert result == {"result": True, "reason": None}


def test_passing_and_has_no_reason():
    rule = {"and": [{">=": [{"var": "age"}, 21]}, {"==": [{"var": "status"}, "active"]}]}
    result = evaluate(rule, {"age": 25, "status": "active"})
    assert result == {"result": True, "reason": None}


# ---------------------------------------------------------------------------
# Failing comparisons — default trace message (no reason op)
# ---------------------------------------------------------------------------

def test_failed_comparison_names_variable():
    result = evaluate({">=": [{"var": "age"}, 21]}, {"age": 18})
    assert result["result"] is False
    assert "age" in result["reason"]
    assert "18" in result["reason"]
    assert "21" in result["reason"]


def test_failed_and_short_circuits_to_first_failure():
    # age fails first — short-circuit means status is never evaluated
    rule = {"and": [{">=": [{"var": "age"}, 21]}, {"==": [{"var": "status"}, "active"]}]}
    result = evaluate(rule, {"age": 18, "status": "active"})
    assert result["result"] is False
    assert "age" in result["reason"]
    assert "status" not in result["reason"]


def test_failed_or_returns_default_reason():
    rule = {"or": [{">=": [{"var": "age"}, 21]}, {"==": [{"var": "status"}, "active"]}]}
    result = evaluate(rule, {"age": 18, "status": "inactive"})
    assert result["result"] is False
    assert result["reason"] is not None


def test_failed_comparison_no_variable_ref():
    result = evaluate({"==": [1, 2]}, {})
    assert result["result"] is False
    assert result["reason"] is not None


# ---------------------------------------------------------------------------
# reason op on if-branch string returns
# ---------------------------------------------------------------------------

def test_reason_op_disq_branch():
    rule = {
        "if": [
            {">=": [{"var": "age"}, 21]},
            {"reason": ["QUAL", "Age requirement met"]},
            {"reason": ["DISQ", "Applicant is under 21"]},
        ]
    }
    result = evaluate(rule, {"age": 18})
    assert result == {"result": "DISQ", "reason": "Applicant is under 21"}


def test_reason_op_qual_branch():
    rule = {
        "if": [
            {">=": [{"var": "age"}, 21]},
            {"reason": ["QUAL", "Age requirement met"]},
            {"reason": ["DISQ", "Applicant is under 21"]},
        ]
    }
    result = evaluate(rule, {"age": 25})
    assert result == {"result": "QUAL", "reason": "Age requirement met"}


def test_passing_branch_without_reason_op_has_no_reason():
    rule = {
        "if": [
            {">=": [{"var": "age"}, 17]},
            "INNER",
            {"reason": ["NOT_EVALUATED", "Prerequisites not met"]},
        ]
    }
    result = evaluate(rule, {"age": 20})
    assert result["result"] == "INNER"
    assert result["reason"] is None


# ---------------------------------------------------------------------------
# reason op on comparisons — custom message overrides default trace
# ---------------------------------------------------------------------------

def test_reason_op_on_comparison_uses_custom_message():
    rule = {"reason": [{">=": [{"var": "age"}, 21]}, "Applicant must be at least 21"]}
    result = evaluate(rule, {"age": 18})
    assert result == {"result": False, "reason": "Applicant must be at least 21"}


def test_reason_op_on_passing_comparison_has_no_reason():
    rule = {"reason": [{">=": [{"var": "age"}, 21]}, "Applicant must be at least 21"]}
    result = evaluate(rule, {"age": 25})
    assert result == {"result": True, "reason": None}


def test_reason_op_in_and_overrides_default():
    rule = {
        "and": [
            {"reason": [{">=": [{"var": "age"}, 21]}, "Must be 21 or older"]},
            {"==": [{"var": "status"}, "active"]},
        ]
    }
    result = evaluate(rule, {"age": 18, "status": "active"})
    assert result["result"] is False
    assert result["reason"] == "Must be 21 or older"


# ---------------------------------------------------------------------------
# Nested if (mirrors the military eligibility rule shape)
# ---------------------------------------------------------------------------

def test_nested_if_disq_from_inner_condition():
    rule = {
        "if": [
            {"and": [{">=": [{"var": "age"}, 17]}, {"<=": [{"var": "age"}, 42]}]},
            {
                "if": [
                    {"==": [{"var": "condition_a"}, True]},
                    {"reason": ["DISQ", "Disqualified: condition_a is present"]},
                    {"reason": ["QUAL", "All conditions met"]},
                ]
            },
            {"reason": ["NOT_EVALUATED", "Age prerequisites not met"]},
        ]
    }

    result = evaluate(rule, {"age": 25, "condition_a": True})
    assert result == {"result": "DISQ", "reason": "Disqualified: condition_a is present"}

    result = evaluate(rule, {"age": 25, "condition_a": False})
    assert result == {"result": "QUAL", "reason": "All conditions met"}

    result = evaluate(rule, {"age": 50, "condition_a": False})
    assert result == {"result": "NOT_EVALUATED", "reason": "Age prerequisites not met"}
