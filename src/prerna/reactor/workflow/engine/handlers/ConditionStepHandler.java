package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;

/**
 * Evaluates a condition expression and sets the branch direction in metadata.
 * The WorkflowExecutor reads metadata.branch to decide ifTrue vs ifFalse successors.
 * 
 * Config:
 *   left       — left operand (resolved value)
 *   operator   — comparison operator: "==", "!=", ">", "<", ">=", "<=", "contains", "empty", "notEmpty"
 *   right      — right operand (resolved value, not needed for "empty"/"notEmpty")
 */
public class ConditionStepHandler implements IWorkflowStepHandler {

	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		Object left = config.get("left");
		String operator = (String) config.get("operator");
		Object right = config.get("right");

		if (operator == null || operator.isEmpty()) {
			return StepResult.error(stepId, "Condition step requires an 'operator' in config", 
					System.currentTimeMillis() - start);
		}

		boolean result;
		try {
			result = evaluate(left, operator, right);
		} catch (Exception e) {
			return StepResult.error(stepId, "Condition evaluation failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}

		StepResult stepResult = StepResult.success(stepId, result, System.currentTimeMillis() - start);
		stepResult.getMetadata().put("branch", result ? "ifTrue" : "ifFalse");
		return stepResult;
	}

	private boolean evaluate(Object left, String operator, Object right) {
		switch (operator.toLowerCase()) {
			case "empty":
				return isEmpty(left);
			case "notempty":
				return !isEmpty(left);
			case "==":
			case "equals":
				return objEquals(left, right);
			case "!=":
			case "notequals":
				return !objEquals(left, right);
			case "contains":
				return left != null && right != null && left.toString().contains(right.toString());
			case ">":
				return compareNumbers(left, right) > 0;
			case "<":
				return compareNumbers(left, right) < 0;
			case ">=":
				return compareNumbers(left, right) >= 0;
			case "<=":
				return compareNumbers(left, right) <= 0;
			default:
				throw new IllegalArgumentException("Unknown operator: " + operator);
		}
	}

	private boolean isEmpty(Object value) {
		if (value == null) return true;
		if (value instanceof String) return ((String) value).isEmpty();
		if (value instanceof java.util.Collection) return ((java.util.Collection<?>) value).isEmpty();
		if (value instanceof Map) return ((Map<?, ?>) value).isEmpty();
		return false;
	}

	private boolean objEquals(Object left, Object right) {
		if (left == null && right == null) return true;
		if (left == null || right == null) return false;
		// Try numeric comparison first
		try {
			double l = toDouble(left);
			double r = toDouble(right);
			return Double.compare(l, r) == 0;
		} catch (NumberFormatException e) {
			// Fall through to string comparison
		}
		return left.toString().equals(right.toString());
	}

	private int compareNumbers(Object left, Object right) {
		return Double.compare(toDouble(left), toDouble(right));
	}

	private double toDouble(Object value) {
		if (value instanceof Number) return ((Number) value).doubleValue();
		return Double.parseDouble(value.toString());
	}
}
