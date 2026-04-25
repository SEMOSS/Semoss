package prerna.reactor.shortcuts.conductor.oss;

import java.util.List;
import java.util.Map;

public class ConditionEngine {

	public static Map<String, Object> execute(Map<String, Object> input, Map<String, Object> config) {

		List<Map<String, Object>> rules = (List<Map<String, Object>>) config.get("rules");

		for (Map<String, Object> rule : rules) {

			String operatorType = (String) rule.get("operatorType");

			List<Map<String, Object>> conditions = (List<Map<String, Object>>) rule.get("conditions");

			boolean matched = evaluate(operatorType, conditions, input);

			if (matched) {
				return Map.of("route", rule.get("result"));
			}
		}

		return Map.of("route", config.get("default"));
	}

	private static boolean evaluate(String operatorType, List<Map<String, Object>> conditions,
			Map<String, Object> input) {

		if ("AND".equalsIgnoreCase(operatorType)) {

			for (Map<String, Object> cond : conditions) {
				if (!evaluateCondition(cond, input)) {
					return false;
				}
			}
			return true;
		}

		if ("OR".equalsIgnoreCase(operatorType)) {

			for (Map<String, Object> cond : conditions) {
				if (evaluateCondition(cond, input)) {
					return true;
				}
			}
			return false;
		}

		return false;
	}

	private static boolean evaluateCondition(Map<String, Object> cond, Map<String, Object> input) {

		String field = (String) cond.get("field");
		String op = (String) cond.get("op");
		Object expected = cond.get("value");

		Object actual = input.get(field);

		if (actual == null) {
			return false;
		}

		switch (op) {

		case "==":
			return actual.toString().equalsIgnoreCase(expected.toString());

		case "!=":
			return !actual.toString().equalsIgnoreCase(expected.toString());

		case ">":
			return toDouble(actual) > toDouble(expected);

		case "<":
			return toDouble(actual) < toDouble(expected);

		case ">=":
			return toDouble(actual) >= toDouble(expected);

		case "<=":
			return toDouble(actual) <= toDouble(expected);

		case "contains":
			return actual.toString().contains(expected.toString());

		case "startsWith":
			return actual.toString().startsWith(expected.toString());

		case "endsWith":
			return actual.toString().endsWith(expected.toString());

		case "regex":
			return actual.toString().matches(expected.toString());

		case "in":
			return ((List<?>) expected).contains(actual);

		default:
			throw new RuntimeException("Unsupported operator: " + op);
		}
	}

	private static double toDouble(Object o) {
		return Double.parseDouble(o.toString());
	}
}
