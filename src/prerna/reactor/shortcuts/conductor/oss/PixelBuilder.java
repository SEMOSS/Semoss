package prerna.reactor.shortcuts.conductor.oss;

import java.util.List;
import java.util.Map;

public class PixelBuilder {
	public static String toPixel(Object obj) {

		if (obj == null) {
			return "null";
		}

		// Map - {key=value,...}
		if (obj instanceof Map) {

			StringBuilder sb = new StringBuilder("{");
			Map<?, ?> map = (Map<?, ?>) obj;

			for (Map.Entry<?, ?> entry : map.entrySet()) {

				String key = entry.getKey().toString();
				Object value = entry.getValue();

				sb.append(key).append("=").append(toPixel(value)).append(",");
			}

			// remove last comma
			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}

			sb.append("}");
			return sb.toString();
		}

		// List - ["a","b",10]
		if (obj instanceof List) {

			StringBuilder sb = new StringBuilder("[");
			List<?> list = (List<?>) obj;

			for (Object item : list) {
				sb.append(toPixel(item)).append(",");
			}

			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}

			sb.append("]");
			return sb.toString();
		}

		// String - "value"
		if (obj instanceof String) {
			return "\"" + escape((String) obj) + "\"";
		}

		// Boolean / Number
		if (obj instanceof Number || obj instanceof Boolean) {
			return obj.toString();
		}

		// Fallback
		return "\"" + escape(obj.toString()) + "\"";
	}

	private static String escape(String s) {
		return s.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
