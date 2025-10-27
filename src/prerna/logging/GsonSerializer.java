package prerna.logging;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import prerna.engine.impl.model.Room;

public class GsonSerializer {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public static String toJson(Object src) {
		JsonElement tree = toJsonTree(src, new HashSet<>());
		return gson.toJson(tree);

	}

	private static JsonElement toJsonTree(Object value, Set<Object> visited) {

		if (value == null)
			return JsonNull.INSTANCE;

		if (visited.contains(value)) {
			return new JsonPrimitive("");
		} else {
			visited.add(value);
		}

		if (value instanceof Number)
			return new JsonPrimitive((Number) value);

		if (value instanceof Boolean)
			return new JsonPrimitive((Boolean) value);

		if (value instanceof Character || value instanceof String)
			return new JsonPrimitive(value.toString());

		if (value instanceof Collection<?>) {
			JsonArray arr = new JsonArray();
			for (Object o : (Collection<?>) value)
				arr.add(toJsonTree(o, visited));
			return arr;
		}

		if (value.getClass().isArray()) {
			JsonArray arr = new JsonArray();
			int len = java.lang.reflect.Array.getLength(value);
			for (int i = 0; i < len; i++)
				arr.add(toJsonTree(java.lang.reflect.Array.get(value, i), visited));
			return arr;
		}

		if (value instanceof Map<?, ?>) {
			JsonObject obj = new JsonObject();
			for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet())
				obj.add(String.valueOf(e.getKey()), toJsonTree(e.getValue(), visited));
			return obj;
		}

		if (value instanceof Room) {
			Room r = (Room) value;
			JsonObject o = new JsonObject();
			o.addProperty(SemossLogUtils.ROOM_ID, r.getId());
			return o;

		}

		// Reflectively handle unknown objects

		JsonObject obj = new JsonObject();
		for (var f : value.getClass().getDeclaredFields()) {
			if (java.lang.reflect.Modifier.isStatic(f.getModifiers()))
				continue;
			f.setAccessible(true);

			try {
				obj.add(f.getName(), toJsonTree(f.get(value), visited));
			} catch (Exception ignored) {
				
			}
		}

		return obj;
	}
}
