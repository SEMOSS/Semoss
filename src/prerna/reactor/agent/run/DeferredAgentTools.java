package prerna.reactor.agent.run;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.gson.GsonUtility;

/** Discovery and room-persistent schema loading for the native RunAgent harness. */
public final class DeferredAgentTools {

	public static final String RUN_AGENT_PARAM = "__semoss_deferred_tool_loading";
	public static final String LOADED_OPTION = "loadedDeferredTools";
	private static final String SEARCH = "SearchTools";
	private static final String LOAD = "LoadTools";
	private static final Logger logger = LogManager.getLogger(DeferredAgentTools.class);

	public static final String PROMPT = "Some tools are deferred. If a needed tool is missing, use SearchTools to find it, "
			+ "then LoadTools with its returned ID before calling it. Loaded tools remain available for this room "
			+ "across later tasks; their full schemas appear on the next model call.";

	private DeferredAgentTools() {
	}

	public static List<Map<String, Object>> definitions() {
		return List.of(definition(SEARCH,
				"Search deferred tools available to this room by keywords. Returns IDs, names, compact descriptions "
						+ "and loaded status. An empty query lists tools. LoadTools activates their complete schemas.",
				Map.of("query", Map.of("type", "string", "description", "Keywords describing the tool needed."),
						"limit", Map.of("type", "integer", "description", "Maximum results; default 10, capped at 20.",
								"default", 10, "minimum", 1)),
				List.of("query")),
				definition(LOAD,
						"Load deferred tools by their SearchTools IDs for subsequent model calls. Loading is idempotent "
								+ "and persists for this room. Disabled or unavailable tools cannot be loaded.",
						Map.of("toolIds", Map.of("type", "array", "items", Map.of("type", "string"), "minItems", 1,
								"description", "Array of IDs from SearchTools, for example [\"default:MultiEdit\"].")),
						List.of("toolIds")));
	}

	private static Map<String, Object> definition(String name, String description, Map<String, Object> properties,
			List<String> required) {
		Map<String, Object> tool = new LinkedHashMap<>();
		tool.put("name", name);
		tool.put("description", description);
		tool.put("inputSchema", Map.of("type", "object", "properties", properties, "required", required));
		tool.put("_meta", Map.of(MCPUtility.SMSS_MCP_EXECUTION, "auto"));
		return new JSONObject(tool).toMap();
	}

	public static boolean isControlTool(String name) {
		return SEARCH.equals(name) || LOAD.equals(name);
	}

	/** Keep a fixed eager prefix, then append eligible schemas in persisted load order. */
	public static List<Map<String, Object>> filterForModel(Room room, List<Map<String, Object>> tools) {
		List<Map<String, Object>> visible = new ArrayList<>();
		Map<String, Map<String, Object>> deferred = new LinkedHashMap<>();
		for (Map<String, Object> tool : tools) {
			if (isDisabled(tool)) {
				continue;
			}
			if (isDeferred(tool)) {
				deferred.putIfAbsent(toolId(room, tool), tool);
			} else {
				visible.add(tool);
			}
		}
		visible.sort(Comparator.comparing(tool -> toolId(room, tool)));
		for (String id : loadedIds(room.getOptionsMap())) {
			Map<String, Object> tool = deferred.get(id);
			if (tool != null) {
				visible.add(tool);
			}
		}
		return visible;
	}

	/** Refresh just this field; RunAgent's workspace/instruction overlays stay in memory. */
	public static void refreshLoadedState(Room room) {
		try (var ignored = RoomMessageStore.acquireMutationLock(room)) {
			applyLoadedState(room, persistedOptions(room));
		}
	}

	/** Called under the same room mutation lock used by LoadTools. */
	public static void preserveLoadedState(Room room, Map<String, Object> requestedOptions) {
		copyLoadedState(requestedOptions, persistedOptions(room));
	}

	public static ToolExecutionResult execute(String name, Map<String, Object> params, Room room,
			Map<String, Object> harnessParams) {
		try (var ignored = RoomMessageStore.acquireMutationLock(room)) {
			Map<String, Map<String, Object>> available = availableTools(room, harnessParams);
			if (SEARCH.equals(name)) {
				applyLoadedState(room, persistedOptions(room));
				return search(params, room, available);
			}
			return load(params, room, available);
		} catch (Exception e) {
			logger.warn("Deferred tool '{}' failed for room '{}': {}", name, room.getId(), e.getMessage(), e);
			String error = "Tool execution error: " + name + ": " + e.getMessage();
			return ToolExecutionResult.error(error, error);
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Map<String, Object>> availableTools(Room room, Map<String, Object> harnessParams) {
		List<Map<String, Object>> tools = new ArrayList<>();
		Object harnessTools = harnessParams.get("tools");
		if (harnessTools instanceof List<?>) {
			tools.addAll((List<Map<String, Object>>) harnessTools);
		}
		// The full lookup survives schema filtering, including shortened provider aliases.
		room.getToolLookupByLLMName().forEach((alias, entry) -> {
			Map<String, Object> tool = new LinkedHashMap<>(entry);
			tool.put("name", alias);
			tools.add(tool);
		});
		Map<String, Map<String, Object>> available = new LinkedHashMap<>();
		for (Map<String, Object> tool : tools) {
			if (isDeferred(tool) && !isDisabled(tool)) {
				available.putIfAbsent(toolId(room, tool), tool);
			}
		}
		return available;
	}

	private static ToolExecutionResult search(Map<String, Object> params, Room room,
			Map<String, Map<String, Object>> available) {
		if (!(params.get("query") instanceof String query)) {
			throw new IllegalArgumentException("query must be a string");
		}
		int limit = 10;
		Object requestedLimit = params.get("limit");
		if (requestedLimit != null) {
			if (!(requestedLimit instanceof Number number) || !Double.isFinite(number.doubleValue())
					|| number.doubleValue() < 1 || number.doubleValue() != Math.floor(number.doubleValue())) {
				throw new IllegalArgumentException("limit must be a positive integer");
			}
			limit = (int) Math.min(20, number.doubleValue());
		}
		String[] keywords = query.toLowerCase(Locale.ROOT).trim().split("\\s+");
		Map<String, Integer> scores = new LinkedHashMap<>();
		available.forEach((id, tool) -> {
			String names = (id + " " + displayName(tool) + " " + tool.getOrDefault("title", ""))
					.toLowerCase(Locale.ROOT);
			String description = String.valueOf(tool.getOrDefault("description", "")).toLowerCase(Locale.ROOT);
			int score = 0;
			for (String keyword : keywords) {
				score += names.contains(keyword) ? 3 : description.contains(keyword) ? 1 : 0;
			}
			if (score > 0) {
				scores.put(id, score);
			}
		});
		Set<String> loaded = loadedIds(room.getOptionsMap());
		List<Map<String, Object>> results = scores.keySet().stream()
				.sorted(Comparator.<String>comparingInt(scores::get).reversed().thenComparing(id -> id))
				.limit(limit).map(id -> {
					Map<String, Object> tool = available.get(id);
					String description = String.valueOf(tool.getOrDefault("description", ""));
					return Map.<String, Object>of("id", id, "name", displayName(tool), "description",
							description.length() > 400 ? description.substring(0, 397) + "..." : description,
							"loaded", loaded.contains(id));
				}).toList();
		return ToolExecutionResult
				.success(new JSONObject(Map.of("tools", results, "totalMatches", scores.size())).toString());
	}

	private static ToolExecutionResult load(Map<String, Object> params, Room room,
			Map<String, Map<String, Object>> available) throws Exception {
		if (!(params.get("toolIds") instanceof List<?> requested) || requested.isEmpty()) {
			throw new IllegalArgumentException("toolIds must be a non-empty array of deferred tool IDs from SearchTools");
		}
		Set<String> ids = new LinkedHashSet<>();
		for (Object value : requested) {
			if (!(value instanceof String id) || !available.containsKey(id)) {
				throw new IllegalArgumentException("Unknown or unavailable deferred tool ID: " + value);
			}
			ids.add(id);
		}
		Map<String, Object> persisted = persistedOptions(room);
		Set<String> loaded = loadedIds(persisted);
		if (loaded.addAll(ids)) {
			persisted.put(LOADED_OPTION, new ArrayList<>(loaded));
			// The ordinary room-options setter logs and swallows failures. Loads must
			// acknowledge a durable write and must never save the runtime option overlays.
			IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
			int updated = QueryExecutionUtility.executeUpdate(db,
					"UPDATE ROOM SET OPTIONS = ? WHERE USER_ID = ? AND ROOM_ID = ?", ps -> {
						db.getQueryUtil().setNullableJson(ps, 1, persisted, GsonUtility.getDefaultGson());
						ps.setString(2, room.getUserId());
						ps.setString(3, room.getId());
					});
			if (updated != 1) {
				throw new IllegalStateException("Unable to persist loaded tools for this room");
			}
		}
		applyLoadedState(room, persisted);
		List<Map<String, Object>> confirmation = ids.stream()
				.map(id -> Map.<String, Object>of("id", id, "name", displayName(available.get(id)), "loaded", true))
				.toList();
		return ToolExecutionResult.success(new JSONObject(Map.of("tools", confirmation)).toString());
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> persistedOptions(Room room) {
		List<Map<String, Object>> rows = ModelInferenceLogsUtils.getRoomOptions(room.getId(), room.getUserId());
		if (rows.size() != 1) {
			throw new IllegalStateException("Unable to read persisted options for this room");
		}
		Object options = rows.get(0).get("OPTIONS");
		if (options == null) {
			return new LinkedHashMap<>();
		}
		if (!(options instanceof Map<?, ?>)) {
			throw new IllegalStateException("Persisted room options must be an object");
		}
		return new LinkedHashMap<>((Map<String, Object>) options);
	}

	private static void applyLoadedState(Room room, Map<String, Object> persisted) {
		copyLoadedState(room.getOptionsMap(), persisted);
		room.setOptionsMap(room.getOptionsMap());
	}

	private static void copyLoadedState(Map<String, Object> options, Map<String, Object> persisted) {
		if (persisted.containsKey(LOADED_OPTION)) {
			options.put(LOADED_OPTION, new ArrayList<>(loadedIds(persisted)));
		} else {
			options.remove(LOADED_OPTION);
		}
	}

	private static Set<String> loadedIds(Map<String, Object> options) {
		Set<String> ids = new LinkedHashSet<>();
		if (options.get(LOADED_OPTION) instanceof List<?> values) {
			for (Object value : values) {
				if (value instanceof String id && !id.isBlank()) {
					ids.add(id);
				}
			}
		}
		return ids;
	}

	private static boolean isDeferred(Map<String, Object> tool) {
		return !isControlTool(String.valueOf(tool.get("name")))
				&& Boolean.TRUE.equals(metadata(tool).get(MCPUtility.SMSS_MCP_DEFERRED));
	}

	private static boolean isDisabled(Map<String, Object> tool) {
		return "disabled".equals(metadata(tool).get(MCPUtility.SMSS_MCP_EXECUTION));
	}

	private static String toolId(Room room, Map<String, Object> tool) {
		String name = String.valueOf(tool.get("name"));
		Map<String, Object> lookup = room.getToolLookupByLLMName().get(name);
		Map<String, Object> meta = metadata(lookup != null ? lookup : tool);
		Object engineId = meta.get(MCPUtility.SMSS_ENGINE_ID);
		return engineId != null ? "mcp:" + engineId + ":" + meta.getOrDefault(MCPUtility.SMSS_ORIGINAL_TOOL_NAME, name)
				: "default:" + name;
	}

	private static String displayName(Map<String, Object> tool) {
		return String.valueOf(metadata(tool).getOrDefault(MCPUtility.SMSS_ORIGINAL_TOOL_NAME, tool.get("name")));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> metadata(Map<String, Object> tool) {
		return tool.get("_meta") instanceof Map<?, ?> meta ? (Map<String, Object>) meta : Map.of();
	}
}
