package prerna.reactor.agent;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.agent.ClaudeCodeTranscriptModels.*;

/**
 * Parses a single JSONL line from a Claude Code transcript file and
 * returns a JSON representation using the models defined in
 * {@link ClaudeCodeTranscriptModels}.
 *
 * <p>Each line in the transcript has a top-level "type" field:
 * <ul>
 *   <li>"user"      — either a user prompt or a tool result</li>
 *   <li>"assistant"  — assistant text and/or tool invocations</li>
 *   <li>"queue-operation", "last-prompt", "attachment" — metadata (skipped)</li>
 * </ul>
 */
public class ClaudeCodeTranscriptParser {

	/**
	 * Parse a raw JSONL line into a structured event JSONObject.
	 *
	 * @param raw the parsed JSONObject from one JSONL line
	 * @return a structured JSON event, or null to skip this line
	 */
	public static JSONObject parse(JSONObject raw) {
		String type = raw.optString("type", "");

		switch (type) {
			case "user":
				return parseUserLine(raw);
			case "assistant":
				return parseAssistantLine(raw);
			default:
				// queue-operation, last-prompt, attachment — skip
				return null;
		}
	}

	/**
	 * A "user" line is either:
	 *  1. A user prompt (message.content is a string)
	 *  2. A tool result (toolUseResult is present)
	 */
	private static JSONObject parseUserLine(JSONObject raw) {
		// Check if this is a tool result
		if (raw.has("toolUseResult")) {
			return parseToolResult(raw);
		}

		// Otherwise it's a user prompt
		JSONObject message = raw.optJSONObject("message");
		if (message == null) {
			return null;
		}

		Object content = message.opt("content");
		if (content instanceof String) {
			UserPrompt prompt = new UserPrompt(
				raw.optString("promptId", null),
				(String) content,
				raw.optString("timestamp", "")
			);
			return toEvent("user_prompt", userPromptToJson(prompt), raw);
		}

		// content is an array (tool_result messages) — parse as tool result
		if (content instanceof JSONArray) {
			return parseToolResultFromContent(raw, (JSONArray) content);
		}

		return null;
	}

	/**
	 * Parse a tool result from the toolUseResult field.
	 */
	private static JSONObject parseToolResult(JSONObject raw) {
		JSONObject tur = raw.getJSONObject("toolUseResult");

		// Get tool_use_id from content array if present
		String toolUseId = null;
		JSONObject message = raw.optJSONObject("message");
		if (message != null) {
			JSONArray content = message.optJSONArray("content");
			if (content != null && content.length() > 0) {
				toolUseId = content.getJSONObject(0).optString("tool_use_id", null);
			}
		}

		ToolStats stats = null;
		if (tur.has("toolStats")) {
			JSONObject ts = tur.getJSONObject("toolStats");
			stats = new ToolStats(
				ts.optInt("readCount", 0),
				ts.optInt("searchCount", 0),
				ts.optInt("bashCount", 0),
				ts.optInt("editFileCount", 0),
				ts.optInt("linesAdded", 0),
				ts.optInt("linesRemoved", 0)
			);
		}

		ToolResult result = new ToolResult(
			toolUseId,
			tur.optString("status", ""),
			tur.optLong("totalDurationMs", 0),
			stats,
			null, // filePath — not directly available in this format
			raw.optString("timestamp", "")
		);

		return toEvent("tool_result", toolResultToJson(result), raw);
	}

	/**
	 * Parse a tool result from the content array (when toolUseResult is absent).
	 */
	private static JSONObject parseToolResultFromContent(JSONObject raw, JSONArray content) {
		for (int i = 0; i < content.length(); i++) {
			JSONObject block = content.getJSONObject(i);
			if ("tool_result".equals(block.optString("type"))) {
				ToolResult result = new ToolResult(
					block.optString("tool_use_id", null),
					"completed",
					0,
					null,
					null,
					raw.optString("timestamp", "")
				);
				return toEvent("tool_result", toolResultToJson(result), raw);
			}
		}
		return null;
	}

	/**
	 * An "assistant" line contains message.content[] blocks that can be:
	 *  - type="text" → AssistantText
	 *  - type="tool_use" → ToolInvocation
	 *
	 * We return one event per line, with lists of texts and tool invocations.
	 */
	private static JSONObject parseAssistantLine(JSONObject raw) {
		JSONObject message = raw.optJSONObject("message");
		if (message == null) {
			return null;
		}

		JSONArray contentBlocks = message.optJSONArray("content");
		if (contentBlocks == null || contentBlocks.length() == 0) {
			return null;
		}

		String model = message.optString("model", "");
		String timestamp = raw.optString("timestamp", "");

		List<JSONObject> texts = new ArrayList<>();
		List<JSONObject> toolInvocations = new ArrayList<>();

		for (int i = 0; i < contentBlocks.length(); i++) {
			JSONObject block = contentBlocks.getJSONObject(i);
			String blockType = block.optString("type", "");

			if ("text".equals(blockType)) {
				AssistantText at = new AssistantText(
					block.optString("text", ""),
					model,
					timestamp
				);
				texts.add(assistantTextToJson(at));

			} else if ("tool_use".equals(blockType)) {
				JSONObject input = block.optJSONObject("input");
				ToolInvocation ti = new ToolInvocation(
					block.optString("id", ""),
					block.optString("name", ""),
					extractDescription(input),
					input != null ? input.optString("subagent_type", null) : null,
					timestamp
				);
				toolInvocations.add(toolInvocationToJson(ti));
			}
		}

		JSONObject data = new JSONObject();
		if (!texts.isEmpty()) {
			data.put("texts", new JSONArray(texts));
		}
		if (!toolInvocations.isEmpty()) {
			data.put("toolInvocations", new JSONArray(toolInvocations));
		}
		data.put("model", model);

		return toEvent("assistant", data, raw);
	}

	// --- Helpers to extract a useful description from tool input ---

	private static String extractDescription(JSONObject input) {
		if (input == null) return "";
		// Try description, then prompt, then file_path, then command
		if (input.has("description")) return input.getString("description");
		if (input.has("prompt")) return truncate(input.getString("prompt"), 200);
		if (input.has("file_path")) return input.getString("file_path");
		if (input.has("command")) return truncate(input.getString("command"), 200);
		return "";
	}

	private static String truncate(String s, int max) {
		if (s == null) return "";
		return s.length() <= max ? s : s.substring(0, max) + "...";
	}

	// --- Record → JSONObject conversions ---

	private static JSONObject userPromptToJson(UserPrompt p) {
		JSONObject j = new JSONObject();
		j.put("promptId", p.promptId() != null ? p.promptId() : JSONObject.NULL);
		j.put("text", p.text());
		j.put("timestamp", p.timestamp());
		return j;
	}

	private static JSONObject toolInvocationToJson(ToolInvocation ti) {
		JSONObject j = new JSONObject();
		j.put("toolUseId", ti.toolUseId());
		j.put("toolName", ti.toolName());
		j.put("description", ti.description());
		j.put("subagentType", ti.subagentType() != null ? ti.subagentType() : JSONObject.NULL);
		j.put("timestamp", ti.timestamp());
		return j;
	}

	private static JSONObject assistantTextToJson(AssistantText at) {
		JSONObject j = new JSONObject();
		j.put("text", at.text());
		j.put("model", at.model());
		j.put("timestamp", at.timestamp());
		return j;
	}

	private static JSONObject toolResultToJson(ToolResult tr) {
		JSONObject j = new JSONObject();
		j.put("toolUseId", tr.toolUseId() != null ? tr.toolUseId() : JSONObject.NULL);
		j.put("status", tr.status());
		j.put("durationMs", tr.durationMs());
		j.put("filePath", tr.filePath() != null ? tr.filePath() : JSONObject.NULL);
		j.put("timestamp", tr.timestamp());
		if (tr.stats() != null) {
			JSONObject s = new JSONObject();
			s.put("readCount", tr.stats().readCount());
			s.put("searchCount", tr.stats().searchCount());
			s.put("bashCount", tr.stats().bashCount());
			s.put("editFileCount", tr.stats().editFileCount());
			s.put("linesAdded", tr.stats().linesAdded());
			s.put("linesRemoved", tr.stats().linesRemoved());
			j.put("stats", s);
		}
		return j;
	}

	// --- Wrap data in a standard event envelope ---

	private static JSONObject toEvent(String eventType, JSONObject data, JSONObject raw) {
		JSONObject event = new JSONObject();
		event.put("event", eventType);
		event.put("uuid", raw.optString("uuid", ""));
		event.put("parentUuid", raw.has("parentUuid") && !raw.isNull("parentUuid")
				? raw.getString("parentUuid") : JSONObject.NULL);
		event.put("sessionId", raw.optString("sessionId", ""));
		event.put("data", data);
		return event;
	}
}
