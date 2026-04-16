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
 *   <li>"user" either a user prompt or a tool result</li>
 *   <li>"assistant" text and/or tool invocations</li>
 *   <li>"queue-operation", "last-prompt", "attachment" metadata (skipped)</li>
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

		if (content instanceof JSONArray) {
			return parseToolResultFromContent(raw, (JSONArray) content);
		}

		return null;
	}

	/**
	 * Parse a tool result from the toolUseResult field.
	 * Handles both JSONObject format (with status/totalDurationMs/toolStats)
	 * and JSONArray format (array of content blocks with type/text).
	 */
	private static JSONObject parseToolResult(JSONObject raw) {
		// Get tool_use_id and text content from message.content array if present
		String toolUseId = null;
		String contentText = null;
		JSONObject message = raw.optJSONObject("message");
		if (message != null) {
			JSONArray content = message.optJSONArray("content");
			if (content != null && content.length() > 0) {
				JSONObject firstBlock = content.getJSONObject(0);
				toolUseId = firstBlock.optString("tool_use_id", null);
				// Extract text from nested content blocks inside the tool_result
				contentText = extractTextFromToolResultBlock(firstBlock);
			}
		}

		String status = "completed";
		long durationMs = 0;
		ToolStats stats = null;
		String filePath = null;

		// toolUseResult can be a JSONObject (with status/duration) or a JSONArray (content blocks)
		Object turRaw = raw.opt("toolUseResult");
		if (turRaw instanceof JSONObject) {
			JSONObject tur = (JSONObject) turRaw;
			status = tur.optString("status", "completed");
			durationMs = tur.optLong("totalDurationMs", 0);

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

			// Extract filePath - direct field (Edit results) or nested in file object (Read results)
			filePath = tur.optString("filePath", null);
			if (filePath == null) {
				JSONObject file = tur.optJSONObject("file");
				if (file != null) {
					filePath = file.optString("filePath", null);
				}
			}

			// If we didn't get content from message.content, try multiple fallbacks
			if (contentText == null) {
				// Direct text field
				contentText = tur.optString("text", null);
			}
			if (contentText == null) {
				// Content array (Agent tool results have content: [{type:"text", text:"..."}])
				JSONArray turContent = tur.optJSONArray("content");
				if (turContent != null) {
					contentText = extractTextFromContentArray(turContent);
				}
			}
			if (contentText == null) {
				// Nested file content (Read tool results have file.content)
				JSONObject file = tur.optJSONObject("file");
				if (file != null && file.has("content") && !file.isNull("content")) {
					contentText = file.optString("content", null);
				}
			}
			if (contentText == null) {
				// Last resort: serialize the entire toolUseResult so no data is lost
				contentText = tur.toString();
			}
		} else if (turRaw instanceof JSONArray) {
			// toolUseResult is an array of content blocks, e.g. [{type:"text", text:"..."}]
			JSONArray turArray = (JSONArray) turRaw;
			contentText = extractTextFromContentArray(turArray);
		}

		ToolResult result = new ToolResult(
			toolUseId,
			status,
			durationMs,
			stats,
			filePath,
			contentText,
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
				String contentText = extractTextFromToolResultBlock(block);

				ToolResult result = new ToolResult(
					block.optString("tool_use_id", null),
					"completed",
					0,
					null,
					null,
					contentText,
					raw.optString("timestamp", "")
				);
				return toEvent("tool_result", toolResultToJson(result), raw);
			}
		}
		return null;
	}

	/**
	 * Extract text content from a tool_result content block.
	 * The block may have:
	 *   - a nested "content" array with {type:"text", text:"..."} entries (Agent tool results)
	 *   - a "content" field that is a plain String (Read/Edit tool results)
	 *   - a direct "text" field
	 */
	private static String extractTextFromToolResultBlock(JSONObject block) {
		// Check for nested content array: content: [{type:"text", text:"..."}]
		JSONArray nestedContent = block.optJSONArray("content");
		if (nestedContent != null) {
			return extractTextFromContentArray(nestedContent);
		}

		// Check for content as a plain string (Read/Edit tools return this)
		Object contentObj = block.opt("content");
		if (contentObj instanceof String) {
			String s = (String) contentObj;
			return s.isEmpty() ? null : s;
		}

		// Check for a direct text field
		if (block.has("text") && !block.isNull("text")) {
			return block.getString("text");
		}

		return null;
	}

	/**
	 * Extract and concatenate all meaningful entries from a content array.
	 *
	 * Recognized entry types:
	 *   - type="text"           → appends the text value directly
	 *   - type="tool_reference" → appends "[Tool: {tool_name}]"
	 */
	private static String extractTextFromContentArray(JSONArray contentArray) {
		if (contentArray == null || contentArray.length() == 0) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < contentArray.length(); i++) {
			JSONObject entry = contentArray.optJSONObject(i);
			if (entry == null) continue;

			String entryType = entry.optString("type", "");
			String piece = null;

			if ("text".equals(entryType)) {
				String text = entry.optString("text", "");
				if (!text.isEmpty()) {
					piece = text;
				}
			} else if ("tool_reference".equals(entryType)) {
				String toolName = entry.optString("tool_name", "");
				if (!toolName.isEmpty()) {
					piece = "[Tool: " + toolName + "]";
				}
			}

			if (piece != null) {
				if (sb.length() > 0) sb.append("\n");
				sb.append(piece);
			}
		}
		return sb.length() > 0 ? sb.toString() : null;
	}

	/**
	 * An "assistant" line contains message.content[] blocks that can be:
	 *  - type="text" AssistantText
	 *  - type="tool_use" ToolInvocation
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
		j.put("content", tr.content() != null ? tr.content() : JSONObject.NULL);
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
