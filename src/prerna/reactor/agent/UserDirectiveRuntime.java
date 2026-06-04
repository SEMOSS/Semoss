package prerna.reactor.agent;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.agent.mcp.MCPUtility;

public class UserDirectiveRuntime {

	private static final Logger LOGGER = LogManager.getLogger(UserDirectiveRuntime.class);

	private UserDirectiveRuntime() {

	}

	public static String renderPromotedFailureDirectiveAdditions(Room room) {
		try {
			if (room == null) {
				return "";
			}
			Set<String> canonicalNames = buildCanonicalToolNames(room);
			if (canonicalNames.isEmpty()) {
				return "";
			}
			Map<String, String> promotedDirectives = ModelInferenceLogsUtils.getPromotedToolFailureDirectives(canonicalNames);
			if (promotedDirectives.isEmpty()) {
				return "";
			}

			StringBuilder out = new StringBuilder();
			out.append("\n\n[LEARNED TOOL FAILURE DIRECTIVES]\n");
			out.append("There are promoted learned directives for these tools. ");
			out.append("Honor them unless the current request explicitly overrides one.\n");
			for (String toolName : canonicalNames) {
				String directive = promotedDirectives.get(toolName);
				if (directive == null || directive.isBlank()) {
					continue;
				}
				out.append("- For tool `").append(toolName).append("`: ").append(directive).append('\n');
			}
			out.append("[END LEARNED TOOL FAILURE DIRECTIVES]");
			return out.toString();
		} catch (Exception e) {
			LOGGER.warn("Failed to render promoted failure directive additions", e);
			return "";
		}
	}

	private static Set<String> buildCanonicalToolNames(Room room) {
		Set<String> seen = new LinkedHashSet<>();
		Map<String, Map<String, Object>> lookup = room.getToolLookupByLLMName();
		if (lookup == null) {
			return seen;
		}
		for (Map.Entry<String, Map<String, Object>> e : lookup.entrySet()) {
			String llmName = e.getKey();
			if (llmName == null || llmName.isBlank()) {
				continue;
			}
			String canonical = llmName;
			Map<String, Object> entry = e.getValue();
			if (entry != null) {
				Object meta = entry.get("_meta");
				if (meta instanceof Map<?, ?> metaMap) {
					Object fn = metaMap.get(MCPUtility.SMSS_FUNCTION_NAME);
					if (fn instanceof String && !((String) fn).isBlank()) {
						canonical = (String) fn;
					}
				}
			}
			seen.add(canonical);
		}
		return seen;
	}
}