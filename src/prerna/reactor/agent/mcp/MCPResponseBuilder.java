package prerna.reactor.agent.mcp;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MCPResponseBuilder {

	public static final String SEMOSS_MULTIMODAL_TOOL_RESPONSE_KEY = "SEMOSSMultimodalToolResponse";

	/**
	 * Creates a text content part for {@link #response(Map[])}.
	 *
	 * @param text response text
	 * @return immutable text content part
	 */
	public static Map<String, Object> textPart(String text) {
		if (text == null || text.isBlank()) {
			throw new IllegalArgumentException("MCP response text must not be blank");
		}
		return Map.of("type", "text", "text", text);
	}

	/**
	 * Creates an image content part containing paths relative to the active room.
	 * The files are resolved and validated when the message is serialized for model
	 * execution.
	 *
	 * @param roomRelativePaths one or more image paths beneath the active room
	 * @return immutable image content part
	 */
	public static Map<String, Object> imagePart(String... roomRelativePaths) {
		if (roomRelativePaths == null || roomRelativePaths.length == 0) {
			throw new IllegalArgumentException("MCP response image paths must not be empty");
		}

		List<String> paths = new ArrayList<>(roomRelativePaths.length);
		for (String path : roomRelativePaths) {
			if (path == null || path.isBlank()) {
				throw new IllegalArgumentException("MCP response image paths must not be blank");
			}
			if (Path.of(path).isAbsolute()) {
				throw new IllegalArgumentException("MCP response image paths must be relative to the active room");
			}
			paths.add(path);
		}

		return Map.of("type", "image", "image", List.copyOf(paths));
	}

	/**
	 * Creates an ordered multimodal result for a Pixel reactor exposed as an MCP
	 * tool.
	 *
	 * @param parts ordered text and image parts
	 * @return immutable {@code SEMOSSMultimodalToolResponse} envelope
	 */
	public static Map<String, Object> response(List<Map<String, Object>> parts) {
		if (parts == null || parts.isEmpty()) {
			throw new IllegalArgumentException("MCP response must contain at least one part");
		}

		List<Map<String, Object>> orderedParts = new ArrayList<>(parts.size());
		for (Map<String, Object> part : parts) {
			if (part == null) {
				throw new IllegalArgumentException("MCP response parts must not be null");
			}
			orderedParts.add(Map.copyOf(part));
		}

		return Map.of(SEMOSS_MULTIMODAL_TOOL_RESPONSE_KEY, List.copyOf(orderedParts));
	}
}
