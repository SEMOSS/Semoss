package prerna.reactor.agent;

public class ClaudeCodeTranscriptModels {

	public record TranscriptMessage(
			 String type,           // "user", "assistant", "attachment", "queue-operation", "last-prompt"
			 String uuid,
			 String parentUuid,
			 String timestamp,
			 String sessionId,
			 String promptId        // nullable - only on root user messages
			) {}

			//The user's original prompt (type="user" where content is a String)
			public record UserPrompt(
			 String promptId,
			 String text,
			 String timestamp
			) {}

			//Claude invoking a tool (parsed from assistant message.content where block.type="tool_use")
			public record ToolInvocation(
			 String toolUseId,      // "id" field - correlates to the tool_result
			 String toolName,       // "Agent", "Read", "Edit"
			 String description,    // extracted from input.description, input.prompt, or input.file_path
			 String subagentType,   // nullable - only for Agent tool (e.g. "Explore")
			 String timestamp
			) {}

			//Claude's text response (parsed from assistant message.content where block.type="text")
			public record AssistantText(
			 String text,
			 String model,
			 String timestamp
			) {}

			//Tool execution result (type="user" where toolUseResult is present)
			public record ToolResult(
			 String toolUseId,      // from sourceToolAssistantUUID or content[].tool_use_id
			 String status,         // "completed", etc.
			 long durationMs,
			 ToolStats stats,       // nullable
			 String filePath,       // nullable - for Read/Edit results
			 String content,        // nullable - the text content returned by the tool
			 String timestamp
			) {}

			public record ToolStats(
			 int readCount,
			 int searchCount,
			 int bashCount,
			 int editFileCount,
			 int linesAdded,
			 int linesRemoved
			) {}

}
