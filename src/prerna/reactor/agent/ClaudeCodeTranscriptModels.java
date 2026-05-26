/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent;

public class ClaudeCodeTranscriptModels {

	public record TranscriptMessage(String type, // "user", "assistant", "attachment", "queue-operation", "last-prompt"
			String uuid, String parentUuid, String timestamp, String sessionId, String promptId // nullable - only on
																								// root user messages
	) {
	}

	// The user's original prompt (type="user" where content is a String)
	public record UserPrompt(String promptId, String text, String timestamp) {
	}

	// Claude invoking a tool (parsed from assistant message.content where
	// block.type="tool_use")
	public record ToolInvocation(String toolUseId, // "id" field - correlates to the tool_result
			String toolName, // "Agent", "Read", "Edit"
			String description, // extracted from input.description, input.prompt, or input.file_path
			String subagentType, // nullable - only for Agent tool (e.g. "Explore")
			String timestamp) {
	}

	// Claude's text response (parsed from assistant message.content where
	// block.type="text")
	public record AssistantText(String text, String model, String timestamp) {
	}

	// Tool execution result (type="user" where toolUseResult is present)
	public record ToolResult(String toolUseId, // from sourceToolAssistantUUID or content[].tool_use_id
			String status, // "completed", etc.
			long durationMs, ToolStats stats, // nullable
			String filePath, // nullable - for Read/Edit results
			String content, // nullable - the text content returned by the tool
			String timestamp) {
	}

	public record ToolStats(int readCount, int searchCount, int bashCount, int editFileCount, int linesAdded,
			int linesRemoved) {
	}

	// Emitted when Claude Code halts because the SDK-configured max turns was hit
	public record MaxTurnsReached(int maxTurns, int turnCount, String timestamp) {
	}

}
