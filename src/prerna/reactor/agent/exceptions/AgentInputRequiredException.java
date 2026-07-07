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
package prerna.reactor.agent.exceptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Thrown by {@link prerna.reactor.agent.runtime.HarnessToolExecutor} when the
 * model's tool-call batch contains one or more MCP tools whose
 * {@code SMSS_MCP_EXECUTION} is {@code "ask"}. The harness catches this to
 * transition the run to {@code INPUT_REQUIRED}, persist pending actions, and
 * release the worker thread. The run resumes when the user approves, edits,
 * rejects, or responds via {@code RunMCPTool} with agent-context parameters.
 *
 * <p>The exception carries the assistant {@code ResponseMessage} parent id and
 * only the pending tool-call descriptors that require user input, with their
 * enriched {@code _meta} so the harness can persist approval UI details.
 */
public class AgentInputRequiredException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private final String parentMessageId;
	private final List<Map<String, Object>> pendingToolCalls;

	/**
	 * @param parentMessageId   the assistant message id that contains the
	 *                          tool-call batch (used as {@code parentMessageId}
	 *                          when writing tool results back to the room)
	 * @param pendingToolCalls  one map per ask tool call, each carrying at least
	 *                          {@code id}, {@code name},
	 *                          {@code arguments}/{@code input}, and the
	 *                          enriched {@code _meta} from
	 *                          {@code Room.updateToolResponseMeta}
	 */
	public AgentInputRequiredException(String parentMessageId, List<Map<String, Object>> pendingToolCalls) {
		super("Agent run paused: " + (pendingToolCalls != null ? pendingToolCalls.size() : 0)
				+ " tool call(s) require user input");
		this.parentMessageId = parentMessageId;
		this.pendingToolCalls = pendingToolCalls != null
				? Collections.unmodifiableList(pendingToolCalls)
				: Collections.emptyList();
	}

	public String getParentMessageId() {
		return parentMessageId;
	}

	public List<Map<String, Object>> getPendingToolCalls() {
		return pendingToolCalls;
	}
}
