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
package prerna.reactor.shortcuts.fileupload.job;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import prerna.om.Insight;

public class NodeExecutor {
	public static CompletableFuture<ExecutionContext> executeNode(WorkflowDefinition workflow, String nodeId,
			ExecutionContext ctx, Insight insight) {

		Node node = workflow.getNode(nodeId);

		if (node == null) {

			return CompletableFuture.completedFuture(ctx);
		}

		System.out.println("Executing Node : " + nodeId);

		return RetryExecutor
				.executeWithRetry(node.pixel, ctx, workflow.retryPolicy, workflow.workflowId, nodeId, insight)
				.thenCompose(execContext -> {

					String next = null;

					if (execContext != null && execContext instanceof ExecutionContext) {

						Map<String, Object> map = execContext.result;

						if (map.containsKey("nextNode") && map.get("nextNode") != null) {

							next = map.get("nextNode").toString();
						}
					}

					if (next == null) {
						next = node.next;
					}

					if (next == null) {
						return CompletableFuture.completedFuture(ctx);
					}

					return executeNode(workflow, next, ctx, insight);
				});
	}
}
