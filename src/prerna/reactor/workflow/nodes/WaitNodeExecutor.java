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
package prerna.reactor.workflow.nodes;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "wait" node: sleeps for the configured number of seconds.
 * The {@code seconds} value supports {@code ${var}} template substitution.
 * Maximum 3600 seconds (1 hour) per invocation.
 */
public final class WaitNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String nodeLabel = ctx.nodeLabel();

		String secondsTemplate = config.get("seconds") != null
				? config.get("seconds").toString() : "1";
		String resolved = WorkflowExecutionUtils.resolve(secondsTemplate, ctx.scope(), ctx.configMap());

		int seconds;
		try {
			seconds = Integer.parseInt(resolved.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Wait node \"" + nodeLabel +
					"\" - seconds value is not a valid integer after resolution: \"" + resolved + "\"");
		}
		seconds = Math.min(Math.max(seconds, 0), 3600);

		try {
			TimeUnit.SECONDS.sleep(seconds);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Wait node \"" + nodeLabel + "\" was interrupted");
		}

		return seconds + " seconds";
	}
}
