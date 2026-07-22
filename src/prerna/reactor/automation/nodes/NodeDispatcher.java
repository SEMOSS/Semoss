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
package prerna.reactor.automation.nodes;

import java.util.Map;

/**
 * Callback bound to {@code TriggerAutomationReactor.executeSingleNode}, allowing composite node
 * executors (e.g. {@code ConditionalNodeExecutor}, {@code WhileLoopNodeExecutor},
 * {@code TryCatchNodeExecutor}, {@code SwitchNodeExecutor}, {@code RetryNodeExecutor},
 * {@code ParallelNodeExecutor}) to recurse into a branch/loop/case's inner nodes without
 * depending on {@code TriggerAutomationReactor} directly.
 *
 * <p>{@code node}/{@code scope} are the only two arguments that vary per recursive call - the
 * rest of the calling node's context ({@code runId}, {@code configMap}, {@code ancestorProjectIds})
 * stays fixed across the recursion and is captured by the lambda this is bound to.
 */
@FunctionalInterface
public interface NodeDispatcher {

	/**
	 * Executes a single inner node exactly as {@code executeSingleNode} would for a top-level
	 * node - markNodeRunning, timing, output-transform, preview, checkpointing, and
	 * success/failure result building all happen inside this call, matching the contract
	 * every branch of the original {@code if/else} chain relied on implicitly.
	 *
	 * @param node  the inner node definition (from a {@code trueGraph}/{@code falseGraph}/
	 *              {@code subGraph}/{@code tryGraph}/{@code catchGraph}/case branch)
	 * @param scope the current execution scope - inner nodes read prior outputs from it and
	 *              (via the caller) may write their own output back into it
	 * @return the same node-result map shape {@code executeSingleNode} normally returns -
	 *         contains at minimum {@code STATUS}, and on success {@code outputValue}
	 */
	Map<String, Object> dispatch(Map<String, Object> node, Map<String, String> scope);
}
