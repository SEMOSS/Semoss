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

/**
 * Executes one automation node's operation, given all the context that node type needs.
 *
 * <p>One concrete implementation per {@code node.type} value (e.g. {@code WaitNodeExecutor},
 * {@code DatabaseEngineNodeExecutor}), resolved via a
 * {@code Map<String, IAutomationNodeExecutor>} registry in
 * {@link prerna.reactor.automation.TriggerAutomationReactor#executeSingleNode} instead of the
 * previous {@code if/else} chain keyed on {@code type}.
 *
 * <p>Mirrors the shape already established in this codebase for "one conceptual operation, many
 * type-specific implementations, resolved by a type key" - see
 * {@link prerna.engine.api.IModelEngine} (resolved via {@code Utility.getModel(engineId)}) and
 * {@link prerna.engine.api.IMCP}.
 *
 * <p>Implementations should be stateless and safe to share as a single static instance across
 * concurrent runs - all per-run/per-node state is passed in via {@link AutomationNodeContext}, not
 * held on the executor instance.
 */
public interface IAutomationNodeExecutor {

	/**
	 * Executes this node's operation and returns its raw output - the same shape previously
	 * returned by each {@code executeXNode} method (a String, a JSON string, or a
	 * {@code Map<String, Object>} that the caller will serialize). The caller
	 * ({@code executeSingleNode}) is responsible for applying the node's output transform,
	 * generating the preview, and checkpointing success/failure to the database - executors
	 * should not do any of that themselves.
	 *
	 * @param ctx the node's execution context - node definition, scope, config, and callbacks
	 *            for recursing into sibling/child nodes where the node type requires it
	 * @return the node's raw output
	 * @throws Exception on any failure - the caller catches and records it as a failed node,
	 *             following the same contract the previous {@code executeXNode} methods had
	 */
	Object execute(AutomationNodeContext ctx) throws Exception;
}
