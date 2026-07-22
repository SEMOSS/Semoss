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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Callback bound to {@code TriggerAutomationReactor.executeNodes}, used by
 * {@code SubAutomationNodeExecutor} to run a target project's entire automation graph to completion
 * as a nested, synchronous child run - distinct from {@link NodeDispatcher}, which runs a single
 * inner node.
 *
 * <p>This is real orchestration (its own cancellation flag, its own heartbeat, its own DB run
 * row/active-run claim) and intentionally stays owned by {@code TriggerAutomationReactor} rather
 * than being reimplemented by the node-executor layer - see the "what does NOT change" scoping
 * note on ticket #2746.
 */
@FunctionalInterface
public interface ChildAutomationRunner {

	/**
	 * Runs an already-claimed, already-topo-sorted child automation run to completion and returns
	 * its final run result (the same shape {@code buildRunResult} produces for a top-level run).
	 *
	 * @param childRunId          the child run's id (already inserted into {@code AUTOMATION_RUNS}
	 *                             and already holding the active-run claim for {@code targetProjectId})
	 * @param targetProjectId     the project id whose automation is being run
	 * @param orderedNodes        the child automation's nodes, already topologically sorted
	 * @param configMap           the child project's {@code automation-config.json} key/value pairs
	 * @param priorOutputs        prior node outputs to skip on resume - empty for a fresh
	 *                             sub-automation call, never itself resumable independently
	 * @param extraInitialScope   the resolved {@code inputMapping} values to seed into the
	 *                             child's initial scope
	 * @param ancestorProjectIds  the chain of project ids already executing on this call stack,
	 *                             including {@code targetProjectId} - used for the
	 *                             self/transitive-call cycle guard on any further nested calls
	 * @return the child run's final result map, containing at minimum {@code STATUS}
	 */
	Map<String, Object> run(String childRunId, String targetProjectId,
			List<Map<String, Object>> orderedNodes, Map<String, String> configMap,
			Map<String, String> priorOutputs, Map<String, String> extraInitialScope,
			Set<String> ancestorProjectIds);
}
