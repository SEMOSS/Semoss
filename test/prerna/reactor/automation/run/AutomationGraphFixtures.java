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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Builds small, valid Automation graphs for tests that need real validated definitions. */
final class AutomationGraphFixtures {

	private AutomationGraphFixtures() {
	}

	static Map<String, Object> node(String id, String label, String type, Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, id);
		node.put(AutomationConstants.NODE_FIELD_TYPE, type);
		node.put(AutomationConstants.NODE_FIELD_LABEL, label);
		node.put("position", Map.of("x", 0, "y", 0));
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	/** A generated transform whose single output field carries {@code value}. */
	static Map<String, Object> transform(String id, String label, String value) {
		Map<String, Object> node = node(id, label, AutomationConstants.NODE_DATA_TRANSFORM,
				Map.of("fields", List.of(Map.of("name", "message", "node", Map.of("kind", "value", "value", value)))));
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, id + "_out");
		return node;
	}

	static Map<String, Object> python(String id, String label) {
		Map<String, Object> node = node(id, label, AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of());
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, id + "_out");
		return node;
	}

	static Map<String, Object> edge(String from, String to) {
		Map<String, Object> edge = new LinkedHashMap<>();
		edge.put(AutomationConstants.NODE_FIELD_ID, "e-" + from + "-" + to);
		edge.put(AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL);
		edge.put(AutomationConstants.EDGE_FIELD_SOURCE, from);
		edge.put(AutomationConstants.EDGE_FIELD_SOURCE_PORT, AutomationConstants.CONTROL_PORT_OUT);
		edge.put(AutomationConstants.EDGE_FIELD_TARGET, to);
		edge.put(AutomationConstants.EDGE_FIELD_TARGET_PORT, AutomationConstants.CONTROL_PORT_IN);
		return edge;
	}

	/** A for-each loop over {@code items}, one item per pass, with the given body. */
	static Map<String, Object> forEachLoop(String id, String label, List<Object> items,
			List<Map<String, Object>> bodyNodes, List<Map<String, Object>> bodyEdges) {
		Map<String, Object> loop = node(id, label, AutomationConstants.NODE_CONTROL_LOOP,
				Map.of(AutomationConstants.CONFIG_LOOP_MODE, AutomationConstants.LOOP_MODE_FOR_EACH,
						AutomationConstants.CONFIG_LOOP_ITEMS, items, AutomationConstants.CONFIG_LOOP_BATCH_SIZE, 1,
						AutomationConstants.CONFIG_LOOP_MAX_ITERATIONS, 10));
		loop.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, id + "_out");
		loop.put(AutomationConstants.NODE_FIELD_BODY,
				Map.of(AutomationConstants.DOC_NODES, bodyNodes, AutomationConstants.DOC_EDGES, bodyEdges));
		return loop;
	}

	static String document(List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
		Map<String, Object> graph = new LinkedHashMap<>();
		graph.put(AutomationConstants.DOC_NODES, nodes);
		graph.put(AutomationConstants.DOC_EDGES, edges);
		Map<String, Object> document = new LinkedHashMap<>();
		document.put(AutomationConstants.DOC_FORMAT_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION);
		document.put(AutomationConstants.DOC_DESCRIPTION, "");
		document.put(AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(Map.of("id", "manual", "type", "manual")));
		document.put(AutomationConstants.DOC_GRAPH, graph);
		return AutomationRuntimeUtils.GSON.toJson(document);
	}
}
