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
package prerna.reactor.export;

import java.awt.Color;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.export.graph.GraphExporterFactory;
import prerna.ds.export.graph.IGraphExporter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CollectGraphReactor extends CollectReactor {

	/**
	 * This class is responsible for collecting all graph data from a frame
	 */

	public CollectGraphReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.task = getTask();

		ITableDataFrame frame = getFrame();
		IGraphExporter exporter;

		// check if the user has defined their own color scheme in the insight
		NounMetadata graphMetadata = this.insight.getVarStore().get("GRAPH_COLORS");
		if (graphMetadata != null) {
			Map<String, Color> colorsMap = (Map<String, Color>) graphMetadata.getValue();
			exporter = GraphExporterFactory.getExporter(frame, colorsMap);
		} else {
			exporter = GraphExporterFactory.getExporter(frame);
		}
		Map<String, Object> collectedData = new HashMap<String, Object>(7);
		collectedData.put("data", exporter.getData());

		Map<String, Object> formatMap = new HashMap<String, Object>();
		formatMap.put("type", "GRAPH");
		collectedData.put("format", formatMap);
		collectedData.put("numCollected", "-1");
		collectedData.put("headerInfo", getHeaderInfo(frame));
		collectedData.put("taskOptions", this.task.getTaskOptions().getOptions());
		collectedData.put("sortInfo", this.task.getSortInfo());
		collectedData.put("filterInfo", this.task.getFilterInfo());
		collectedData.put("taskId", this.task.getId());
		collectedData.put("sources", getSources(frame));

		NounMetadata result = new NounMetadata(collectedData, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		return result;
	}

	private List<Map<String, Object>> getHeaderInfo(ITableDataFrame frame) {
		// TODO: this is dumb
		// why did I not make everything consistent...
		// why am i dumb
		List<Map<String, Object>> x = (List<Map<String, Object>>) frame.getMetaData().getTableHeaderObjects()
				.get("headers");
		for (Map<String, Object> val : x) {
			val.put("alias", val.remove("displayName"));
		}
		return x;
	}

	private List<Map<String, Object>> getSources(ITableDataFrame frame) {
		Map<String, Object> sourceMap = new HashMap<>();
		sourceMap.put("name", frame.getName());
		sourceMap.put("type", AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME);
		List<Map<String, Object>> sources = new ArrayList<>();
		sources.add(sourceMap);
		return sources;
	}

	private ITableDataFrame getFrame() {
		// try the key
		GenRowStruct fGrs = store.getGenRowStruct(this.keysToGet[0]);
		if (fGrs != null && !fGrs.isEmpty()) {
			return (ITableDataFrame) fGrs.get(0);
		}

		// try the cur row
		List<Object> allNumericInputs = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if (allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return (ITableDataFrame) allNumericInputs.get(0);
		}

		return (ITableDataFrame) this.insight.getDataMaker();
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null) {
			return outputs;
		}

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}
}
