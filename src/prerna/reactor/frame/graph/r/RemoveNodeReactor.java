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
package prerna.reactor.frame.graph.r;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveNodeReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RemoveNodeReactor.class.getName();

	public RemoveNodeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(),
				ReactorKeysEnum.VALUE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String[] packages = new String[] { "igraph" };
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		Logger logger = getLogger(CLASS_NAME);

		ITableDataFrame frame = getFrame();
		if (!(frame instanceof TinkerFrame)) {
			throw new IllegalArgumentException("Frame must be a graph frame type");
		}
		TinkerFrame graph = (TinkerFrame) frame;
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
		if (!graph.isIGraphSynched()) {
			String wd = this.insight.getInsightFolder();
			iGraphUtilities.synchronizeGraphToR(graph, rJavaTranslator, graph.getName(), wd, logger);
		}

		String type = getColumn();
		List<Object> values = getValues();
		graph.remove(type, values);
		iGraphUtilities.removeNodeFromR(graph.getName(), rJavaTranslator, type, values, logger);
		return new NounMetadata(graph, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getColumn() {
		String fullUpdateCol = getStringFromKeyOrCurRow(this.keysToGet[1], 0);
		if (fullUpdateCol != null && !fullUpdateCol.isEmpty()) {
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define column type");
	}

	private List<Object> getValues() {
		GenRowStruct valuesGrs = this.store.getGenRowStruct(keysToGet[2]);
		if (valuesGrs == null || valuesGrs.isEmpty()) {
			throw new IllegalArgumentException("Need to define values");
		}
		return valuesGrs.getAllValues();
	}
}
