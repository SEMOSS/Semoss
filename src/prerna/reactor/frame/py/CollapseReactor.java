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
package prerna.reactor.frame.py;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CollapseReactor extends AbstractPyFrameReactor {

	public CollapseReactor() {
		this.keysToGet = new String[] { "groupByColumn", ReactorKeysEnum.VALUE.getKey(),
				ReactorKeysEnum.DELIMITER.getKey(), ReactorKeysEnum.MAINTAIN_COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperFrameName = frame.getWrapperName();
		List<String> groupByCol = getGroupByCols();
		String valueCol = ", '" + this.keyValue.get(this.keysToGet[1]) + "'";
		String delim = ", '" + this.keyValue.get(this.keysToGet[2]) + "'";

		String groupByColsR = "[";
		// group by cols
		for (int i = 0; i < groupByCol.size(); i++) {
			String groupCol = groupByCol.get(i);
			if (i == 0) {
				groupByColsR = groupByColsR + "'" + groupCol + "'";
			} else {
				groupByColsR = groupByColsR + ", '" + groupCol + "'";
			}
		}
		groupByColsR += "]";

		// main cols
		// get columns to keep
		// convert to a list
		StringBuilder maintainCols = new StringBuilder("");
		HashSet<String> colsToKeep = getKeepCols();
		if (colsToKeep != null) {
			// merge columns
			maintainCols.append(", [");
			colsToKeep.addAll(groupByCol);

			Iterator<String> maintainIterator = colsToKeep.iterator();
			for (int maintainColIndex = 0; maintainIterator.hasNext(); maintainColIndex++) {
				String thisCol = maintainIterator.next();
				if (maintainColIndex > 0) {
					maintainCols.append(", ");
				}
				maintainCols.append("'").append(thisCol).append("'");
			}
			maintainCols.append("]");
		}

		String script = frame.getName() + " = " + wrapperFrameName + ".collapse(" + groupByColsR + valueCol + delim
				+ maintainCols + ")";
		frame.runScript(script);
		this.addExecutedCode(script);

		frame = (PandasFrame) recreateMetadata(frame);

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		return retNoun;
	}

	private List<String> getGroupByCols() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getGenRowStruct(this.keysToGet[0]);
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					colInputs.add(column);
				}
			}
		}
		return colInputs;
	}

	private HashSet<String> getKeepCols() {
		HashSet<String> colInputs = new HashSet<String>();
		GenRowStruct colGRS = this.store.getGenRowStruct(ReactorKeysEnum.MAINTAIN_COLUMNS.getKey());
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					colInputs.add(column);
				}
				return colInputs;
			}
		}
		return null;
	}
}
