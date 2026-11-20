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

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.util.insight.InsightUtility;

public class ConvertToQueryReactor extends AbstractReactor {

	private static final String IGNORE_STATE_KEY = "ignoreState";

	public ConvertToQueryReactor() {
		this.keysToGet = new String[] { IGNORE_STATE_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Boolean addInsightState = !Boolean.parseBoolean(keyValue.get(this.keysToGet[0]) + "");
		String query = null;

		SelectQueryStruct qs = getQs();
		if (qs == null) {
			ITask task = getTask();
			if (task != null && task instanceof BasicIteratorTask) {
				qs = ((BasicIteratorTask) task).getQueryStruct();
			}
		}

		if (qs == null) {
			throw new NullPointerException("Must input a query struct");
		}

		// if query defined
		// just grab it
		if (qs instanceof HardSelectQueryStruct) {
			query = ((HardSelectQueryStruct) qs).getQuery();
			return new NounMetadata(query, PixelDataType.CONST_STRING);
		}

		if (addInsightState) {
			InsightUtility.fillQsReferencesAndMergeOptions(this.insight, qs);
		}

		// else, we grab the interpreter
		// from the engine or frame
		// if frame - must convert to physical from alias
		IQueryInterpreter interp = null;
		if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			interp = qs.retrieveQueryStructEngine().getQueryInterpreter();
		} else if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			ITableDataFrame frame = qs.getFrame();
			interp = frame.getQueryInterpreter();
			if (frame instanceof NativeFrame) {
				qs = ((NativeFrame) frame).prepQsForExecution(qs);
			} else {
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			}
		} else {
			throw new IllegalArgumentException("Cannot generate a query for this source");
		}

		interp.setQueryStruct(qs);
		// grab the query
		query = interp.composeQuery();
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}

	private SelectQueryStruct getQs() {
		GenRowStruct grsQs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
		NounMetadata noun;
		// if we don't have tasks in the curRow, check if it exists in genrow under the
		// qs key
		if (grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			return (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				return (SelectQueryStruct) noun.getValue();
			}
		}

		return null;
	}

	private ITask getTask() {
		GenRowStruct grsTask = this.store.getGenRowStruct(PixelDataType.TASK.getKey());
		NounMetadata noun;
		// if we don't have tasks in the curRow, check if it exists in genrow under the
		// task key
		if (grsTask != null && !grsTask.isEmpty()) {
			noun = grsTask.getNoun(0);
			return (ITask) noun.getValue();
		} else {
			List<NounMetadata> taskList = this.curRow.getNounsOfType(PixelDataType.TASK);
			if (taskList != null && !taskList.isEmpty()) {
				noun = taskList.get(0);
				return (ITask) noun.getValue();
			}
		}

		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(IGNORE_STATE_KEY)) {
			return "Boolean flag to ignore/not add the frame and panel level options to the query struct";
		}
		return super.getDescriptionForKey(key);
	}

}
