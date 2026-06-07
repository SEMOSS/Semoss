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
package prerna.reactor.frame.nativeframe;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.imports.NativeImporter;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class NativeCollectNewColReactor extends TaskBuilderReactor {

	private static final Logger classLogger = LogManager.getLogger(NativeCollectNewColReactor.class);

	public NativeCollectNewColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String warning = null;

		if (!((this.task = getTask()) instanceof BasicIteratorTask)) {
			throw new IllegalArgumentException("Can only add a new column using a basic query on a frame");
		}

		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask) this.task).getQueryStruct();
		NativeFrame frame = (NativeFrame) sqs.getFrame();

		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = null;

		try {
			// convert to to the physical structure
			pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		} catch (Exception ex) {
			return getWarning(
					"Calculation is using columns that do not exist in the frame. Cannot perform this operation");
		}

		if (pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0) {
			warning = "You are applying a calculation while there are filters, this is not recommended and can lead to unpredictable results";
			pqs.ignoreFilters = true;
		}

		// there should be only one selector
		List<IQuerySelector> allSelectors = sqs.getSelectors();
		if (allSelectors.size() == 0) {
			throw new IllegalArgumentException("No new columns to add");
		}

		NativeImporter importer;
		try {
			// set the engine id for the sqs to be that of the native frame
			pqs.setEngineId(frame.getEngineId());
			// now we can import without the importer needed to be modified
			importer = new NativeImporter(frame, pqs, ((BasicIteratorTask) task).getIterator());
			importer.insertData();
		} catch (Exception e) {
			classLogger.error("Failed to collect and append calculated columns into native frame {}.", frame.getName(),
					e);
			throw new IllegalArgumentException(e.getMessage());
		}

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(getSuccess("Added Col " + allSelectors.get(0).getAlias()));
		if (warning != null) {
			noun.addAdditionalReturn(getWarning(warning));
		}

		return noun;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null && !outputs.isEmpty()) {
			return outputs;
		}

		outputs = new ArrayList<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	@Override
	protected void buildTask() {
		// do nothing

	}
}
