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
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;

public class CollectReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private static final Logger classLogger = LogManager.getLogger(CollectReactor.class);

	private int limit = 0;

	public CollectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.INCLUDE_META_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.task = getTask();

		this.limit = getTotalToCollect();
		this.task.setNumCollect(this.limit);
		try {
			buildTask();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		}

		PixelOperationType retOpType = PixelOperationType.TASK_DATA;
		// this is the second place I need to change
		TaskOptions ornamnetOptions = genOrnamentTaskOptions();
		if (ornamnetOptions != null || (task.getTaskOptions() != null && task.getTaskOptions().isOrnament())) {
			this.task.setTaskOptions(ornamnetOptions);
			retOpType = PixelOperationType.PANEL_ORNAMENT_DATA;
		}

		// this is causing more confusion and breaks facet
//		if(this.task.getTaskOptions() == null) {
//			// I am setting the panel id on the task options and getting it from here
//			TaskOptions taskOptions = this.insight.getLastTaskOptions();
//			if(taskOptions != null) {
//				Set<String> pIds = taskOptions.getPanelIds();
//				String panelId = pIds.iterator().next();
//				String layout = taskOptions.getLayout(panelId);
//				
//				if(this.task instanceof BasicIteratorTask) {
//					SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
//					if(!qs.getSelectors().isEmpty()) {
//						TaskOptions newTOptions = AutoTaskOptionsHelper.getAutoOptions(qs, panelId, layout);
//						this.task.setTaskOptions(newTOptions);
//					}
//				}
//				
//				if(this.task.getTaskOptions() == null) {
//					this.task.setTaskOptions(this.insight.getLastTaskOptions());
//				}
//			}
//		}

		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, retOpType);
	}

	@Override
	protected void buildTask() throws Exception {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if (this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}

	private TaskOptions genOrnamentTaskOptions() {
		if (this.subAdditionalReturn != null && this.subAdditionalReturn.size() == 1) {
			NounMetadata noun = this.subAdditionalReturn.get(0);
			if (noun.getNounType() == PixelDataType.ORNAMENT_MAP) {
				// we will use this map as task options
				TaskOptions options = new TaskOptions((Map<String, Object>) noun.getValue());
				options.setOrnament(true);
				return options;
			}
		}
		return null;
	}

	// returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getGenRowStruct(keysToGet[1]);
		if (numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}

		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if (allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}

		// default to 500
		return 500;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null && !outputs.isEmpty()) {
			return outputs;
		}

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
