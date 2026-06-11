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
package prerna.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.insight.InsightUtility;

public abstract class TaskBuilderReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TaskBuilderReactor.class);

	private static final String CLASS_NAME = TaskBuilderReactor.class.getName();
	protected ITask task;
	protected List<NounMetadata> subAdditionalReturn;

	// This method is implemented by child classes, each class is responsible for
	// building different pieces of the task
	protected abstract void buildTask() throws Exception;

	@Override
	public NounMetadata execute() {
		this.task = getTask();
		try {
			buildTask();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		}
		// append onto the task
		return new NounMetadata(this.task, PixelDataType.TASK); // return the task
	}

	@Override
	public List<NounMetadata> getOutputs() {
		// all of the classes return the same thing
		// which is a TASK
		// this works because even if execute hasn't occurred yet
		// because the same preference exists for the task
		// and since out is called prior to update the planner
		// the cannot be null
		List<NounMetadata> outputs = new Vector<>();
		if (this.task != null) {
			NounMetadata output = new NounMetadata(task, PixelDataType.TASK);
			outputs.add(output);
		}
		return outputs;
	}

	/**
	 * Get the task for the task builder
	 * 
	 * @return
	 */
	protected ITask getTask() {
		ITask task = null;

		GenRowStruct grsTasks = this.store.getGenRowStruct(PixelDataType.TASK.getKey());
		// if we don't have jobs in the curRow, check if it exists in genrow under the
		// key job
		if (grsTasks != null && !grsTasks.isEmpty()) {
			task = (ITask) grsTasks.get(0);
		} else {
			List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
			if (tasks != null && !tasks.isEmpty()) {
				task = (ITask) tasks.get(0);
			}
		}

		if (task == null) {
			grsTasks = this.store.getGenRowStruct(PixelDataType.FORMATTED_DATA_SET.getKey());
			// if we don't have jobs in the curRow, check if it exists in genrow under the
			// key job
			if (grsTasks != null && !grsTasks.isEmpty()) {
				Object possibleT = grsTasks.get(0);
				if (possibleT instanceof ITask) {
					task = (ITask) possibleT;
				}
			} else {
				List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
				if (tasks != null && !tasks.isEmpty()) {
					Object possibleT = tasks.get(0);
					if (possibleT instanceof ITask) {
						task = (ITask) possibleT;
					}
				}
			}
		}

		if (task == null) {
			task = constructTaskFromQs();
			task.toOptimize(true);
		}

		Logger logger = this.getLogger(CLASS_NAME);
		task.setLogger(logger);
		return task;
	}

	/**
	 * Generate the task from the query struct
	 * 
	 * @return
	 */
	protected ITask constructTaskFromQs() {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
		// if we don't have tasks in the curRow, check if it exists in genrow under the
		// qs key
		if (grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			qs = (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				qs = (SelectQueryStruct) noun.getValue();
			}
		}

		// no qs either... i guess we will return an empty constant data task
		// this will just store the information
		if (qs == null) {
			// THIS SHOULD ONLY HAPPEN WHEN YOU CARE CALLING COLLECTGRAPH
			// SINCE THERE ARE NO SELECTORS
			ConstantDataTask cdTask = new ConstantDataTask();
			cdTask.setOutputData(new HashMap<Object, Object>());
			this.insight.getTaskStore().addTask(cdTask);
			return cdTask;
		}

		// set any additional details required
		if (noun != null) {
			this.subAdditionalReturn = noun.getAdditionalReturn();
		}

		return InsightUtility.constructTaskFromQs(insight, qs);
	}

	@Override
	public void mergeUp() {
		// if this has a QS
		// merge it back up
		if (parentReactor != null) {
			SelectQueryStruct qs = null;
			GenRowStruct grsQs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
			// if we don't have tasks in the curRow, check if it exists in genrow under the
			// qs key
			if (grsQs != null && !grsQs.isEmpty()) {
				NounMetadata noun = grsQs.getNoun(0);
				qs = (SelectQueryStruct) noun.getValue();
			} else {
				List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
				if (qsList != null && !qsList.isEmpty()) {
					NounMetadata noun = qsList.get(0);
					qs = (SelectQueryStruct) noun.getValue();
				}
			}

			if (qs != null) {
				NounMetadata data = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
				if (parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
						|| parentReactor instanceof GenericReactor) {
					parentReactor.getCurRow().add(data);
				} else {
					GenRowStruct parentQSInput = parentReactor.getNounStore()
							.makeGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
					parentQSInput.add(data);
				}
			}
		}
	}

}
