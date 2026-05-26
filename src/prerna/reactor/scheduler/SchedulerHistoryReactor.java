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
package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Utility;

public class SchedulerHistoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SchedulerHistoryReactor.class);

	public SchedulerHistoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.SORT.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.JOB_TAGS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		// start up scheduler if it isn't on
		SchedulerDatabaseUtility.startScheduler(scheduler);
		IRDBMSEngine schedulerDb = SchedulerDatabaseUtility.getSchedulerDB();

		List<String> jobTags = getJobTags();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				new QueryColumnSelector(SchedulerConstants.SMSS_JOB_RECIPES + "__" + SchedulerConstants.JOB_NAME));
		qs.addSelector(new QueryColumnSelector(SchedulerConstants.SMSS_JOB_RECIPES + "__" + SchedulerConstants.JOB_ID));
		qs.addSelector(new QueryColumnSelector(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.JOB_ID));
		qs.addSelector(
				new QueryColumnSelector(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.JOB_GROUP));
		qs.addSelector(new QueryColumnSelector(
				SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.EXECUTION_START));
		qs.addSelector(
				new QueryColumnSelector(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.EXECUTION_END));
		qs.addSelector(new QueryColumnSelector(
				SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.EXECUTION_DELTA));
		qs.addSelector(
				new QueryColumnSelector(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.SUCCESS));
		qs.addSelector(
				new QueryColumnSelector(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.IS_LATEST));
		qs.addSelector(new QueryColumnSelector(
				SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.SCHEDULER_OUTPUT));
		qs.addRelation(SchedulerConstants.SMSS_JOB_RECIPES + "__" + SchedulerConstants.JOB_ID,
				SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.JOB_ID, "left.outer.join");

		if (jobTags != null && !jobTags.isEmpty()) {
			// require specific job tags
			qs.addRelation(SchedulerConstants.SMSS_JOB_RECIPES + "__" + SchedulerConstants.JOB_ID,
					SchedulerConstants.SMSS_JOB_TAGS + "__" + SchedulerConstants.JOB_ID, "inner.join");
			for (String tag : jobTags) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
						SchedulerConstants.SMSS_JOB_TAGS + "__" + SchedulerConstants.JOB_TAG, "==", tag,
						PixelDataType.CONST_STRING));
			}
		}

		GenRowFilters additionalFilters = getFilters();
		if (additionalFilters != null) {
			qs.mergeExplicitFilters(additionalFilters);
		}
		List<IQuerySort> sorts = getSort();
		if (sorts != null) {
			qs.setOrderBy(sorts);
		} else {
			// set default
			qs.addOrderBy(SchedulerConstants.SMSS_AUDIT_TRAIL + "__" + SchedulerConstants.EXECUTION_START,
					QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC.toString());
		}
		qs.setLimit(getLong(ReactorKeysEnum.LIMIT.getKey(), -1L));
		qs.setOffSet(getLong(ReactorKeysEnum.OFFSET.getKey(), -1L));

		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(schedulerDb, qs);
		} catch (Exception e) {
			classLogger.error("Failed to query scheduler history from SMSS_AUDIT_TRAIL: {}", e.getMessage(), e);
			String message = e.getMessage();
			if (message == null || message.isEmpty()) {
				throw new IllegalArgumentException(message);
			} else {
				throw new IllegalArgumentException("An error occurred attempting to get your requests");
			}
		}
		BasicIteratorTask task = new BasicIteratorTask(qs, iterator);
		task.setNumCollect(-1);
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
	}

	////////////////////////////////////////////////////////////////////////////////////
	/*
	 * Additional inputs for filtering
	 */

	protected GenRowFilters getFilters() {
		GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.FILTERS.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			GenRowFilters filters = null;
			for (int i = 0; i < inputsGRS.size(); i++) {
				NounMetadata filterNoun = inputsGRS.getNoun(i);
				SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
				if (filters == null) {
					filters = qs.getCombinedFilters();
				} else {
					filters.merge(qs.getCombinedFilters());
				}
			}
			return filters;
		}
		return null;
	}

	protected List<IQuerySort> getSort() {
		GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.SORT.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata sortNoun = inputsGRS.getNoun(0);
			SelectQueryStruct qs = (SelectQueryStruct) sortNoun.getValue();
			List<IQuerySort> orderBy = qs.getOrderBy();
			return orderBy;
		}
		return null;
	}

	private List<String> getJobTags() {
		List<String> jobTags = null;
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.JOB_TAGS.getKey());
		if (grs != null && !grs.isEmpty()) {
			jobTags = new ArrayList<>();
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				jobTags.add(grs.get(i) + "");
			}
		}
		return jobTags;
	}
}
