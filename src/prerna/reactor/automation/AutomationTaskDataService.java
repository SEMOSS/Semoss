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
 * -----------------------------------------------------------------------------
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
 ******************************************************************************/
package prerna.reactor.automation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.reactor.qs.SqlQueryReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;

/**
 * Owns task-backed data used by Automation nodes.
 *
 * <p>
 * Query authorization and SQL routing remain in {@link SqlQueryReactor}. This
 * service changes only the transport: the resulting {@link ITask} stays in the
 * run Insight and downstream Python receives an opaque
 * {@link AutomationDataReference} instead of a materialized result set.
 */
final class AutomationTaskDataService {
	private static final Logger classLogger = LogManager.getLogger(AutomationTaskDataService.class);

	private AutomationTaskDataService() {
	}

	/**
	 * Executes one guarded SQL read and retains its lazy task in the execution
	 * Insight.
	 */
	static AutomationDataReference createQuery(Insight insight, AutomationDataOwner owner, String databaseId,
			String query, int maxRows) {
		validateExecutionInsight(insight, owner.runId());
		if (maxRows < AutomationConstants.DB_QUERY_MIN_LIMIT
				|| maxRows > AutomationConstants.DB_QUERY_MAX_LIMIT) {
			throw new IllegalArgumentException("Automation database query limit must be between "
					+ AutomationConstants.DB_QUERY_MIN_LIMIT + " and " + AutomationConstants.DB_QUERY_MAX_LIMIT + ".");
		}

		SqlQueryReactor queryReactor = new SqlQueryReactor();
		queryReactor.setInsight(insight);
		queryReactor.setNounStore(queryStore(databaseId, query, maxRows));
		NounMetadata result = queryReactor.execute();
		if (!(result.getValue() instanceof BasicIteratorTask task)) {
			closeQuietly(result.getValue());
			throw new IllegalStateException("Automation database query did not return a lazy query task.");
		}

		String referenceId = UUID.randomUUID().toString();
		try {
			// The configured limit remains the owner-visible maximum. Disabling the
			// iterator's one-shot collect counter lets bounded page reads reset safely.
			task.setNumCollect(maxRows);
			task.setCollectLimit(-1);
			long availableRows = TaskUtility.getNumRows(task);
			if (availableRows < 0 || (availableRows == 0 && task.hasNext())) {
				throw new IllegalStateException("Unable to determine the Automation query row count.");
			}
			task.setNumRows(Math.min(availableRows, maxRows));
			task.setId(referenceId);
			insight.getTaskStore().addTask(referenceId, task);
			AutomationDataReference reference = new AutomationDataReference(
					AutomationDataReference.CURRENT_SCHEMA_VERSION, referenceId,
					AutomationValueType.DATASET);
			AutomationRunDataRegistry.register(insight, reference, owner, AutomationRunDataRegistry.Backing.TASK,
					referenceId);
			return reference;
		} catch (Exception e) {
			if (insight.getTaskStore().getTask(referenceId) == task) {
				insight.getTaskStore().removeTask(referenceId);
			} else {
				closeQuietly(task);
			}
			if (e instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			throw new IllegalStateException("Unable to prepare Automation query data.", e);
		}
	}

	/** Returns one deterministic, bounded page from a retained query task. */
	static AutomationDataPage readPage(Insight insight, AutomationDataOwner owner, AutomationDataReference reference,
			int offset,
			int limit) {
		validateExecutionInsight(insight, owner.runId());
		if (reference.valueType() != AutomationValueType.DATASET) {
			throw new IllegalArgumentException("Automation task data must be a dataset.");
		}
		if (offset < 0 || limit < 1 || limit > AutomationConstants.INTERNAL_DATA_PAGE_LIMIT) {
			throw new IllegalArgumentException("Automation task data page has invalid bounds.");
		}

		AutomationRunDataRegistry.Entry entry = AutomationRunDataRegistry.require(insight, reference, owner);
		if (entry.backing() != AutomationRunDataRegistry.Backing.TASK) {
			throw new IllegalArgumentException("Automation data reference is not backed by a query task.");
		}
		ITask stored = insight.getTaskStore().getTask(entry.resourceId());
		if (!(stored instanceof BasicIteratorTask task)) {
			throw new IllegalStateException("Automation task data is no longer available in this run workspace.");
		}
		return readPage(task, offset, limit);
	}

	private static AutomationDataPage readPage(BasicIteratorTask task, int offset, int limit) {
		synchronized (task) {
			long total = task.getNumRows();
			int maxRows = (int) Math.min(task.getNumCollect(), total);
			if (offset > maxRows) {
				throw new IllegalArgumentException("Automation task data offset exceeds the configured query limit.");
			}
			try {
				long currentOffset = ((AbstractTask) task).getInternalOffset();
				if (currentOffset > offset) {
					task.reset();
					currentOffset = 0;
				}
				while (currentOffset < offset && currentOffset < maxRows && task.hasNext()) {
					task.next();
					currentOffset++;
				}

				int pageLimit = Math.min(limit, maxRows - offset);
				List<String> headers = new ArrayList<>();
				List<List<Object>> rows = new ArrayList<>(pageLimit);
				while (rows.size() < pageLimit && task.hasNext()) {
					IHeadersDataRow row = task.next();
					if (headers.isEmpty()) {
						headers.addAll(Arrays.asList(row.getHeaders()));
					}
					rows.add(new ArrayList<>(Arrays.asList(row.getValues())));
				}
				boolean hasMore = offset + rows.size() < maxRows && task.hasNext();
				return new AutomationDataPage(AutomationDataPage.Kind.TABLE, AutomationValueType.DATASET, offset, limit,
						rows.size(), total, hasMore, headers, rows, null);
			} catch (Exception e) {
				throw new IllegalStateException("Unable to read Automation query data.", e);
			}
		}
	}

	private static NounStore queryStore(String databaseId, String query, int maxRows) {
		NounStore store = new NounStore("SqlQuery");
		addLiteral(store, ReactorKeysEnum.DATABASE.getKey(), databaseId);
		addLiteral(store, ReactorKeysEnum.QUERY_KEY.getKey(), query);
		GenRowStruct limit = store.makeGenRowStruct(ReactorKeysEnum.LIMIT.getKey());
		limit.add(new NounMetadata(maxRows, PixelDataType.CONST_INT));
		return store;
	}

	private static void addLiteral(NounStore store, String key, String value) {
		GenRowStruct row = store.makeGenRowStruct(key);
		row.add(new NounMetadata(value, PixelDataType.CONST_STRING));
	}

	private static void validateExecutionInsight(Insight insight, String runId) {
		if (runId == null || runId.isBlank() || !AutomationRunExecutionService.isExecutionInsight(insight, runId)) {
			throw new IllegalArgumentException("Automation task data is available only inside its execution run.");
		}
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !String.valueOf(run.get(AutomationConstants.PROJECT_ID)).equals(insight.getProjectId())) {
			throw new IllegalArgumentException("Automation run was not found for this execution workspace.");
		}
	}

	private static void closeQuietly(Object value) {
		if (value instanceof ITask task) {
			try {
				task.close();
			} catch (IOException e) {
				classLogger.warn("Unable to close rejected Automation query task '{}'.", task.getId(), e);
			}
		}
	}
}
