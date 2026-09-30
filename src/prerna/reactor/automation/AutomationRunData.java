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
 *******************************************************************************/
package prerna.reactor.automation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;

/**
 * Registers opaque Automation values in the Insight that owns one execution.
 *
 * <p>
 * The public reference deliberately contains no storage details. This private
 * registry binds the reference to its project, run, creator, and backing store,
 * while the existing Insight TaskStore and Python runtime continue to own the
 * actual values and their lifecycle.
 */
final class AutomationRunData {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunData.class);
	private static final String REGISTRY_PREFIX = "__automation_data_reference__";

	private AutomationRunData() {
	}

	/** Retains an existing bounded task without recreating its query or policy. */
	static AutomationDataReference retainTask(Insight insight, String runId, ITask task) {
		RunOwner owner = validateExecutionInsight(insight, runId);
		if (!(task instanceof BasicIteratorTask iteratorTask)) {
			throw new IllegalArgumentException("Automation can retain only a row-oriented iterator task.");
		}
		int maximumRows = iteratorTask.getNumCollect();
		if (maximumRows < 1) {
			throw new IllegalArgumentException("Automation can retain only a task with a positive row limit.");
		}

		String referenceId = UUID.randomUUID().toString();
		try {
			// Paging enforces the task's configured maximum. Disabling the monotonic
			// collection counter permits a bounded earlier page after task.reset().
			iteratorTask.setCollectLimit(-1);
			long availableRows = TaskUtility.getNumRows(iteratorTask);
			if (availableRows < 0 || (availableRows == 0 && iteratorTask.hasNext())) {
				throw new IllegalStateException("Automation task did not provide a reliable row count.");
			}
			iteratorTask.setNumRows(Math.min(availableRows, maximumRows));
			iteratorTask.setId(referenceId);
			insight.getTaskStore().addTask(referenceId, iteratorTask);

			AutomationDataReference reference = new AutomationDataReference(
					AutomationDataReference.CURRENT_SCHEMA_VERSION, referenceId);
			put(insight, owner, reference, Backing.TASK);
			return reference;
		} catch (RuntimeException e) {
			if (insight.getTaskStore().getTask(referenceId) == iteratorTask) {
				insight.getTaskStore().removeTask(referenceId);
			} else {
				closeQuietly(iteratorTask);
			}
			throw e;
		} catch (Exception e) {
			if (insight.getTaskStore().getTask(referenceId) == iteratorTask) {
				insight.getTaskStore().removeTask(referenceId);
			} else {
				closeQuietly(iteratorTask);
			}
			throw new IllegalStateException("Unable to retain Automation task data.", e);
		}
	}

	/** Registers references created by the run Insight's Python value store. */
	static void registerPythonReferences(Insight insight, String runId, Object value) {
		List<AutomationDataReference> references = new ArrayList<>();
		collectReferences(value, references);
		if (references.isEmpty()) {
			return;
		}
		RunOwner owner = validateExecutionInsight(insight, runId);
		for (AutomationDataReference reference : references) {
			RegistryEntry existing = entry(insight, reference);
			if (existing == null) {
				put(insight, owner, reference, Backing.PYTHON);
			} else {
				validateOwner(owner, existing);
			}
		}
	}

	/** Returns whether the reference is backed by an Insight task. */
	static boolean isTaskBacked(Insight insight, String runId, AutomationDataReference reference) {
		return resolve(insight, runId, reference).backing() == Backing.TASK;
	}

	/** Returns one deterministic, bounded page from a task in the run Insight. */
	static Map<String, Object> readTaskPage(Insight insight, String runId, AutomationDataReference reference,
			int offset, int limit) {
		RegistryEntry resolved = resolve(insight, runId, reference);
		if (resolved.backing() != Backing.TASK) {
			throw new IllegalArgumentException("Automation data reference is not task-backed.");
		}
		if (offset < 0 || limit < 1 || limit > AutomationConstants.INTERNAL_DATA_PAGE_LIMIT) {
			throw new IllegalArgumentException("Automation task data page has invalid bounds.");
		}
		ITask stored = insight.getTaskStore().getTask(reference.referenceId());
		if (!(stored instanceof BasicIteratorTask task)) {
			throw new IllegalStateException("Automation task data is no longer available in this run workspace.");
		}

		synchronized (task) {
			long total = Math.min(Math.max(task.getNumRows(), 0), task.getNumCollect());
			int maximumRows = (int) total;
			if (offset > maximumRows) {
				throw new IllegalArgumentException("Automation task data offset exceeds its configured row limit.");
			}
			try {
				long currentOffset = ((AbstractTask) task).getInternalOffset();
				if (currentOffset > offset) {
					task.reset();
					currentOffset = 0;
				}
				while (currentOffset < offset && currentOffset < maximumRows && task.hasNext()) {
					task.next();
					currentOffset++;
				}

				int pageLimit = Math.min(limit, maximumRows - offset);
				List<String> headers = new ArrayList<>();
				List<List<Object>> rows = new ArrayList<>(pageLimit);
				while (rows.size() < pageLimit && task.hasNext()) {
					IHeadersDataRow row = task.next();
					if (headers.isEmpty()) {
						headers.addAll(Arrays.asList(row.getHeaders()));
					}
					rows.add(new ArrayList<>(Arrays.asList(row.getValues())));
				}
				Map<String, Object> page = basePage("table", offset, limit, rows.size(), total,
						offset + rows.size() < maximumRows && task.hasNext());
				page.put("headers", headers);
				page.put("rows", rows);
				return page;
			} catch (Exception e) {
				throw new IllegalStateException("Unable to read Automation task data.", e);
			}
		}
	}

	static Map<String, Object> basePage(String kind, int offset, int limit, int count, long total, boolean hasMore) {
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("available", true);
		page.put("kind", kind);
		page.put("offset", offset);
		page.put("limit", limit);
		page.put("count", count);
		page.put("total", total);
		page.put("hasMore", hasMore);
		return page;
	}

	private static RegistryEntry resolve(Insight insight, String runId, AutomationDataReference reference) {
		RunOwner owner = validateExecutionInsight(insight, runId);
		RegistryEntry registryEntry = entry(insight, reference);
		if (registryEntry == null) {
			throw new IllegalStateException("Automation data is no longer available in this run workspace.");
		}
		validateOwner(owner, registryEntry);
		return registryEntry;
	}

	private static RegistryEntry entry(Insight insight, AutomationDataReference reference) {
		NounMetadata noun = insight.getVarStore().get(key(reference));
		return noun != null && noun.getValue() instanceof RegistryEntry registryEntry ? registryEntry : null;
	}

	private static void put(Insight insight, RunOwner owner, AutomationDataReference reference, Backing backing) {
		RegistryEntry registryEntry = new RegistryEntry(owner.projectId(), owner.runId(), owner.createdBy(), backing);
		insight.getVarStore().put(key(reference),
				new NounMetadata(registryEntry, PixelDataType.CUSTOM_DATA_STRUCTURE));
	}

	private static String key(AutomationDataReference reference) {
		return REGISTRY_PREFIX + reference.referenceId();
	}

	private static RunOwner validateExecutionInsight(Insight insight, String runId) {
		if (!AutomationRunExecutionService.isExecutionInsight(insight, runId)) {
			throw new IllegalArgumentException("Automation data is available only inside its execution run.");
		}
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !String.valueOf(run.get(AutomationConstants.PROJECT_ID)).equals(insight.getProjectId())) {
			throw new IllegalArgumentException("Automation run was not found for this execution workspace.");
		}
		return new RunOwner(insight.getProjectId(), runId,
				String.valueOf(run.get(AutomationConstants.CREATED_BY)));
	}

	private static void validateOwner(RunOwner owner, RegistryEntry registryEntry) {
		if (!owner.projectId().equals(registryEntry.projectId()) || !owner.runId().equals(registryEntry.runId())
				|| !owner.createdBy().equals(registryEntry.createdBy())) {
			throw new IllegalArgumentException("Automation data reference does not belong to this execution run.");
		}
	}

	private static void collectReferences(Object value, List<AutomationDataReference> references) {
		AutomationDataReference reference = AutomationDataReference.fromValue(value);
		if (reference != null) {
			references.add(reference);
			return;
		}
		if (value instanceof Map<?, ?> map) {
			for (Object item : map.values()) {
				collectReferences(item, references);
			}
		} else if (value instanceof List<?> list) {
			for (Object item : list) {
				collectReferences(item, references);
			}
		}
	}

	private static void closeQuietly(ITask task) {
		try {
			task.close();
		} catch (IOException e) {
			classLogger.warn("Unable to close rejected Automation task '{}'.", task.getId(), e);
		}
	}

	private enum Backing {
		TASK,
		PYTHON
	}

	private record RunOwner(String projectId, String runId, String createdBy) {
	}

	private record RegistryEntry(String projectId, String runId, String createdBy, Backing backing) {
	}
}
