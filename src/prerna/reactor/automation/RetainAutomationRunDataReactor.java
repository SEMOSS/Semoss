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

import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

/**
 * Retains an upstream task in the current Automation execution Insight.
 *
 * <p>
 * This reactor is intentionally policy-neutral. The upstream reactor remains
 * responsible for authorization, guardrails, query construction, and limits.
 */
public class RetainAutomationRunDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";

	public RetainAutomationRunDataReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String runId = this.keyValue.get(RUN_ID_KEY);
		AutomationDataReference reference = AutomationRunData.retainTask(this.insight, runId, task());
		return new NounMetadata(reference.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private ITask task() {
		ITask task = taskFromStore(PixelDataType.FORMATTED_DATA_SET);
		if (task == null) {
			task = taskFromStore(PixelDataType.TASK);
		}
		if (task == null) {
			task = taskFromRow(PixelDataType.FORMATTED_DATA_SET);
		}
		if (task == null) {
			task = taskFromRow(PixelDataType.TASK);
		}
		if (task == null) {
			throw new IllegalArgumentException("RetainAutomationRunData requires an upstream task.");
		}
		return task;
	}

	private ITask taskFromStore(PixelDataType type) {
		GenRowStruct values = this.store.getGenRowStruct(type.getKey());
		if (values == null || values.isEmpty()) {
			return null;
		}
		Object value = values.get(0);
		return value instanceof ITask task ? task : null;
	}

	private ITask taskFromRow(PixelDataType type) {
		List<Object> values = this.curRow.getValuesOfType(type);
		if (values == null || values.isEmpty()) {
			return null;
		}
		Object value = values.get(0);
		return value instanceof ITask task ? task : null;
	}

	@Override
	public String getReactorDescription() {
		return "Keeps an upstream task in the current Automation execution workspace and returns an opaque reference.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Automation run identifier that owns the current execution workspace.";
		}
		return super.getDescriptionForKey(key);
	}
}
