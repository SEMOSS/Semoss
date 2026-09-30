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

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates an opaque, task-backed Automation dataset from a guarded SQL read.
 */
public class CreateAutomationQueryDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";

	public CreateAutomationQueryDataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.QUERY_KEY.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), RUN_ID_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int limit;
		try {
			limit = Integer.parseInt(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Automation database query limit must be an integer.", e);
		}
		AutomationDataReference reference = AutomationTaskData.createQuery(this.insight,
				this.keyValue.get(RUN_ID_KEY), this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()),
				this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey()), limit);
		return new NounMetadata(reference.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Runs a guarded SQL read and keeps its lazy result in the current Automation execution workspace.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Automation run identifier that owns the current execution workspace.";
		}
		return super.getDescriptionForKey(key);
	}
}
