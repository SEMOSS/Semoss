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

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns one bounded page to a Python node running in the owning run Insight. */
public class GetAutomationTaskDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String REFERENCE_ID_KEY = "referenceId";

	public GetAutomationTaskDataReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY, REFERENCE_ID_KEY, ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int offset = integerValue(ReactorKeysEnum.OFFSET.getKey(), 0);
		int limit = integerValue(ReactorKeysEnum.LIMIT.getKey(), 1);
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, this.keyValue.get(REFERENCE_ID_KEY),
				AutomationValueType.DATASET);
		String runId = this.keyValue.get(RUN_ID_KEY);
		AutomationDataOwner owner = AutomationRunExecutionService.dataOwner(this.insight.getProjectId(), runId);
		Map<String, Object> page = AutomationTaskDataService
				.readPage(this.insight, owner, reference, offset, limit).toMap();
		return new NounMetadata(page, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private int integerValue(String key, int minimum) {
		try {
			int value = Integer.parseInt(this.keyValue.get(key));
			if (value < minimum) {
				throw new IllegalArgumentException(key + " must be at least " + minimum + ".");
			}
			return value;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be an integer.", e);
		}
	}
}
