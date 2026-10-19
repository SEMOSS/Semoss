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
package prerna.reactor.automation.definition;

/**
 * BI frame operations exposed through Automation's Clean and Transform nodes.
 */
public enum AutomationFrameOperation {
	PURGE("purge", false),
	TRIM("trim", false),
	REPLACE("replace", false),
	REMOVE_DUPLICATES("removeDuplicates", false),
	FILTER_EMPTY("filterEmpty", false),
	IMPUTE_NULLS("imputeNulls", false),
	CHANGE_TYPE("changeType", true),
	DROP_COLUMN("dropColumn", true),
	RENAME_COLUMN("renameColumn", true),
	ADD_COLUMN("addColumn", true),
	DUPLICATE_COLUMN("duplicateColumn", true),
	CONCATENATE("concatenate", true),
	PIVOT("pivot", true),
	LOWER_CASE("lowerCase", true),
	PROPER_CASE("properCase", true),
	SPLIT("split", true),
	SPLIT_UNPIVOT("splitUnpivot", true),
	TIMESTAMP_DATA("timestampData", true),
	TO_PERCENT("toPercent", true),
	TRANSPOSE("transpose", true),
	UNPIVOT("unpivot", true),
	UPPER_CASE("upperCase", true),
	JOIN("join", true);

	private final String value;
	private final boolean transform;

	AutomationFrameOperation(String value, boolean transform) {
		this.value = value;
		this.transform = transform;
	}

	/**
	 * @return stable operation value persisted in the node configuration
	 */
	public String getValue() {
		return value;
	}

	public boolean isTransform() {
		return transform;
	}

	public static AutomationFrameOperation fromValue(String value) {
		for (AutomationFrameOperation operation : values()) {
			if (operation.value.equals(value)) {
				return operation;
			}
		}
		throw new IllegalArgumentException("Unsupported Automation frame operation: " + value);
	}
}
