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
package prerna.reactor.automation;

import static prerna.reactor.automation.AutomationConstants.AUTOMATION_ID;
import static prerna.reactor.automation.AutomationConstants.BIGINT;
import static prerna.reactor.automation.AutomationConstants.CANCEL_REQUESTED;
import static prerna.reactor.automation.AutomationConstants.CLAIMED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_NODES;
import static prerna.reactor.automation.AutomationConstants.CREATED_BY;
import static prerna.reactor.automation.AutomationConstants.DURATION_MS;
import static prerna.reactor.automation.AutomationConstants.ERROR_MESSAGE;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_ORDER;
import static prerna.reactor.automation.AutomationConstants.FAILED_NODE_ID;
import static prerna.reactor.automation.AutomationConstants.INTEGER;
import static prerna.reactor.automation.AutomationConstants.LAST_HEARTBEAT;
import static prerna.reactor.automation.AutomationConstants.NODE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_LABEL;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_PREVIEW;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VALUE;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VAR;
import static prerna.reactor.automation.AutomationConstants.PROJECT_ID;
import static prerna.reactor.automation.AutomationConstants.RESULT_SUMMARY_COL;
import static prerna.reactor.automation.AutomationConstants.RUN_ID;
import static prerna.reactor.automation.AutomationConstants.STARTED_AT;
import static prerna.reactor.automation.AutomationConstants.STATUS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_ACTIVE_RUN;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RUNS;
import static prerna.reactor.automation.AutomationConstants.TOTAL_NODES;
import static prerna.reactor.automation.AutomationConstants.TRIGGER_TYPE;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_2000;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_255;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_500;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;

/**
 * OWL schema declaration for the three automation tables stored in the
 * scheduler database. Follows the same pattern as other system-engine OWL
 * creators (e.g. {@link prerna.reactor.scheduler.SchedulerOwlCreator}).
 *
 * <p>Called from {@link AutomationDatabaseUtility#initialize()} so that the
 * automation OWL schema stays entirely within the {@code prerna.reactor.automation}
 * package and does not require the scheduler package to import automation types.
 */
public class AutomationOwlCreator extends AbstractOwlCreator {

	// Reuse the DB-level BOOLEAN/TIMESTAMP/CLOB type names used by the scheduler OWL.
	// SchedulerConstants uses these string literals directly (e.g. "BOOLEAN", "TIMESTAMP", "CLOB").
	private static final String BOOLEAN = "BOOLEAN";
	private static final String TIMESTAMP = "TIMESTAMP";
	private static final String CLOB = "CLOB";

	public AutomationOwlCreator() {
		createColumnsAndTypes();
	}

	public void createColumnsAndTypes() {
		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable(TABLE_AUTOMATION_RUNS, Arrays.asList(
				Pair.with(RUN_ID, VARCHAR_255),
				Pair.with(PROJECT_ID, VARCHAR_255),
				Pair.with(AUTOMATION_ID, VARCHAR_255),
				Pair.with(STATUS, VARCHAR_255),
				Pair.with(TRIGGER_TYPE, VARCHAR_255),
				Pair.with(STARTED_AT, TIMESTAMP),
				Pair.with(COMPLETED_AT, TIMESTAMP),
				Pair.with(FAILED_NODE_ID, VARCHAR_255),
				Pair.with(ERROR_MESSAGE, CLOB),
				Pair.with(LAST_HEARTBEAT, TIMESTAMP),
				Pair.with(TOTAL_NODES, INTEGER),
				Pair.with(COMPLETED_NODES, INTEGER),
				Pair.with(CREATED_BY, VARCHAR_255),
				Pair.with(CANCEL_REQUESTED, BOOLEAN),
				Pair.with(RESULT_SUMMARY_COL, VARCHAR_2000)));

		addTable(TABLE_AUTOMATION_NODE_OUTPUTS, Arrays.asList(
				Pair.with(RUN_ID, VARCHAR_255),
				Pair.with(NODE_ID, VARCHAR_255),
				Pair.with(NODE_LABEL, VARCHAR_500),
				Pair.with(EXECUTION_ORDER, INTEGER),
				Pair.with(STATUS, VARCHAR_255),
				Pair.with(STARTED_AT, TIMESTAMP),
				Pair.with(COMPLETED_AT, TIMESTAMP),
				Pair.with(DURATION_MS, BIGINT),
				Pair.with(OUTPUT_VAR, VARCHAR_255),
				Pair.with(OUTPUT_VALUE, CLOB),
				Pair.with(OUTPUT_PREVIEW, VARCHAR_2000),
				Pair.with(ERROR_MESSAGE, CLOB)));

		addTable(TABLE_AUTOMATION_ACTIVE_RUN, Arrays.asList(
				Pair.with(PROJECT_ID, VARCHAR_255),
				Pair.with(RUN_ID, VARCHAR_255),
				Pair.with(CLAIMED_AT, TIMESTAMP)));
		// @formatter:on
	}

}
