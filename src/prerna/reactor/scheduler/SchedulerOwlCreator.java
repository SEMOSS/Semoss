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

import static prerna.reactor.scheduler.SchedulerConstants.BIGINT;
import static prerna.reactor.scheduler.SchedulerConstants.BLOB;
import static prerna.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.BOOLEAN;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.CLOB;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_EXPRESSION;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_TIMEZONE;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.DESCRIPTION;
import static prerna.reactor.scheduler.SchedulerConstants.END_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.ENTRY_ID;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_DELTA;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_END;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_START;
import static prerna.reactor.scheduler.SchedulerConstants.EXEC_ID;
import static prerna.reactor.scheduler.SchedulerConstants.FIRED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.IMAGE;
import static prerna.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.INTEGER;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.IS_DURABLE;
import static prerna.reactor.scheduler.SchedulerConstants.IS_LATEST;
import static prerna.reactor.scheduler.SchedulerConstants.IS_NONCONCURRENT;
import static prerna.reactor.scheduler.SchedulerConstants.IS_UPDATE_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CATEGORY;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CLASS_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_ID;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_TAG;
import static prerna.reactor.scheduler.SchedulerConstants.LAST_CHECKIN_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.LOCK_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.MISFIRE_INSTR;
import static prerna.reactor.scheduler.SchedulerConstants.NEXT_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.NUMERIC_13_4;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE_PARAMETERS;
import static prerna.reactor.scheduler.SchedulerConstants.PREV_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.PRIORITY;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_BLOB_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CALENDARS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CRON_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_FIRED_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_JOB_DETAILS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_LOCKS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_PAUSED_TRIGGER_GRPS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SCHEDULER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPLE_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPROP_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_COUNT;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.REQUESTS_RECOVERY;
import static prerna.reactor.scheduler.SchedulerConstants.SCHEDULER_OUTPUT;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.SMALLINT;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_AUDIT_TRAIL;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_EXECUTION;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_RECIPES;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_TAGS;
import static prerna.reactor.scheduler.SchedulerConstants.START_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.STATE;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_3;
import static prerna.reactor.scheduler.SchedulerConstants.SUCCESS;
import static prerna.reactor.scheduler.SchedulerConstants.TIMESTAMP;
import static prerna.reactor.scheduler.SchedulerConstants.TIMES_TRIGGERED;
import static prerna.reactor.scheduler.SchedulerConstants.TIME_ZONE_ID;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_ON_LOAD;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_TYPE;
import static prerna.reactor.scheduler.SchedulerConstants.UI_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.USER_ID;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_120;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_16;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_200;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_250;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_255;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_40;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_512;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_1000;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_2000;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.WriteOWLEngine;

public class SchedulerOwlCreator extends AbstractOwlCreator {

	public SchedulerOwlCreator() {
		createColumnsAndTypes();
	}

	public void createColumnsAndTypes() {
		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable(QRTZ_CALENDARS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(CALENDAR_NAME, VARCHAR_200),
				Pair.with(CALENDAR, IMAGE)));

		addTable(QRTZ_CRON_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(CRON_EXPRESSION, VARCHAR_120),
				Pair.with(TIME_ZONE_ID, VARCHAR_80)));

		addTable(QRTZ_FIRED_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(ENTRY_ID, VARCHAR_95),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(INSTANCE_NAME, VARCHAR_200),
				Pair.with(FIRED_TIME, BIGINT),
				Pair.with(SCHED_TIME, BIGINT),
				Pair.with(PRIORITY, INTEGER),
				Pair.with(STATE, VARCHAR_16),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(IS_NONCONCURRENT, BOOLEAN),
				Pair.with(REQUESTS_RECOVERY, BOOLEAN)));

		addTable(QRTZ_PAUSED_TRIGGER_GRPS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_GROUP, VARCHAR_200)));

		addTable(QRTZ_SCHEDULER_STATE, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(INSTANCE_NAME, VARCHAR_200),
				Pair.with(LAST_CHECKIN_TIME, BIGINT),
				Pair.with(CHECKIN_INTERVAL, BIGINT)));

		addTable(QRTZ_LOCKS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(LOCK_NAME, VARCHAR_40)));

		addTable(QRTZ_JOB_DETAILS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(DESCRIPTION, VARCHAR_250),
				Pair.with(JOB_CLASS_NAME, VARCHAR_250),
				Pair.with(IS_DURABLE, BOOLEAN),
				Pair.with(IS_NONCONCURRENT, BOOLEAN),
				Pair.with(IS_UPDATE_DATA, BOOLEAN),
				Pair.with(REQUESTS_RECOVERY, BOOLEAN),
				Pair.with(JOB_DATA, IMAGE)));

		addTable(QRTZ_SIMPLE_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(REPEAT_COUNT, BIGINT),
				Pair.with(REPEAT_INTERVAL, BIGINT),
				Pair.with(TIMES_TRIGGERED, BIGINT)));

		addTable(QRTZ_SIMPROP_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(STR_PROP_1, VARCHAR_512),
				Pair.with(STR_PROP_2, VARCHAR_512),
				Pair.with(STR_PROP_3, VARCHAR_512),
				Pair.with(INT_PROP_1, INTEGER),
				Pair.with(INT_PROP_2, INTEGER),
				Pair.with(LONG_PROP_1, BIGINT),
				Pair.with(LONG_PROP_2, BIGINT),
				Pair.with(DEC_PROP_1, NUMERIC_13_4),
				Pair.with(DEC_PROP_2, NUMERIC_13_4),
				Pair.with(BOOL_PROP_1, BOOLEAN),
				Pair.with(BOOL_PROP_2, BOOLEAN)));

		addTable(QRTZ_BLOB_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(BLOB_DATA, IMAGE)));

		addTable(QRTZ_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(DESCRIPTION, VARCHAR_250),
				Pair.with(NEXT_FIRE_TIME, BIGINT),
				Pair.with(PREV_FIRE_TIME, BIGINT),
				Pair.with(PRIORITY, INTEGER),
				Pair.with(TRIGGER_STATE, VARCHAR_16),
				Pair.with(TRIGGER_TYPE, VARCHAR_8),
				Pair.with(START_TIME, BIGINT),
				Pair.with(END_TIME, BIGINT),
				Pair.with(CALENDAR_NAME, VARCHAR_200),
				Pair.with(MISFIRE_INSTR, SMALLINT),
				Pair.with(JOB_DATA, IMAGE)));

		addTable(SMSS_JOB_RECIPES, Arrays.asList(
				Pair.with(USER_ID, VARCHAR_120),
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(CRON_EXPRESSION, VARCHAR_250),
				Pair.with(CRON_TIMEZONE, VARCHAR_120),
				Pair.with(PIXEL_RECIPE, BLOB),
				Pair.with(PIXEL_RECIPE_PARAMETERS, BLOB),
				Pair.with(JOB_CATEGORY, VARCHAR_200),
				Pair.with(TRIGGER_ON_LOAD, BOOLEAN),
				Pair.with(UI_STATE, BLOB)));

		addTable(SMSS_AUDIT_TRAIL, Arrays.asList(
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(EXECUTION_START, TIMESTAMP),
				Pair.with(EXECUTION_END, TIMESTAMP),
				Pair.with(EXECUTION_DELTA, VARCHAR_255),
				Pair.with(SUCCESS, BOOLEAN),
				Pair.with(IS_LATEST, BOOLEAN),
				Pair.with(SCHEDULER_OUTPUT, CLOB)));

		addTable(SMSS_JOB_TAGS, Arrays.asList(
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_TAG, VARCHAR_200)));

		addTable(SMSS_EXECUTION, Arrays.asList(
				Pair.with(EXEC_ID, VARCHAR_200),
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200)));

		// WORKFLOW_RUNS, WORKFLOW_NODE_OUTPUTS, WORKFLOW_FOREACH_ROWS are intentionally
		// absent here. WorkflowDatabaseUtility.initialize() (called by SMSSWebWatcher) is
		// the sole authority for those tables — it handles primary keys, indexes, NOT NULL
		// constraints, and addColumnIfNotExists migrations that this declarative mechanism
		// does not support.
		// @formatter:on
	}

	@Override
	protected void writeRelations(WriteOWLEngine owler) throws Exception {
		// add Foreign Keys/Relations
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_CRON_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_CRON_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_CRON_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);

		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPLE_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPLE_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPLE_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);

		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPROP_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPROP_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS,
				QRTZ_SIMPROP_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);

		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS,
				QRTZ_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_JOB_DETAILS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS,
				QRTZ_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_JOB_DETAILS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS,
				QRTZ_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_JOB_DETAILS + "." + TRIGGER_GROUP);
	}

}
