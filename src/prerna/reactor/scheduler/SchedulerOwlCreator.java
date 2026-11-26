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
import static prerna.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
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
import static prerna.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
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
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Defines scheduler-owned database tables, column types, and relationships in
 * the scheduler OWL.
 *
 * <p>
 * This includes Automation run-history tables because their lifecycle is tied
 * to scheduled and manual execution rather than project business data.
 */
public class SchedulerOwlCreator extends AbstractOwlCreator {

	/** A trigger names the job it fires by its JOB_NAME and JOB_GROUP */
	private static final String TRIGGER_JOB_NAME_RELATION = QRTZ_TRIGGERS + "." + JOB_NAME + "." + QRTZ_JOB_DETAILS
			+ "." + JOB_NAME;
	private static final String TRIGGER_JOB_GROUP_RELATION = QRTZ_TRIGGERS + "." + JOB_GROUP + "." + QRTZ_JOB_DETAILS
			+ "." + JOB_GROUP;

	public SchedulerOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		this.allSchemas = new ArrayList<>();

		final String INTEGER_DATATYPE = queryUtil.getIntegerDataTypeName();
		final String TIMESTAMP_DATATYPE = queryUtil.getDateWithTimeDataType();
		final String BLOB_DATATYPE = queryUtil.getBlobDataTypeName();
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String CLOB_DATATYPE = queryUtil.getClobDataTypeName();
		final String IMAGE_DATATYPE = queryUtil.getImageDataTypeName();

		// @formatter:off
		// the Quartz tables: the job store the Quartz scheduler keeps itself (JobStoreTX with the
		// QRTZ_ prefix, in quartz.properties). SchedulerDatabaseUtility creates them and their keys
		// at startup, and SCHED_NAME, the scheduler's name, starts every key. A job
		// (QRTZ_JOB_DETAILS) is fired by its triggers (QRTZ_TRIGGERS), and each trigger keeps its
		// schedule in the table for its kind: QRTZ_CRON_TRIGGERS, QRTZ_SIMPLE_TRIGGERS,
		// QRTZ_SIMPROP_TRIGGERS or QRTZ_BLOB_TRIGGERS

		// a named Quartz calendar: CALENDAR is a serialized Calendar of the times a trigger skips.
		// A trigger names its calendar in QRTZ_TRIGGERS.CALENDAR_NAME; the platform's triggers name
		// none
		addTable(QRTZ_CALENDARS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(CALENDAR_NAME, VARCHAR_200),
				Pair.with(CALENDAR, IMAGE_DATATYPE)));

		// the schedule of a cron trigger: its CRON_EXPRESSION and the TIME_ZONE_ID it runs in,
		// keyed by the trigger's SCHED_NAME, TRIGGER_NAME and TRIGGER_GROUP in QRTZ_TRIGGERS. Every
		// recurring schedule the platform sets is a cron trigger: a scheduled job
		// (ScheduleJobReactor, EditScheduledJobReactor) and a Collaboration routine
		addTable(QRTZ_CRON_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(CRON_EXPRESSION, VARCHAR_120),
				Pair.with(TIME_ZONE_ID, VARCHAR_80)));

		// one row per trigger firing in progress: the trigger and the job it fired, the scheduler
		// instance that fired it (INSTANCE_NAME), when it fired and when it was due, and its STATE,
		// such as ACQUIRED or EXECUTING. Quartz deletes the row when the job finishes, and when the
		// scheduler is clustered it reads the rows of a failed instance to recover that instance's
		// jobs
		addTable(QRTZ_FIRED_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(ENTRY_ID, VARCHAR_95),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(INSTANCE_NAME, VARCHAR_200),
				Pair.with(FIRED_TIME, BIGINT),
				Pair.with(SCHED_TIME, BIGINT),
				Pair.with(PRIORITY, INTEGER_DATATYPE),
				Pair.with(STATE, VARCHAR_16),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(IS_NONCONCURRENT, BOOLEAN_DATATYPE),
				Pair.with(REQUESTS_RECOVERY, BOOLEAN_DATATYPE)));

		// the trigger groups paused as a whole; a trigger added to a paused group starts paused.
		// Pausing one trigger, as PauseJobTriggerReactor does, sets that trigger's TRIGGER_STATE in
		// QRTZ_TRIGGERS instead
		addTable(QRTZ_PAUSED_TRIGGER_GRPS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_GROUP, VARCHAR_200)));

		// one row per scheduler instance when the scheduler is clustered (IS_CLUSTERED_SCHEDULER):
		// when the instance last checked in (LAST_CHECKIN_TIME) and how often it checks in
		// (CHECKIN_INTERVAL, in milliseconds). An instance that stops checking in is taken as
		// failed, and another recovers its fired triggers
		addTable(QRTZ_SCHEDULER_STATE, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(INSTANCE_NAME, VARCHAR_200),
				Pair.with(LAST_CHECKIN_TIME, BIGINT),
				Pair.with(CHECKIN_INTERVAL, BIGINT)));

		// the database row locks a clustered scheduler takes so that one instance at a time changes
		// triggers or scheduler state: one row per LOCK_NAME, TRIGGER_ACCESS and STATE_ACCESS
		addTable(QRTZ_LOCKS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(LOCK_NAME, VARCHAR_40)));

		// one row per Quartz job: its JOB_NAME and JOB_GROUP, the JOB_CLASS_NAME that runs it
		// (RunPixelJobFromDB for the platform's jobs), whether it stays stored with no trigger
		// (IS_DURABLE), and JOB_DATA, its serialized JobDataMap holding the pixel to run, the job's
		// schedule settings and USER_ACCESS, the login the job runs as. The platform names a job by
		// its job id, so JOB_NAME and JOB_GROUP match SMSS_JOB_RECIPES.JOB_ID and JOB_GROUP
		addTable(QRTZ_JOB_DETAILS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(DESCRIPTION, VARCHAR_250),
				Pair.with(JOB_CLASS_NAME, VARCHAR_250),
				Pair.with(IS_DURABLE, BOOLEAN_DATATYPE),
				Pair.with(IS_NONCONCURRENT, BOOLEAN_DATATYPE),
				Pair.with(IS_UPDATE_DATA, BOOLEAN_DATATYPE),
				Pair.with(REQUESTS_RECOVERY, BOOLEAN_DATATYPE),
				Pair.with(JOB_DATA, IMAGE_DATATYPE)));

		// the schedule of a simple trigger: how many times it repeats (REPEAT_COUNT), how far apart
		// (REPEAT_INTERVAL, in milliseconds) and how many times it has fired (TIMES_TRIGGERED). The
		// platform's one-time runs are simple triggers: a job run now through Quartz triggerJob
		// (ExecuteScheduledJobReactor, a job that triggers on load, a Collaboration routine run)
		// and a delayed Collaboration routine run
		addTable(QRTZ_SIMPLE_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(REPEAT_COUNT, BIGINT),
				Pair.with(REPEAT_INTERVAL, BIGINT),
				Pair.with(TIMES_TRIGGERED, BIGINT)));

		// the schedule of a calendar interval or daily time interval trigger, held in generic
		// string, integer, long, decimal and boolean columns that each of those trigger kinds maps
		// its settings onto. The platform's jobs use cron and simple triggers
		addTable(QRTZ_SIMPROP_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(STR_PROP_1, VARCHAR_512),
				Pair.with(STR_PROP_2, VARCHAR_512),
				Pair.with(STR_PROP_3, VARCHAR_512),
				Pair.with(INT_PROP_1, INTEGER_DATATYPE),
				Pair.with(INT_PROP_2, INTEGER_DATATYPE),
				Pair.with(LONG_PROP_1, BIGINT),
				Pair.with(LONG_PROP_2, BIGINT),
				Pair.with(DEC_PROP_1, NUMERIC_13_4),
				Pair.with(DEC_PROP_2, NUMERIC_13_4),
				Pair.with(BOOL_PROP_1, BOOLEAN_DATATYPE),
				Pair.with(BOOL_PROP_2, BOOLEAN_DATATYPE)));

		// a trigger of a kind that has no table of its own, stored whole as a serialized object in
		// BLOB_DATA. The platform's triggers each have their own table
		addTable(QRTZ_BLOB_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(BLOB_DATA, IMAGE_DATATYPE)));

		// one row per Quartz trigger: the job it fires (JOB_NAME and JOB_GROUP in
		// QRTZ_JOB_DETAILS), when it fires next and fired last (NEXT_FIRE_TIME, PREV_FIRE_TIME, in
		// epoch milliseconds), its TRIGGER_STATE (such as WAITING, PAUSED or COMPLETE), its
		// TRIGGER_TYPE (CRON, SIMPLE, CAL_INT, DAILY_I or BLOB), which names the table holding its
		// schedule, its start and end, MISFIRE_INSTR for a missed firing, and JOB_DATA, data the
		// trigger passes to its job, such as the pixel of a one-time run. ScheduleJobReactor names
		// a job's trigger <jobId>Trigger in the group <jobGroup>TriggerGroup, and the job listings
		// and AdminSchedulerStatsReactor read NEXT_FIRE_TIME and TRIGGER_STATE from here
		addTable(QRTZ_TRIGGERS, Arrays.asList(
				Pair.with(SCHED_NAME, VARCHAR_120),
				Pair.with(TRIGGER_NAME, VARCHAR_200),
				Pair.with(TRIGGER_GROUP, VARCHAR_200),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(DESCRIPTION, VARCHAR_250),
				Pair.with(NEXT_FIRE_TIME, BIGINT),
				Pair.with(PREV_FIRE_TIME, BIGINT),
				Pair.with(PRIORITY, INTEGER_DATATYPE),
				Pair.with(TRIGGER_STATE, VARCHAR_16),
				Pair.with(TRIGGER_TYPE, VARCHAR_8),
				Pair.with(START_TIME, BIGINT),
				Pair.with(END_TIME, BIGINT),
				Pair.with(CALENDAR_NAME, VARCHAR_200),
				Pair.with(MISFIRE_INSTR, SMALLINT),
				Pair.with(JOB_DATA, IMAGE_DATATYPE)));

		// the platform's own scheduler tables beside the Quartz ones: what each scheduled job runs
		// and who owns it, its tags, its run history, and the runs in flight. A job's rows match
		// its Quartz job by JOB_ID and JOB_GROUP

		// one row per scheduled job: the user who owns it (USER_ID), its JOB_ID (the Quartz job's
		// JOB_NAME), its display JOB_NAME, its JOB_GROUP (the project it runs for, or the
		// Collaboration routines' group), its cron expression and time zone, the PIXEL_RECIPE and
		// PIXEL_RECIPE_PARAMETERS it runs, JOB_CATEGORY, whether it also runs once at every server
		// start (TRIGGER_ON_LOAD) and the UI_STATE saved with it. ScheduleJobReactor writes it with
		// the Quartz job, EditScheduledJobReactor updates it, and the job listings join it to
		// QRTZ_TRIGGERS and SMSS_AUDIT_TRAIL
		addTable(SMSS_JOB_RECIPES, Arrays.asList(
				Pair.with(USER_ID, VARCHAR_120),
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_NAME, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(CRON_EXPRESSION, VARCHAR_250),
				Pair.with(CRON_TIMEZONE, VARCHAR_120),
				Pair.with(PIXEL_RECIPE, BLOB_DATATYPE),
				Pair.with(PIXEL_RECIPE_PARAMETERS, BLOB_DATATYPE),
				Pair.with(JOB_CATEGORY, VARCHAR_200),
				Pair.with(TRIGGER_ON_LOAD, BOOLEAN_DATATYPE),
				Pair.with(UI_STATE, BLOB_DATATYPE)));

		// one row per finished run of a scheduled job, written by RunPixelJobFromDB: when it
		// started and ended, how long it took (EXECUTION_DELTA, in milliseconds), whether the
		// scheduler endpoint answered with success, and SCHEDULER_OUTPUT, the endpoint's response.
		// IS_LATEST marks the job's newest run, which the job listings show as its last run.
		// SchedulerHistoryReactor and AdminSchedulerStatsReactor read the history, which stays when
		// a project's jobs are removed
		addTable(SMSS_AUDIT_TRAIL, Arrays.asList(
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200),
				Pair.with(EXECUTION_START, TIMESTAMP_DATATYPE),
				Pair.with(EXECUTION_END, TIMESTAMP_DATATYPE),
				Pair.with(EXECUTION_DELTA, VARCHAR_255),
				Pair.with(SUCCESS, BOOLEAN_DATATYPE),
				Pair.with(IS_LATEST, BOOLEAN_DATATYPE),
				Pair.with(SCHEDULER_OUTPUT, CLOB_DATATYPE)));

		// the tags of a scheduled job, one row per JOB_ID and JOB_TAG. Saving a job replaces its
		// tags, and the job listings and SchedulerHistoryReactor filter jobs by them
		addTable(SMSS_JOB_TAGS, Arrays.asList(
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_TAG, VARCHAR_200)));

		// one row per scheduled run in flight: RunPixelJobFromDB adds a random EXEC_ID with the
		// job's JOB_ID and JOB_GROUP before it calls the scheduler endpoint, and deletes it when
		// the run ends. The endpoint's SchedulerFilter runs a request only when its execId is here
		// with the same job id and group
		addTable(SMSS_EXECUTION, Arrays.asList(
				Pair.with(EXEC_ID, VARCHAR_200),
				Pair.with(JOB_ID, VARCHAR_200),
				Pair.with(JOB_GROUP, VARCHAR_200)));

		// Automation run history, written and read by AutomationRunStore.
		// AUTOMATION_RUN_NODE_SOURCES, AUTOMATION_NODE_OUTPUTS, AUTOMATION_RUN_WAITS and
		// AUTOMATION_SAVED_RESULTS join AUTOMATION_RUNS by RUN_ID, and AUTOMATION_RESULT_CHUNKS
		// joins AUTOMATION_SAVED_RESULTS by RESULT_ID

		// one row per Automation run. PROJECT_ID is the run scope, the Automation project or the
		// caller's inline scope (SYSTEM__AUTOMATION_INLINE__<hash>) for RunAutomationDefinition,
		// and AUTOMATION_ID the automation in it. The run keeps an immutable snapshot of the graph
		// it runs (DEFINITION_SNAPSHOT, with its format version and hash) and of its inputs, its
		// STATUS (SUBMITTED, RUNNING, WAITING_FOR_INPUT, then SUCCESS, FAILED, CANCELLED or
		// INTERRUPTED), its TRIGGER_TYPE (MANUAL, PLAYGROUND or SCHEDULED), the node that failed
		// and why, its node counts, who started it, CANCEL_REQUESTED, which cancels it from any
		// server, and RESULT_SUMMARY. LAST_HEARTBEAT moves while it runs, and a run whose heartbeat
		// goes stale is marked INTERRUPTED
		addTable(AutomationConstants.TABLE_AUTOMATION_RUNS, Arrays.asList(
				Pair.with(AutomationConstants.RUN_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.PROJECT_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.AUTOMATION_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.DEFINITION_VERSION, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.DEFINITION_HASH, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.DEFINITION_SNAPSHOT, CLOB_DATATYPE),
				Pair.with(AutomationConstants.INPUT_SNAPSHOT, CLOB_DATATYPE),
				Pair.with(AutomationConstants.STATUS, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.TRIGGER_TYPE, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.STARTED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.COMPLETED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.FAILED_NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.ERROR_MESSAGE, CLOB_DATATYPE),
				Pair.with(AutomationConstants.LAST_HEARTBEAT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.TOTAL_NODES, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.COMPLETED_NODES, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.CREATED_BY, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.CANCEL_REQUESTED, BOOLEAN_DATATYPE),
				Pair.with(AutomationConstants.RESULT_SUMMARY_COL, AutomationConstants.VARCHAR_2000)));

		// the Python source of each developer.python node as the run captured it, with its
		// SOURCE_HASH. The run executes this snapshot, and the hash is checked before the source is
		// returned
		addTable(AutomationConstants.TABLE_AUTOMATION_RUN_NODE_SOURCES, Arrays.asList(
				Pair.with(AutomationConstants.RUN_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.SOURCE_HASH, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.SOURCE_CODE, CLOB_DATATYPE)));

		// the history of each node in a run: one row per graph node, made PENDING in
		// EXECUTION_ORDER when the run starts, and one row per body node of each loop pass, which
		// takes its own NODE_ID and names its loop (PARENT_NODE_ID), its pass (ITERATION_INDEX) and
		// its graph node (SOURCE_NODE_ID). Each row keeps the node's STATUS and timing, the
		// OUTPUT_VAR_NAME its output goes under, OUTPUT_KIND (TABLE for a captured table), the
		// bounded OUTPUT_VALUE and OUTPUT_PREVIEW, any error, and the room, workspace, model
		// message and agent run a model or agent node used
		addTable(AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS, Arrays.asList(
				Pair.with(AutomationConstants.RUN_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.NODE_LABEL, AutomationConstants.VARCHAR_500),
				Pair.with(AutomationConstants.EXECUTION_ORDER, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.STATUS, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.STARTED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.COMPLETED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.DURATION_MS, AutomationConstants.BIGINT),
				Pair.with(AutomationConstants.OUTPUT_VAR_NAME, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.OUTPUT_KIND, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.OUTPUT_VALUE, CLOB_DATATYPE),
				Pair.with(AutomationConstants.OUTPUT_PREVIEW, AutomationConstants.VARCHAR_2000),
				Pair.with(AutomationConstants.ROOM_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.WORKSPACE_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.MODEL_MESSAGE_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.AGENT_RUN_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.PARENT_NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.ITERATION_INDEX, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.SOURCE_NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.ERROR_MESSAGE, CLOB_DATATYPE)));

		// a point where a run waits for input: an agent node (WAIT_TYPE AGENT_ACTION) paused on a
		// pending action of its agent run (AGENT_RUN_ID and ROOM_ID in the model inference logs).
		// STATUS goes from PENDING to RESUMING when the run is claimed to go on, then RESOLVED;
		// RESUME_NODE_ID is the node the run goes on from, and EXPIRES_AT the approval deadline
		addTable(AutomationConstants.TABLE_AUTOMATION_RUN_WAITS, Arrays.asList(
				Pair.with(AutomationConstants.WAIT_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.RUN_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.WAIT_TYPE, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.AGENT_RUN_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.ROOM_ID, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.RESUME_NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.STATUS, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.CREATED_BY, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.STARTED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.EXPIRES_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.RESOLVED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.RESOLVED_BY, AutomationConstants.VARCHAR_255)));

		// a retained node result: a table output (RESULT_KIND TABLE) of a run's node, with its row
		// and byte counts, its CHECKSUM, its schema in PAYLOAD_VALUE and its output variable in
		// LOGICAL_NAME. The rows themselves are in AUTOMATION_RESULT_CHUNKS. RETAIN_UNTIL is 30
		// days after CREATED_AT
		addTable(AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS, Arrays.asList(
				Pair.with(AutomationConstants.RESULT_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.PROJECT_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.RUN_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.PARENT_NODE_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.ITERATION_INDEX, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.RESULT_KIND, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.FORMAT_VERSION, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.ROW_COUNT, AutomationConstants.BIGINT),
				Pair.with(AutomationConstants.BYTE_COUNT, AutomationConstants.BIGINT),
				Pair.with(AutomationConstants.CHECKSUM, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.CREATED_AT, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.RETAIN_UNTIL, TIMESTAMP_DATATYPE),
				Pair.with(AutomationConstants.CAPTURE_STATE, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.RESULT_ROLE, AutomationConstants.VARCHAR_50),
				Pair.with(AutomationConstants.LOGICAL_NAME, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.PAYLOAD_VALUE, CLOB_DATATYPE)));

		// the rows of a saved table result in ordered chunks: CHUNK_INDEX, the index of the chunk's
		// first row, its row and byte counts, its CHECKSUM and CHUNK_VALUE, the rows themselves. A
		// chunk holds at most 1,000 rows or 1 MB, so a large table is read a page at a time
		addTable(AutomationConstants.TABLE_AUTOMATION_RESULT_CHUNKS, Arrays.asList(
				Pair.with(AutomationConstants.RESULT_ID, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.CHUNK_INDEX, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.FIRST_ROW_INDEX, AutomationConstants.BIGINT),
				Pair.with(AutomationConstants.CHUNK_ROW_COUNT, AutomationConstants.INTEGER),
				Pair.with(AutomationConstants.BYTE_COUNT, AutomationConstants.BIGINT),
				Pair.with(AutomationConstants.CHECKSUM, AutomationConstants.VARCHAR_255),
				Pair.with(AutomationConstants.CHUNK_VALUE, CLOB_DATATYPE)));

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
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, TRIGGER_JOB_NAME_RELATION);
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, TRIGGER_JOB_GROUP_RELATION);

		owler.addRelation(AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS, AutomationConstants.TABLE_AUTOMATION_RUNS,
				AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS + "." + AutomationConstants.RUN_ID + "."
						+ AutomationConstants.TABLE_AUTOMATION_RUNS + "." + AutomationConstants.RUN_ID);
		owler.addRelation(AutomationConstants.TABLE_AUTOMATION_RUN_NODE_SOURCES,
				AutomationConstants.TABLE_AUTOMATION_RUNS,
				AutomationConstants.TABLE_AUTOMATION_RUN_NODE_SOURCES + "." + AutomationConstants.RUN_ID + "."
						+ AutomationConstants.TABLE_AUTOMATION_RUNS + "." + AutomationConstants.RUN_ID);
		owler.addRelation(AutomationConstants.TABLE_AUTOMATION_RUN_WAITS, AutomationConstants.TABLE_AUTOMATION_RUNS,
				AutomationConstants.TABLE_AUTOMATION_RUN_WAITS + "." + AutomationConstants.RUN_ID + "."
						+ AutomationConstants.TABLE_AUTOMATION_RUNS + "." + AutomationConstants.RUN_ID);
		owler.addRelation(AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS,
				AutomationConstants.TABLE_AUTOMATION_RUNS,
				AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS + "." + AutomationConstants.RUN_ID + "."
						+ AutomationConstants.TABLE_AUTOMATION_RUNS + "." + AutomationConstants.RUN_ID);
		owler.addRelation(AutomationConstants.TABLE_AUTOMATION_RESULT_CHUNKS,
				AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS,
				AutomationConstants.TABLE_AUTOMATION_RESULT_CHUNKS + "." + AutomationConstants.RESULT_ID + "."
						+ AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS + "." + AutomationConstants.RESULT_ID);
	}

	/**
	 * Remakes an OWL that does not join a trigger to its job by JOB_NAME and
	 * JOB_GROUP.
	 *
	 * @param engine the scheduler database whose persisted OWL is being inspected
	 * @return true if either join is missing
	 */
	@Override
	protected boolean additionalRemakeChecks(IDatabaseEngine engine) {
		return relationMissing(engine, QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, TRIGGER_JOB_NAME_RELATION)
				|| relationMissing(engine, QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, TRIGGER_JOB_GROUP_RELATION);
	}

}
