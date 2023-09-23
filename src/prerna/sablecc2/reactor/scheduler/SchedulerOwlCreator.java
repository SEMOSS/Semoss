package prerna.sablecc2.reactor.scheduler;

import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BIGINT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BLOB;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOLEAN;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CLOB;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CRON_EXPRESSION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CRON_TIMEZONE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DEC_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DEC_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DESCRIPTION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.END_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.ENTRY_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_DELTA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_END;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_START;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXEC_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.FIRED_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IMAGE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INTEGER;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INT_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INT_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_DURABLE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_LATEST;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_NONCONCURRENT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_UPDATE_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_CATEGORY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_CLASS_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_GROUP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_TAG;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LAST_CHECKIN_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LOCK_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LONG_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LONG_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.MISFIRE_INSTR;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.NEXT_FIRE_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.NUMERIC_13_4;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE_PARAMETERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PREV_FIRE_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PRIORITY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_BLOB_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_CALENDARS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_CRON_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_FIRED_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_JOB_DETAILS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_LOCKS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_PAUSED_TRIGGER_GRPS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SCHEDULER_STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SIMPLE_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SIMPROP_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REPEAT_COUNT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REPEAT_INTERVAL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REQUESTS_RECOVERY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SCHEDULER_OUTPUT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SCHED_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SCHED_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMALLINT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_AUDIT_TRAIL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_EXECUTION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_JOB_RECIPES;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_JOB_TAGS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.START_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_3;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SUCCESS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIMESTAMP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIMES_TRIGGERED;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIME_ZONE_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_GROUP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_ON_LOAD;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_TYPE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.UI_STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.USER_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_120;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_16;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_200;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_250;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_255;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_40;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_512;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class SchedulerOwlCreator {
	
	private static List<String> conceptsRequired = new Vector<>();
	private IDatabaseEngine schedulerDb;

	static {
		conceptsRequired.add(QRTZ_CALENDARS);
		conceptsRequired.add(QRTZ_CRON_TRIGGERS);
		conceptsRequired.add(QRTZ_FIRED_TRIGGERS);
		conceptsRequired.add(QRTZ_PAUSED_TRIGGER_GRPS);
		conceptsRequired.add(QRTZ_SCHEDULER_STATE);
		conceptsRequired.add(QRTZ_LOCKS);
		conceptsRequired.add(QRTZ_JOB_DETAILS);
		conceptsRequired.add(QRTZ_SIMPLE_TRIGGERS);
		conceptsRequired.add(QRTZ_SIMPROP_TRIGGERS);
		conceptsRequired.add(QRTZ_BLOB_TRIGGERS);
		conceptsRequired.add(QRTZ_TRIGGERS);
		conceptsRequired.add(SMSS_JOB_RECIPES);
		conceptsRequired.add(SMSS_AUDIT_TRAIL);
		conceptsRequired.add(SMSS_EXECUTION);
		conceptsRequired.add(SMSS_JOB_TAGS);
	}

	public SchedulerOwlCreator(IDatabaseEngine schedulerDb) {
		this.schedulerDb = schedulerDb;
	}

	/**
	 * Determine if we need to remake the OWL
	 * 
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check
		 * Just looking at the tables
		 * Not doing anything with columns but should eventually do that
		 */

		List<String> cleanConcepts = new Vector<>();
		try {
			List<String> concepts = schedulerDb.getPhysicalConcepts();
			for (String concept : concepts) {
				if (concept.equals("http://semoss.org/ontologies/Concept")) {
					continue;
				}
				String cTable = Utility.getInstanceName(concept);
				cleanConcepts.add(cTable);
			}
		} catch(Exception e) {
			//ignore
		}

		boolean check1 = cleanConcepts.containsAll(conceptsRequired);
		if(check1) {
			List<String> props = schedulerDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/SMSS_AUDIT_TRAIL");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/IS_LATEST/SMSS_AUDIT_TRAIL")) {
				return true;
			}
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/SCHEDULER_OUTPUT/SMSS_AUDIT_TRAIL")) {
				return true;
			}
			
			props = schedulerDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/SMSS_JOB_RECIPES");
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/UI_STATE/SMSS_JOB_RECIPES")) {
				return true;
			}
			if(!props.contains("http://semoss.org/ontologies/Relation/Contains/CRON_TIMEZONE/SMSS_JOB_RECIPES")) {
				return true;
			}
			
		}
		return !check1;
	}

	/**
	 * Remake the OWL
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		// get the existing engine and close it
		RDFFileSesameEngine baseEngine = schedulerDb.getBaseDataEngine();
		if (baseEngine != null) {
			baseEngine.close();
		}

		// now delete the file if exists
		// and we will make a new
		String owlLocation = schedulerDb.getOWL();

		File f = new File(owlLocation);
		if (f.exists()) {
			Path path = Paths.get(owlLocation);
			Files.delete(path);
			// f.delete();
		}

		// write the new OWL
		writeNewOwl(owlLocation);

		// now reload into security db
		schedulerDb.setOWL(owlLocation);
	}

	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * 
	 * @param owlLocation
	 * @throws Exception 
	 */
	private void writeNewOwl(String owlLocation) throws Exception {
		Owler owler = new Owler(Constants.SCHEDULER_DB, owlLocation, DATABASE_TYPE.RDBMS);

		// QRTZ_CALENDARS
		owler.addConcept(QRTZ_CALENDARS, null, null);
		owler.addProp(QRTZ_CALENDARS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_CALENDARS, CALENDAR_NAME, VARCHAR_200);
		owler.addProp(QRTZ_CALENDARS, CALENDAR, IMAGE);

		// QRTZ_CRON_TRIGGERS
		owler.addConcept(QRTZ_CRON_TRIGGERS, null, null);
		owler.addProp(QRTZ_CRON_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_CRON_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_CRON_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_CRON_TRIGGERS, CRON_EXPRESSION, VARCHAR_120);
		owler.addProp(QRTZ_CRON_TRIGGERS, TIME_ZONE_ID, VARCHAR_80);

		// QRTZ_FIRED_TRIGGERS
		owler.addConcept(QRTZ_FIRED_TRIGGERS, null, null);
		owler.addProp(QRTZ_FIRED_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_FIRED_TRIGGERS, ENTRY_ID, VARCHAR_95);
		owler.addProp(QRTZ_FIRED_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_FIRED_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_FIRED_TRIGGERS, INSTANCE_NAME, VARCHAR_200);
		owler.addProp(QRTZ_FIRED_TRIGGERS, FIRED_TIME, BIGINT);
		owler.addProp(QRTZ_FIRED_TRIGGERS, SCHED_TIME, BIGINT);
		owler.addProp(QRTZ_FIRED_TRIGGERS, PRIORITY, INTEGER);
		owler.addProp(QRTZ_FIRED_TRIGGERS, STATE, VARCHAR_16);
		owler.addProp(QRTZ_FIRED_TRIGGERS, JOB_NAME, VARCHAR_200);
		owler.addProp(QRTZ_FIRED_TRIGGERS, JOB_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_FIRED_TRIGGERS, IS_NONCONCURRENT, BOOLEAN);
		owler.addProp(QRTZ_FIRED_TRIGGERS, REQUESTS_RECOVERY, BOOLEAN);

		// QRTZ_PAUSED_TRIGGER_GRPS
		owler.addConcept(QRTZ_PAUSED_TRIGGER_GRPS, null, null);
		owler.addProp(QRTZ_PAUSED_TRIGGER_GRPS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_PAUSED_TRIGGER_GRPS, TRIGGER_GROUP, VARCHAR_200);

		// QRTZ_SCHEDULER_STATE
		owler.addConcept(QRTZ_SCHEDULER_STATE, null, null);
		owler.addProp(QRTZ_SCHEDULER_STATE, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_SCHEDULER_STATE, INSTANCE_NAME, VARCHAR_200);
		owler.addProp(QRTZ_SCHEDULER_STATE, LAST_CHECKIN_TIME, BIGINT);
		owler.addProp(QRTZ_SCHEDULER_STATE, CHECKIN_INTERVAL, BIGINT);

		// QRTZ_LOCKS
		owler.addConcept(QRTZ_LOCKS, null, null);
		owler.addProp(QRTZ_LOCKS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_LOCKS, LOCK_NAME, VARCHAR_40);

		// QRTZ_JOB_DETAILS
		owler.addConcept(QRTZ_JOB_DETAILS, null, null);
		owler.addProp(QRTZ_JOB_DETAILS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_JOB_DETAILS, JOB_NAME, VARCHAR_200);
		owler.addProp(QRTZ_JOB_DETAILS, JOB_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_JOB_DETAILS, DESCRIPTION, VARCHAR_250);
		owler.addProp(QRTZ_JOB_DETAILS, JOB_CLASS_NAME, VARCHAR_250);
		owler.addProp(QRTZ_JOB_DETAILS, IS_DURABLE, BOOLEAN);
		owler.addProp(QRTZ_JOB_DETAILS, IS_NONCONCURRENT, BOOLEAN);
		owler.addProp(QRTZ_JOB_DETAILS, IS_UPDATE_DATA, BOOLEAN);
		owler.addProp(QRTZ_JOB_DETAILS, REQUESTS_RECOVERY, BOOLEAN);
		owler.addProp(QRTZ_JOB_DETAILS, JOB_DATA, IMAGE);

		// QRTZ_SIMPLE_TRIGGERS
		owler.addConcept(QRTZ_SIMPLE_TRIGGERS, null, null);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, REPEAT_COUNT, BIGINT);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, REPEAT_INTERVAL, BIGINT);
		owler.addProp(QRTZ_SIMPLE_TRIGGERS, TIMES_TRIGGERED, BIGINT);

		// qrtz_simprop_triggers
		owler.addConcept(QRTZ_SIMPROP_TRIGGERS, null, null);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, STR_PROP_1, VARCHAR_512);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, STR_PROP_2, VARCHAR_512);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, STR_PROP_3, VARCHAR_512);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, INT_PROP_1, INTEGER);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, INT_PROP_2, INTEGER);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, LONG_PROP_1, BIGINT);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, LONG_PROP_2, BIGINT);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, DEC_PROP_1, NUMERIC_13_4);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, DEC_PROP_2, NUMERIC_13_4);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, BOOL_PROP_1, BOOLEAN);
		owler.addProp(QRTZ_SIMPROP_TRIGGERS, BOOL_PROP_2, BOOLEAN);

		// QRTZ_BLOB_TRIGGERS
		owler.addConcept(QRTZ_BLOB_TRIGGERS, null, null);
		owler.addProp(QRTZ_BLOB_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_BLOB_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_BLOB_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_BLOB_TRIGGERS, BLOB_DATA, IMAGE);

		// QRTZ_TRIGGERS
		owler.addConcept(QRTZ_TRIGGERS, null, null);
		owler.addProp(QRTZ_TRIGGERS, SCHED_NAME, VARCHAR_120);
		owler.addProp(QRTZ_TRIGGERS, TRIGGER_NAME, VARCHAR_200);
		owler.addProp(QRTZ_TRIGGERS, TRIGGER_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_TRIGGERS, JOB_NAME, VARCHAR_200);
		owler.addProp(QRTZ_TRIGGERS, JOB_GROUP, VARCHAR_200);
		owler.addProp(QRTZ_TRIGGERS, DESCRIPTION, VARCHAR_250);
		owler.addProp(QRTZ_TRIGGERS, NEXT_FIRE_TIME, BIGINT);
		owler.addProp(QRTZ_TRIGGERS, PREV_FIRE_TIME, BIGINT);
		owler.addProp(QRTZ_TRIGGERS, PRIORITY, INTEGER);
		owler.addProp(QRTZ_TRIGGERS, TRIGGER_STATE, VARCHAR_16);
		owler.addProp(QRTZ_TRIGGERS, TRIGGER_TYPE, VARCHAR_8);
		owler.addProp(QRTZ_TRIGGERS, START_TIME, BIGINT);
		owler.addProp(QRTZ_TRIGGERS, END_TIME, BIGINT);
		owler.addProp(QRTZ_TRIGGERS, CALENDAR_NAME, VARCHAR_200);
		owler.addProp(QRTZ_TRIGGERS, MISFIRE_INSTR, SMALLINT);
		owler.addProp(QRTZ_TRIGGERS, JOB_DATA, IMAGE);

		// SMSS_JOB_RECIPES
		owler.addConcept(SMSS_JOB_RECIPES, null, null);
		owler.addProp(SMSS_JOB_RECIPES, USER_ID, VARCHAR_120);
		owler.addProp(SMSS_JOB_RECIPES, JOB_ID, VARCHAR_200);
		owler.addProp(SMSS_JOB_RECIPES, JOB_NAME, VARCHAR_200);
		owler.addProp(SMSS_JOB_RECIPES, JOB_GROUP, VARCHAR_200);
		owler.addProp(SMSS_JOB_RECIPES, CRON_EXPRESSION, VARCHAR_250);
		owler.addProp(SMSS_JOB_RECIPES, CRON_TIMEZONE, VARCHAR_120);
		owler.addProp(SMSS_JOB_RECIPES, PIXEL_RECIPE, BLOB);
		owler.addProp(SMSS_JOB_RECIPES, PIXEL_RECIPE_PARAMETERS, BLOB);
		owler.addProp(SMSS_JOB_RECIPES, JOB_CATEGORY, VARCHAR_200);
		owler.addProp(SMSS_JOB_RECIPES, TRIGGER_ON_LOAD, BOOLEAN);
		owler.addProp(SMSS_JOB_RECIPES, UI_STATE, BLOB);

		// SMSS_AUDIT_TRAIL
		owler.addConcept(SMSS_AUDIT_TRAIL, null, null);
		owler.addProp(SMSS_AUDIT_TRAIL, JOB_ID, VARCHAR_200);
		owler.addProp(SMSS_AUDIT_TRAIL, JOB_NAME, VARCHAR_200);
		owler.addProp(SMSS_AUDIT_TRAIL, JOB_GROUP, VARCHAR_200);
		owler.addProp(SMSS_AUDIT_TRAIL, EXECUTION_START, TIMESTAMP);
		owler.addProp(SMSS_AUDIT_TRAIL, EXECUTION_END, TIMESTAMP);
		owler.addProp(SMSS_AUDIT_TRAIL, EXECUTION_DELTA, VARCHAR_255);
		owler.addProp(SMSS_AUDIT_TRAIL, SUCCESS, BOOLEAN);
		owler.addProp(SMSS_AUDIT_TRAIL, IS_LATEST, BOOLEAN);
		owler.addProp(SMSS_AUDIT_TRAIL, SCHEDULER_OUTPUT, CLOB);

		// SMSS_JOB_TAGS
		owler.addConcept(SMSS_JOB_TAGS, null, null);
		owler.addProp(SMSS_JOB_TAGS, JOB_ID, VARCHAR_200);
		owler.addProp(SMSS_JOB_TAGS, JOB_TAG, VARCHAR_200);

		// SMSS_EXECUTION
		owler.addConcept(SMSS_EXECUTION, null, null);
		owler.addProp(SMSS_EXECUTION, EXEC_ID, VARCHAR_200);
		owler.addProp(SMSS_EXECUTION, JOB_NAME, VARCHAR_200);
		owler.addProp(SMSS_EXECUTION, JOB_GROUP, VARCHAR_200);
		
		// add Foreign Keys/Relations
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS, QRTZ_CRON_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS, QRTZ_CRON_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_CRON_TRIGGERS, QRTZ_TRIGGERS, QRTZ_CRON_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);

		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPLE_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPLE_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_SIMPLE_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPLE_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);

		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPROP_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_TRIGGERS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPROP_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_TRIGGERS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_SIMPROP_TRIGGERS, QRTZ_TRIGGERS, QRTZ_SIMPROP_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_TRIGGERS + "." + TRIGGER_GROUP);
		
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, QRTZ_TRIGGERS + "." + SCHED_NAME + "." + QRTZ_JOB_DETAILS + "." + SCHED_NAME);
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, QRTZ_TRIGGERS + "." + TRIGGER_NAME + "." + QRTZ_JOB_DETAILS + "." + TRIGGER_NAME);
		owler.addRelation(QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, QRTZ_TRIGGERS + "." + TRIGGER_GROUP + "." + QRTZ_JOB_DETAILS + "." + TRIGGER_GROUP);

		owler.commit();
		owler.export();
	}
}
