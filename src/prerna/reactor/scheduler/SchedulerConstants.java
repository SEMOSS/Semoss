package prerna.reactor.scheduler;

public final class SchedulerConstants {

	private SchedulerConstants() {}

	// Quartz tables
	public static final String QRTZ_CALENDARS = "QRTZ_CALENDARS";
	public static final String QRTZ_CRON_TRIGGERS = "QRTZ_CRON_TRIGGERS";
	public static final String QRTZ_FIRED_TRIGGERS = "QRTZ_FIRED_TRIGGERS";
	public static final String QRTZ_PAUSED_TRIGGER_GRPS = "QRTZ_PAUSED_TRIGGER_GRPS";
	public static final String QRTZ_SCHEDULER_STATE = "QRTZ_SCHEDULER_STATE";
	public static final String QRTZ_LOCKS = "QRTZ_LOCKS";
	public static final String QRTZ_JOB_DETAILS = "QRTZ_JOB_DETAILS";
	public static final String QRTZ_SIMPLE_TRIGGERS = "QRTZ_SIMPLE_TRIGGERS";
	public static final String QRTZ_SIMPROP_TRIGGERS = "QRTZ_SIMPROP_TRIGGERS";
	public static final String QRTZ_BLOB_TRIGGERS = "QRTZ_BLOB_TRIGGERS";
	public static final String QRTZ_TRIGGERS = "QRTZ_TRIGGERS";

	// Semoss tables
	public static final String SMSS_JOB_RECIPES = "SMSS_JOB_RECIPES";
	public static final String SMSS_AUDIT_TRAIL = "SMSS_AUDIT_TRAIL";
	public static final String SMSS_EXECUTION = "SMSS_EXECUTION";
	public static final String SMSS_JOB_TAGS  = "SMSS_JOB_TAGS";

	// Column Headers
	public static final String SCHED_NAME = "SCHED_NAME";
	public static final String CALENDAR_NAME = "CALENDAR_NAME";
	public static final String CALENDAR = "CALENDAR";
	public static final String TRIGGER_NAME = "TRIGGER_NAME";
	public static final String TRIGGER_GROUP = "TRIGGER_GROUP";
	public static final String CRON_EXPRESSION = "CRON_EXPRESSION";
	public static final String CRON_TIMEZONE = "CRON_TIMEZONE";
	public static final String TIME_ZONE_ID = "TIME_ZONE_ID";
	public static final String ENTRY_ID = "ENTRY_ID";
	public static final String INSTANCE_NAME = "INSTANCE_NAME";
	public static final String FIRED_TIME = "FIRED_TIME";
	public static final String SCHED_TIME = "SCHED_TIME";
	public static final String PRIORITY = "PRIORITY";
	public static final String STATE = "STATE";
	public static final String JOB_TAG = "JOB_TAG";
	public static final String JOB_ID = "JOB_ID";
	public static final String JOB_NAME = "JOB_NAME";
	public static final String JOB_TAGS = "JOB_TAGS";
	public static final String JOB_GROUP = "JOB_GROUP";
	public static final String IS_NONCONCURRENT = "IS_NONCONCURRENT";
	public static final String REQUESTS_RECOVERY = "REQUESTS_RECOVERY";
	public static final String LAST_CHECKIN_TIME = "LAST_CHECKIN_TIME";
	public static final String CHECKIN_INTERVAL = "CHECKIN_INTERVAL";
	public static final String LOCK_NAME = "LOCK_NAME";
	public static final String DESCRIPTION = "DESCRIPTION";
	public static final String JOB_CLASS_NAME = "JOB_CLASS_NAME";
	public static final String IS_DURABLE = "IS_DURABLE";
	public static final String IS_UPDATE_DATA = "IS_UPDATE_DATA";
	public static final String JOB_DATA = "JOB_DATA";
	public static final String REPEAT_COUNT = "REPEAT_COUNT";
	public static final String REPEAT_INTERVAL = "REPEAT_INTERVAL";
	public static final String TIMES_TRIGGERED = "TIMES_TRIGGERED";
	public static final String STR_PROP_1 = "STR_PROP_1";
	public static final String STR_PROP_2 = "STR_PROP_2";
	public static final String STR_PROP_3 = "STR_PROP_3";
	public static final String INT_PROP_1 = "INT_PROP_1";
	public static final String INT_PROP_2 = "INT_PROP_2";
	public static final String LONG_PROP_1 = "LONG_PROP_1";
	public static final String LONG_PROP_2 = "LONG_PROP_2";
	public static final String DEC_PROP_1 = "DEC_PROP_1";
	public static final String DEC_PROP_2 = "DEC_PROP_2";
	public static final String BOOL_PROP_1 = "BOOL_PROP_1";
	public static final String BOOL_PROP_2 = "BOOL_PROP_2";
	public static final String BLOB_DATA = "BLOB_DATA";
	public static final String START_TIME = "START_TIME";
	public static final String END_TIME = "END_TIME";
	public static final String MISFIRE_INSTR = "MISFIRE_INSTR";
	public static final String NEXT_FIRE_TIME = "NEXT_FIRE_TIME";
	public static final String PREV_FIRE_TIME = "PREV_FIRE_TIME";
	public static final String TRIGGER_STATE = "TRIGGER_STATE";
	public static final String TRIGGER_TYPE = "TRIGGER_TYPE";
	public static final String USER_ID = "USER_ID";
	public static final String PIXEL_RECIPE = "PIXEL_RECIPE";
	public static final String PIXEL_RECIPE_PARAMETERS = "PIXEL_RECIPE_PARAMETERS";
	public static final String EXECUTION_START = "EXECUTION_START";
	public static final String EXECUTION_END = "EXECUTION_END";
	public static final String EXECUTION_DELTA = "EXECUTION_DELTA";
	public static final String SUCCESS = "SUCCESS";
	public static final String JOB_CATEGORY = "JOB_CATEGORY";
	public static final String TRIGGER_ON_LOAD = "TRIGGER_ON_LOAD";
	public static final String UI_STATE = "UI_STATE";
	public static final String EXEC_ID = "EXEC_ID";
	public static final String IS_LATEST = "IS_LATEST";
	public static final String SCHEDULER_OUTPUT = "SCHEDULER_OUTPUT";
	
	// SQL data types
	public static final String VARCHAR_8 = "VARCHAR (8)";
	public static final String VARCHAR_16 = "VARCHAR (16)";
	public static final String VARCHAR_40 = "VARCHAR (40)";
	public static final String VARCHAR_80 = "VARCHAR (80)";
	public static final String VARCHAR_95 = "VARCHAR (95)";
	public static final String VARCHAR_120 = "VARCHAR (120)";
	public static final String VARCHAR_200 = "VARCHAR (200)";
	public static final String VARCHAR_250 = "VARCHAR (250)";
	public static final String VARCHAR_255 = "VARCHAR (255)";
	public static final String VARCHAR_512 = "VARCHAR (512)";

	public static final String INTEGER = "INTEGER";
	public static final String BOOLEAN = "BOOLEAN";
	public static final String BIT = "BIT";
	public static final String BIGINT = "BIGINT";
	public static final String SMALLINT = "SMALLINT";
	// need to do this based on the sql type
	@Deprecated
	public static final String IMAGE = "IMAGE";
	public static final String NUMERIC_13_4 = "NUMERIC(13,4)";
	public static final String TIMESTAMP = "TIMESTAMP";
	public static final String BLOB = "BLOB";
	public static final String CLOB = "CLOB";
	
	// Constraints
	public static final String NOT_NULL = "NOT NULL";
}
