package prerna.sablecc2.om;

public enum PixelOperationType {

	// JOB INFORMATION
	TASK,
	RESET_PANEL_TASKS,
	REMOVE_TASK,
	TASK_DATA,
	TASK_METADATA,
	
	// QUERY
	QUERY,
	
	// FRAME INFORMATION
	FRAME,
	FRAME_DATA_CHANGE,
	FRAME_HEADERS_CHANGE,
	// USED FOR ADDITIONAL OUTPUT
	ADD_HEADERS,
	REMOVE_HEADERS,
	MODIFY_HEADERS,
	
	FRAME_HEADERS,
	FRAME_FILTER,
	FILTER_MODEL,
	FRAME_METAMODEL,
	
	// PANEL OPERATIONS
	PANEL,
	PANEL_COLOR_BY_VALUE,
	PANEL_OPEN,
	PANEL_CLOSE,
	PANEL_ORNAMENT,
	PANEL_VIEW,
	PANEL_LABEL,
	PANEL_HEADER,
	PANEL_CLONE,
	PANEL_COMMENT,
	PANEL_EVENT,
	PANEL_POSITION,
	PANEL_FILTER,
	PANEL_SORT,
	
	// EXTERNAL WINDOWS
	OPEN_TAB,
	
	// META DATA INFORMATION
	APP_INFO,
	APP_INSIGHTS,
	
	// these are the new traverse options
	// TODO: go back and see which of these FE 
	// still needs
	DATABASE_TABLE_STRUCTURE,
	DATABASE_TRAVERSE_OPTIONS,
	
	// forms calls that change the db
	ALTER_DATABASE,
	
	TRAVERSAL_OPTIONS,
	WIKI_LOGICAL_NAMES,
	CONNECTED_CONCEPTS,
	DATABASE_LIST,
	DATABASE_METAMODEL,
	DATABASE_CONCEPTS,
	DATABASE_CONCEPT_PROPERTIES,
	LOGICAL_NAMES,
	
	// APP SPECIFIC WIDGETS
	APP_WIDGETS, 
	
	//Database
	DELETE_ENGINE,
	
	// INSIGHT INFORMATION
	CURRENT_INSIGHT_RECIPE,
	SAVED_INSIGHT_RECIPE,
	OPEN_SAVED_INSIGHT,
	NEW_EMPTY_INSIGHT,
	DROP_INSIGHT,
	CLEAR_INSIGHT,
	INSIGHT_HANDLE,
	SAVE_INSIGHT,
	INSIGHT_ORNAMENT,
	DELETE_INSIGHT,
	
	// DASHBAORD
	DASHBOARD_INSIGHT_CONFIGURATION,
	
	// RUNNING JAVA CODE
	CODE_EXECUTION,
	// MULTI OUTPUT
	VECTOR,
	
	// ROUTINES THAT SEND BACK DATA TO VISUALIZE WITH A LAYOUT
	VIZ_OUTPUT,
	
	// FILE DOWNLOAD
	FILE_DOWNLOAD,
	
	// OLD INSIGHT
	OLD_INSIGHT,
	PLAYSHEET_PARAMS,
	
	// GIT_MARKET
	MARKET_PLACE, // general market routine
	MARKET_PLACE_INIT,
	MARKET_PLACE_ADDITION,
	
	// SCHEDULER INFORMATION
	SCHEDULE_JOB,
	LIST_JOB, 
	RESCHEDULE_JOB, 
	UNSCHEDULE_JOB, 
	
	//GA ANALYTICS
	VIZ_RECOMMENDATION,
	RECOMMENDATION,

	// CLOUD
	GOOGLE_SHEET_LIST,
	GOOGLE_DRIVE_LIST,
	CLOUD_FILE_LIST,
	
	// SOME KIND OF OPERATION THAT WE WANT TO OUTPUT
	HELP,
	RECIPE_COMMENT,
	OPERATION,
	WARNING,
	
	FRAME_SIZE_LIMIT_EXCEEDED,
	UNEXECUTED_PIXELS,
	ERROR,
	
	USER_INPUT_REQUIRED,
	LOGGIN_REQUIRED_ERROR,
	
	CHECK_R_PACKAGES,

	INVALID_SYNTAX;
}
