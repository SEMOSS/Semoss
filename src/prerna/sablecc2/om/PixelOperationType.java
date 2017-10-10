package prerna.sablecc2.om;

public enum PixelOperationType {

	// JOB INFORMATION
	TASK,
	REMOVE_TASK,
	TASK_DATA,
	TASK_METADATA,
	
	// FRAME INFORMATION
	FRAME,
	FRAME_DATA_CHANGE,
	FRAME_HEADERS_CHANGE,
	
	FRAME_HEADERS,
	FRAME_FILTER,
	FILTER_MODEL,
	FRAME_METAMODEL,
	
	// PANEL OPERATIONS
	PANEL,
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
	TRAVERSAL_OPTIONS,
	WIKI_LOGICAL_NAMES,
	CONNECTED_CONCEPTS,
	DATABASE_LIST,
	DATABASE_METAMODEL,
	DATABASE_CONCEPTS,
	DATABASE_CONCEPT_PROPERTIES,
	
	// INSIGHT INFORMATION
	SAVED_INSIGHT_RECIPE,
	OPEN_SAVED_INSIGHT,
	NEW_EMPTY_INSIGHT,
	DROP_INSIGHT,
	CLEAR_INSIGHT,
	INSIGHT_HANDLE,
	SAVE_INSIGHT,
	INSIGHT_ORNAMENT,
	
	// DASHBAORD
	DASHBOARD_INSIGHT_CONFIGURATION,
	
	// RUNNING JAVA CODE
	CODE_EXECUTION,
	
	// ROUTINES THAT SEND BACK DATA TO VISUALIZE WITH A LAYOUT
	VIZ_OUTPUT,
	
	// OLD INSIGHT
	OLD_INSIGHT,
	
	// SOME KIND OF OPERATION THAT WE WANT TO OUTPUT
	OPERATION,
	WARNING,
	ERROR; 
}
