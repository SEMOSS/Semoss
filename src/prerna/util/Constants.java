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
package prerna.util;

/**
 * This class contains all of the constants referenced elsewhere in the code.
 */
public class Constants {

	public static final String DIHELPER_PROP_FILE_LOCATION = "DIHELPER_PROP_FILE_LOCATION";
	public static final String PLAYSHEETS_DEFINED = "PLAYSHEETS_DEFINED";
	public static final String SUBJECT = "SUBJECT";
	public static final String OBJECT = "OBJECT";
	public static final String TRANSPARENT = "TRANSPARENT";
	public static final String ENTER_TEXT = "Enter your search text here";
	public static final String ENTER_SEARCH_DISABLED_TEXT = "Search is disabled for faster processing";
	public static final String DB_NAME_FIELD = "dbSelectorField";
	public static final String IMPORT_FILE_FIELD = "importFileNameField";
	public static final String BASE_URI_TEXT_FIELD = "customBaseURItextField";
	public static final String IMPORT_COMBOBOX = "dbImportTypeComboBox";
	public static final String IMPORT_TYPE_COMBOBOX = "loadingFormatComboBox";
	public static final String IMPORT_CSV_IMPORT_LBL = "csvPropLbl";
	public static final String IMPORT_CSV_FILE_BUTTON = "csvPropBrowseBtn";
	public static final String IMPORT_CSV_FILE_FIELD = "csvPropFilenameField";
	public static final String IMPORT_TYPE_LABEL = "lblDataInputFormat";
	public static final String IMPORT_PANEL = "dbImportPanel";
	public static final String IMPORT_ENTERDB_LABEL = "dbNameLbl";
	public static final String IMPORT_FILE_LABEL = "selectionFileLbl";
	public static final String IMPORT_BUTTON_BROWSE = "fileBrowseBtn";
	public static final String IMPORT_BUTTON = "importButton";
	public static final String AUTO_GENERATE_INSIGHTS_CHECK_BOX = "autoGenerateInsights";
	public static final String MAP_BROWSE_BUTTON = "mapBrowseBtn";
	public static final String DB_PROP_BROWSE_BUTTON = "dbPropBrowseButton";
	public static final String QUESTION_BROWSE_BUTTON = "questionBrowseButton";
	public static final String MAP_TEXT_FIELD = "importMapFileNameField";
	public static final String DB_PROP_TEXT_FIELD = "dbPropFileNameField";
	public static final String QUESTION_TEXT_FIELD = "questionFileNameField";
	public static final String ADVANCED_IMPORT_OPTIONS_PANEL = "advancedImportOptionsPanel";
	public static final String ADVANCED_IMPORT_OPTIONS_BUTTON = "btnShowAdvancedImportFeatures";
	public static final String IMPORT_RDBMS_URL_LABEL = "lblDBImportURL";
	public static final String IMPORT_RDBMS_URL_FIELD = "dbImportURLField";
	public static final String IMPORT_RDBMS_DRIVER_LABEL = "lblDBImportDriverType";
	public static final String IMPORT_RDBMS_DRIVER_COMBOBOX = "dbImportRDBMSDriverComboBox";
	public static final String IMPORT_RDBMS_USERNAME_LABEL = "lblDBImportUsername";
	public static final String IMPORT_RDBMS_USERNAME_FIELD = "dbImportUsernameField";
	public static final String IMPORT_RDBMS_PW_LABEL = "lblDBImportPW";
	public static final String IMPORT_RDBMS_PW_FIELD = "dbImportPWField";
	public static final String TEST_RDBMS_CONNECTION = "btnTestRDBMSConnection";
	public static final String GET_RDBMS_SCHEMA = "btnGetRDBMSSchema";
	public static final String IMPORT_MAP_LABEL = "lblselectCustomMap";
	public static final String DBCM_Prop = "DBCM_Prop";
	public static final String EMPTY = "@@";
	public static final String LAYOUT = "LAYOUT";
	public static final String VERTEX_NAME = "VERTEX_LABEL_PROPERTY";
	public static final String VERTEX_TYPE = "VERTEX_TYPE_PROPERTY";
	public static final String VERTEX_COLOR = "VERTEX_COLOR_PROPERTY";
	public static final String GENERIC_IMAGE = "GENERIC";
	public static final String PERSPECTIVE = "PERSPECTIVE";
	public static final String INEDGE_COUNT = "Inputs";
	public static final String OUTEDGE_COUNT = "Outputs";
	public static final String PROCESS_CURRENT_DATE = "PROCESS_CURRENT_DATE";
	public static final String PROCESS_CURRENT_USER = "PROCESS_CURRENT_USER";
	public static final String CURRENT_PLAYSHEET = "layoutValue";
	public static final String BASE_FOLDER = "BaseFolder";

	public static final String LOCAL_MASTER_DB_NAME = "LocalMasterDatabase";
	
	// graphplaysheet option constants
	// layouts
	public static final String GPSSudowl = "GPS_SUDOWL_DEFAULT";
	public static final String GPSSearch = "GPS_SEARCH_DEFAULT";
	public static final String GPSProp = "GPS_PROPERTIES_DEFAULT";
	public static final String highQualityExport = "GPSHighQualityExport";
	public static final String sudowlCheck = "sudowlCheck";
	public static final String searchCheck = "searchCheck";
	public static final String propertyCheck = "propertyCheck";
	public static final String highQualityExportCheck = "highQualityExportCheck";

	// Used by POIReader
	public static final String RELATION_URI_CONCATENATOR = ":"; // used in
	// between the
	// in node and
	// out node for
	// relation
	// instance
	// uris.
	public static final String DEFAULT_NODE_CLASS = "Concept";
	public static final String DEFAULT_RELATION_CLASS = "Relation";
	public static final String SUBPROPERTY_URI = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
	public static final String SUBCLASS_URI = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
	public static final String CLASS_URI = "http://www.w3.org/2000/01/rdf-schema#Class";
	public static final String DEFAULT_PROPERTY_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	public static final String SEMOSS_URI = "SEMOSS_URI";
	public static final String CLASS = "_CLASS";
	public static final String TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String DEFAULT_PROPERTY_CLASS = "Relation/Contains";
	
//	public static final String DISPLAY_NAME = "DISPLAY_NAME";
//	public static final String DEFAULT_DISPLAY_CLASS = "DisplayName";
//	public static final String DEFAULT_DISPLAY_NAME = "DisplayName";
//	public static String DISPLAY_URI =  Constants.BASE_URI + Constants.DEFAULT_DISPLAY_CLASS + "/";

	public static final String DEFAULT_PHYSICAL_NAME = "PhysicalName";
	
	public static String BASE_URI = "http://semoss.org/ontologies/";
	public static String CONCEPT_URI = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/";
	public static String PROPERTY_URI =  Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/";
	
	// layouts
	public static final String FR = "Fruchterman-Reingold";
	public static final String KK = "Kamada-Kawai";
	public static final String SPRING = "Spring-Layout";
	public static final String SPRING2 = "Spring-Layout2";
	public static final String CIRCLE_LAYOUT = "Circle-Layout";
	public static final String ISO = "ISO-Layout";
	public static final String TREE_LAYOUT = "Tree-Layout";
	public static final String RADIAL_TREE_LAYOUT = "Radial-Tree-Layout";
	public static final String BALLOON_LAYOUT = "Balloon Layout";

	public static final String LEGEND = "_LEGEND";

	public static final String DESCR = "DESCRIPTION";
	public static final String QUERY = "QUERY";
	public static final String CUSTOMIZE_SPARQL = "btnCustomSparql";
	public static final String GET_CURRENT_SPARQL = "btnGetQuestionSparql";
	public static final String SHOW_HINT = "btnShowHint";
	public static final String SHOW_PLAYSHEETS_LIST = "btnShowPlaySheets";
	public static final String ENGINE = "ENGINE";
	public static final String ENGINE_ALIAS = "ENGINE_ALIAS";
	public static final String HIDDEN_DATABASE = "HIDDEN_DATABASE";
	public static final String ENGINES = "ENGINES";
	public static final String ENGINE_PROPERTIES_FILE = "ENGINE_PROP";
	public static final String ENGINE_TYPE = "ENGINE_TYPE";
	public static final String LISTENERS = "LISTENERS";
	public static final String MODEL = "MODEL";
	public static final String CONTROL = "CONTROLLER";
	public static final String VIEW = "VIEW";
	public static final String PARENT_VIEW = "PARENT_VIEW";
	public static final String RIGHT_VIEW = "RIGHT_VIEW";
	public static final String SPARQL_AREA_FIELD = "sparqlArea";
	public static final String PARAM_PANEL_FIELD = "paramPanel";
	public static final String QUESTION_LIST_FIELD = "questionSelector";
	public static final String RIGHT_VIEW_FIELD = "rightView";
	public static final String MAIN_FRAME = "playPane";
	public static final String DESKTOP_PANE = "desktopPane";

	// colors
	public static final String BROWN = "BROWN";
	public static final String RED = "RED";
	public static final String GREEN = "GREEN";
	public static final String BLUE = "BLUE";
	public static final String ORANGE = "ORANGE";
	public static final String YELLOW = "YELLOW";
	public static final String PURPLE = "PURPLE";
	public static final String AQUA = "AQUA";
	public static final String MAGENTA = "MAGENTA";
	public static final String BLACK = "BLACK";
	public static final String CYAN = "CYAN";
	public static final String DARK_GRAY = "DARK_GRAY";
	public static final String LIGHT_GRAY = "LIGHT_GRAY";

	// shapes
	public static final String SQUARE = "SQUARE";
	public static final String TRIANGLE = "TRIANGLE";
	public static final String DIAMOND = "DIAMOND";
	public static final String STAR = "STAR";
	public static final String CIRCLE = "CIRCLE";
	public static final String HEXAGON = "HEXAGON";
	public static final String PENTAGON = "PENTAGON";

	public static final String TYPE_QUERY = "TYPE_QUERY";
	public static final String TCCALC_PROGRESS_BAR = "calcTCprogressBar";
	public static final String TMCALC_PROGRESS_BAR = "calcTMprogressBar";
	public static final String ENTITY = "entity";
	public static final String REPO_LIST = "repoList";
	public static final String FILTER_TABLE = "filterTable";
	public static final String EDGE_TABLE = "edgeTable";
	public static final String PROP_TABLE = "propertyTable";
	public static final String FILTER_PANEL = "filterPanel";
	public static final String PLAYSHEETS = "playsheetList";
	public static final String APPEND = "appendButton";
	public static final String PROP_URI = "PROP_URI";
	public static final String PREDICATE_URI = "PREDICATE_URI";
	public static final String EDGE_NAME = "EDGE_NAME";
	public static final String EDGE_TYPE = "EDGE_TYPE";
	public static final String OPTION = "OPTION"; // used by entity filler
	public static final String EDGE_ADJUSTER_TABLE = "edgeAdjusterTable";
	public static final String LABEL_TABLE = "labelTable";
	public static final String TOOLTIP_TABLE = "tooltipTable";
	public static final String BUSINESS_VALUE = "System/Business_Value";
	public static final String CAPABILITY_BUSINESS_VALUE = "Capability/Business_Value";
	public static final String BUSINESS_PROCESS_BUSINESS_VALUE = "BusinessProcess/Business_Value";
	public static final String CALC_MATRIX = "_Matrix";
	public static final String CALC_MATRIX_EXT_STAB = "_Matrix_Ext_Stab";
	public static final String CALC_MATRIX_TECH_STD = "_Matrix_Tech_Std";
	public static final String CALC_PROPERTY_LABEL = "_PropLabels";
	public static final String CALC_COLUMN_LABELS = "_ColLabels";
	public static final String CALC_ROW_LABELS = "_RowLabels";
	public static final String CALC_EXAMPLE_EDGE = "_ExEdge";
	public static final String URI = "URI";
	public static final String CALC_NAMES_LIST = "_Names";
	public static final String CALC_NAMES_TECH_STD_LIST = "_Names_Tech_Std";
	public static final String GRID_VIEW = "prerna.ui.components.playsheets.GridPlaySheet";
	public static final String PROP_HASH = "_PropHash";
	public static final String TRAVERSE_JENA_MODEL = "traverseJenaModel";
	public static final String UNDO_BOOLEAN = "undoBoolean";
	public static final String UNDOBTN = "undoBtn";
	public static final String TECH_MATURITY = "System/Tech_Maturity";
	public static final String TRANSITION_COSTS = "TRANSITION_COSTS";
	public static final String TM_LIFECYCLE = "_Tech_Maturity_Lifecycle";
	public static final String TM_CATEGORY = "_Tech_Maturity_Category";
	public static final String TC_OVERHEAD_ARRAY = "_TC_Overhead_Array";
	public static final String TC_CORE_MATRIX = "_TC_Core_Matrix";
	public static final String TC_SDLC_CORE_MATRIX = "_TC_SDLC_Core_Matrix";
	public static final String RDF_FILE_NAME = "RDF_FILE_NAME";
	public static final String RDF_FILE_TYPE = "RDF_FILE_TYPE";
	public static final String RDF_FILE_BASE_URI = "RDF_FILE_BASE_URI";
	public static final String OBJECT_PROP_TABLE = "objectPropertiesTable";
	public static final String DATA_PROP_TABLE = "dataPropertiesTable";
	public static final String OBJECT_PROP_STRING = "objectPropertiesString";
	public static final String DATA_PROP_STRING = "dataPropertiesString";
	public static final String COLOR_SHAPE_TABLE = "colorShapeTable";
	public static final String SIZE_TABLE = "sizeTable";
	public static final String EXTEND_TABLE = "extendTable";
	public static final String SUBMIT_BUTTON = "submitButton";
	public static final String TRANS_ALL_FRAME = "transAllFrame";
	public static final String TRANS_ALL_SYSTEM_LABEL = "sysNoLabel";
	public static final String TRANS_ALL_DATA_LABEL = "dataNoLabel";
	public static final String TRANS_ALL_ICD_LABEL = "icdNoLabel";
	public static final String TRANS_ALL_SYSTEM_AREA = "transAllSysArea";
	public static final String TRANS_ALL_DATA_AREA = "transAllDataArea";
	public static final String TRANS_ALL_ICD_AREA = "transAllICDArea";
	public static final String TRANS_ALL_WSPRO_AREA = "transAllWSPArea";
	public static final String TRANS_ALL_WSCON_AREA = "transAllWSCArea";
	public static final String PLAYSHEET_COMBOBOXLIST = "playSheetComboBox";
	public static final String SPARQLLABEL = "sparqlLbl";
	public static final String BLANK_URL = "http://bornhere.com/noparent/blank/";
	public static final String PPT_TRAINING_BUTTON = "pptTrainingBtn";
	public static final String HTML_TRAINING_BUTTON = "htmlTrainingBtn";

	// Traverse Freely Queries
	public static final String NEIGHBORHOOD_TYPE_QUERY = "NEIGHBORHOOD_TYPE_QUERY";
	public static final String NEIGHBORHOOD_TYPE_QUERY_JENA = "NEIGHBORHOOD_TYPE_QUERY_JENA";
	public static final String NEIGHBORHOOD_OBJECT_QUERY = "NEIGHBORHOOD_OBJECT_QUERY";
	public static final String NEIGHBORHOOD_PREDICATE_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_FINDER_QUERY";
	public static final String NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY";
	public static final String NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY";
	public static final String TRAVERSE_FREELY_QUERY = "TRAVERSE_FREELY_QUERY";
	public static final String TRAVERSE_FREELY_QUERY_MULTI_TYPE = "TRAVERSE_FREELY_QUERY_MULTI_TYPE";
	public static final String TRAVERSE_FREELY_QUERY_JENA = "TRAVERSE_FREELY_QUERY_JENA";
	public static final String TRAVERSE_INSTANCE_FREELY_QUERY = "TRAVERSE_INSTANCE_FREELY_QUERY";
	public static final String SUBJECT_TYPE_QUERY = "SUBJECT_TYPE_QUERY";
	public static final String PREDICATE = "PREDICATE";
	public static final String IGNORE_URI = "IGNORE_URI";

	// TAP Cost Transition Queries
	public static final String TRANSITION_COST_INSERT_WITH_OVERHEAD = "TRANSITION_COST_INSERT_WITH_OVERHEAD";
	public static final String TRANSITION_COST_INSERT_WITHOUT_OVERHEAD = "TRANSITION_COST_INSERT_WITHOUT_OVERHEAD";
	public static final String TRANSITION_COST_DELETE = "TRANSITION_COST_DELETE";
	public static final String TRANSITION_COST_INSERT_SITEGLITEM = "TRANSITION_COST_INSERT_SITEGLITEM";
	public static final String TRANSITION_QUERY_SEPARATOR = "&";
	public static final String TRANSITION_COST_INSERT_SUSTAINMENT = "TRANSITION_COST_INSERT_SUSTAINMENT";
	public static final String TRANSITION_COST_INSERT_TRAINING = "TRANSITION_COST_INSERT_TRAINING";
	public static final String TRANSITION_COST_INSERT_SEMANTICS = "TRANSITION_COST_INSERT_SEMANTICS";
	public static final String TRANSITION_DATA_FEDERATION_PHASE_INDEPENDENT = "TRANSITION_DATA_FEDERATION_PHASE_INDEPENDENT";
	public static final String TRANSITION_REPORT_COMBO_BOX = "transCostReportSystemComboBox";
	public static final String TRANSITION_REPORT_TYPE_COMBO_BOX = "transReportTypeComboBox";
	public static final String TRANSITION_REPORT_FORMAT_COMBO_BOX = "transReportFormatComboBox";
	public static final String TRANSITION_APPLY_OVERHEAD_RADIO = "rdbtnApplyTapOverhead";
	public static final String TRANSITION_NOT_APPLY_OVERHEAD_RADIO = "rdbtnDoNotApplyOverhead";
	public static final String TRANSITION_SERVICE_PANEL = "transitionServicePanel";
	public static final String SOURCE_SELECT_PANEL = "sourceSelectPanel";
	public static final String TRANSITION_CHECK_BOX_DATA_FED = "chckbxDataFederationTransReport";
	public static final String TRANSITION_CHECK_BOX_DATA_CONSUMER = "chckbxDataConsumer";
	public static final String TRANSITION_ITEM_GEN_BUTTON = "loadGenBtn";
	public static final String TRANSITION_CHECK_BOX_BLU_PROVIDER = "chckbxBLUprovider";
	public static final String TRANSITION_CHECK_BOX_DATA_GENERIC = "chckbxDataEsbImplementation";
	public static final String TRANSITION_CHECK_BOX_BLU_GENERIC = "chckbxBluEsbImplementation";
	public static final String TRANSITION_SYSTEM_DROP_DOWN_PANEL = "transReportSysDropDownPanel";
	public static final String TRANSITION_GENERIC_BLU = "TRANSITION_GENERIC_BLU";
	public static final String TRANSITION_GENERIC_DATA = "TRANSITION_GENERIC_DATA";
	public static final String TRANSITION_SPECIFIC_DATA_CONSUMER = "TRANSITION_SPECIFIC_DATA_CONSUMER";
	public static final String TRANSITION_DATA_FEDERATION = "TRANSITION_DATA_FEDERATION";
	public static final String TRANSITION_BLU_PROVIDER = "TRANSITION_BLU_PROVIDER";
	public static final String TRANSITION_SPECIFIC_SITE_CONSUMER = "TRANSITION_SPECIFIC_SITE_CONSUMER";
	public static final String ADVANCED_TRANSITION_FUNCTIONS_PANEL = "advancedFunctionsPanel";
	public static final String SERVICE_SELECTION_BUTTON = "serviceSelectionBtn";
	public static final String SERVICE_SELECTION_PANE = "serviceSelectScrollPane";
	public static final String SOURCE_SELECTION_PANE = "sourceSelectScrollPane";
	public static final String TIER1_CHECKBOX = "tierCheck1";
	public static final String TIER2_CHECKBOX = "tierCheck2";
	public static final String TIER3_CHECKBOX = "tierCheck3";

	// Used by optimization organizer
	public static final String TRANSITION_GENERIC_COSTS = "TRANSITION_GENERIC_COSTS";
	public static final String TRANSITION_CONSUMER_COSTS = "TRANSITION_CONSUMER_COSTS";
	public static final String TRANSITION_PROVIDER_COSTS = "TRANSITION_PROVIDER_COSTS";

	public static final String UPDATE_SPARQL_AREA = "customUpdateTextPane";

	// SOA Transition All
	public static final String SOA_TRANSITION_ALL_DATA_QUERY = "SOA_TRANSITION_ALL_DATA_QUERY";
	public static final String SOA_TRANSITION_ALL_GENERIC_DATA_QUERY = "SOA_TRANSITION_ALL_GENERIC_DATA_QUERY";
	public static final String SOA_TRANSITION_ALL_GENERIC_BLU_QUERY = "SOA_TRANSITION_ALL_GENERIC_BLU_QUERY";
	public static final String SOA_TRANSITION_ALL_BLU_QUERY = "SOA_TRANSITION_ALL_BLU_QUERY";

	public static final String RDBMS_INSIGHTS = "RDBMS_INSIGHTS";
	public static final String RDBMS_INSIGHTS_CONNECTION_URL_END = "RDBMS_INSIGHTS_CONNECTION_URL_END";
	
	public static final String PERSPECTIVE_SELECTOR = "perspectiveSelector";

	public static final String BROWSER_TYPE = "BROWSER_TYPE";

	public static final String SPARQL_QUERY_ENDPOINT = "SPARQL_QUERY_ENDPOINT";
	public static final String SPARQL_UPDATE_ENDPOINT = "SPARQL_UPDATE_ENDPOINT";

	public static final String DATA_LATENCY_WEEKS_TEXT = "dataLatencyMonthsTextField";
	public static final String DATA_LATENCY_DAYS_TEXT = "dataLatencyDaysTextField";
	public static final String DATA_LATENCY_HOURS_TEXT = "dataLatencyHoursTextField";

	public static final String HTML = "HTML";
	public static final String PROPERTY = "PROPERTY";

	// Load Sheet Export Panel
	public static final String EXPORT_LOAD_SHEET_SOURCE_COMBOBOX = "exportDataSourceComboBox";
	public static final String EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX = "subjectNodeTypeComboBox";
	public static final String EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX = "objectNodeTypeComboBox";
	public static final String EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX = "nodeRelationshipComboBox";
	public static final String EXPORT_LOAD_SHEET_MAX_LIMIT_MESSAGE = "lblMaxExportLimit";
	public static final String EXPORT_LOAD_SHEET_CLEAR_ALL_BUTTON = "btnClearAll";
	public static final String EXPORT_LOAD_SHEET_ADD_EXPORT_BUTTON = "btnAddExport";
	public static final int MAX_EXPORTS = 9;

	// Update Cost DB Panel
	public static final String CHANGED_DB_COMBOBOX = "changedDBComboBox";
	public static final String COST_DB_COMBOBOX = "costDBComboBox";

	public static final String COST_DB_BASE_URI_FIELD = "costDBBaseURIField";
	public static final String GLITEM_LOADING_SHEET = "LoadingSheets1.xlsx";
	public static final String WATCHERS = "WATCHERS";
	public static final String ENGINE_WATCHER = "ENGINE_WATCHER";

	public static final String GLITEM_CORE_LOADING_SHEET = "LoadingSheets1.xlsx";

	public static final String GLITEM_SITE_LOADING_SHEET = "Site_HWSW_GLItems.xlsx";
	public static final String OWL = "OWL";
	public static final String PIXEL_UPDATE = "PIXEL_UPDATE";
	public static final String DB = "DB";
	
	public static final String URL_PARAM = "URL_PARAM";
	public static final String PROPS = "PROPS";
	// public static final String TYPE_URI = "TYPE_URI";
	public static final String INSIGHT = "INSIGHT";
	public static final String LABEL = "LABEL";
	public static final String OUTPUT = "OUTPUT";
	public static final String TYPE = "TYPE";
	public static final String SPARQL = "SPARQL";

	public static final String ENGINE_WEB_WATCHER = "ENGINE_WEB_WATCHER";
	public static final String DEPEND = "DEPEND";
	public static final String REDIS_HOST = "REDIS_HOST";
	public static final String REDIS_PORT = "REDIS_PORT";
	public static final Object URI_BASE = "URI_BASE";
	
	public static final String INSIGHT_CACHE_DIR = "INSIGHT_CACHE_DIR";
	public static final String CSV_INSIGHT_CACHE_FOLDER = "CSV_INSIGHT_CACHE_FOLDER";
	
	// question modification
	public static final String LABEL_QUESTION_SELECT_PERSPECTIVE = "lblQuestionSelectPerspective";
	public static final String QUESTION_PERSPECTIVE_SELECTOR = "questionPerspectiveSelector";
	public static final String LABEL_SELECT_QUESTION = "lblSelectQuestion";
	public static final String QUESTION_MOD_SELECTOR = "questionModSelector";
	public static final String QUESTION_PERSPECTIVE_FIELD = "questionPerspectiveField";
	public static final String QUESTION_FIELD = "questionField";
	public static final String QUESTION_LAYOUT_FIELD = "questionLayoutField";
	public static final String QUESTION_SPARQL_TEXT_PANE = "questionSparqlTextPane";
	public static final String QUESTION_MOD_BUTTON = "questionModButton";
	public static final String ADD_QUESTION_BUTTON = "addQuestionButton";
	public static final String EDIT_QUESTION_BUTTON = "editQuestionButton";
	public static final String DELETE_QUESTION_BUTTON = "deleteQuestionButton";
	public static final String QUESTION_DB_SELECTOR = "questionDatabaseSelector";
	public static final String QUESTION_ADD_PARAMETER_BUTTON = "questionAddParameterButton";
	public static final String QUESTION_ADD_PARAMETER_COMBO_BOX = "addParameterComboBox";
	public static final String QUESTION_SHOW_MORE_OPTIONS_BUTTON = "questionMoreOptionsButton";
	public static final String LABEL_PARAMETER_DEPEND = "lblParameterDepend";
	public static final String PARAMETER_DEPEND_SCROLL = "parameterDependScroll";
	public static final String ADD_PARAMETER_DEPENDENCY_BUTTON = "addParameterDependencyButton";
	public static final String PARAMETER_QUERY_SCROLL = "parameterQueryScroll";
	public static final String LABEL_PARAMETER_QUERY = "lblParameterQuery";
	public static final String ADD_PARAMETER_QUERY_BUTTON = "addParameterQueryButton";
	public static final String LABEL_PARAMETER_DEPEND_LIST = "lblParameterDependList";
	public static final String PARAMETER_DEPEND_SCROLL_LIST = "parameterDependScrollList";
	public static final String LABEL_PARAMETER_QUERY_LIST = "lblParameterQueryList";
	public static final String PARAMETER_QUERY_SCROLL_LIST = "parameterQueryScrollList";
	public static final String PARAMETER_QUERY_TEXT_PANE = "parameterQueryTextPane";
	public static final String PARAMETER_DEPEND_TEXT_PANE = "parameterDependTextPane";
	public static final String PARAMETER_DEPEND_DELETE_BUTTON = "dependenciesDeleteButton";
	public static final String PARAMETER_DEPEND_EDIT_BUTTON = "dependenciesEditButton";
	public static final String PARAMETER_QUERY_DELETE_BUTTON = "parameterQueriesDeleteButton";
	public static final String PARAMETER_QUERY_EDIT_BUTTON = "parameterQueriesEditButton";
	public static final String PARAMETER_DEPENDENCIES_JLIST = "parameterDependList";
	public static final String PARAMETER_QUERIES_JLIST = "parameterQueryList";
	public static final String QUESTION_ORDER_COMBO_BOX = "questionOrderComboBox";
	public static final String PARAMETER_OPTION_EDIT_BUTTON = "optionsEditButton";
	public static final String PARAMETER_OPTION_DELETE_BUTTON = "optionsDeleteButton";
	public static final String LABEL_PARAMETER_OPTION_LIST = "lblParameterOptionList";
	public static final String PARAMETER_OPTION_SCROLL_LIST = "parameterOptionScrollList";
	public static final String PARAMETER_OPTION_SCROLL = "parameterOptionScroll";
	public static final String LABEL_PARAMETER_OPTION = "lblParameterOption";
	public static final String ADD_PARAMETER_OPTION_BUTTON = "addParameterOptionButton";
	public static final String PARAMETER_OPTION_TEXT_PANE = "parameterOptionTextPane";
	public static final String PARAMETER_OPTIONS_JLIST = "parameterOptionList";
	public static final String ENGINE_PROPERTIES = "PROPERTIES";
	public static final String QUESTION_MOD_PLAYSHEET_COMBOBOXLIST = "questionLayoutComboBox";
	public static final String QUESTION_MOD_PLAYSHEET_COMBO_LABEL = "lblQuestionLayoutText";
	public static final String QUESTION_XML_WARNING = "lblOldXMLWarning";

	// Compare Databases
	public static final String NEW_DB_COMBOBOX = "newDBComboBox";
	public static final String OLD_DB_COMBOBOX = "oldDBComboBox";
	public static final String DB_COMPARISON_BUTTON = "compareDBButton";
	
	public static final String CONNECTION_URL = "CONNECTION_URL";
//	public static final String TEMP_CONNECTION_URL = "TEMP_CONNECTION_URL";
	public static final String DRIVER = "DRIVER";
	public static final String USERNAME = "USERNAME";
	public static final String PASSWORD = "PASSWORD";
	public static final String RDBMS_TYPE = "RDBMS_TYPE";
	
	// Auto generate queries
	public static final String AUTO_GENERATE_INSIGHTS_FOR_ENGINE_COMBOBOX = "autoGenerateQueriesForEngineSelector";
	
	//Tool Panel Functions
	public static final String MHS_FUNCTIONS = "prerna.semoss.web.services.specific.tap.";
	
	// Security
	public static final String SECURITY_DB = "security";
	public static final String SECURITY_ENABLED = "security-enabled";
	public static final String SESSION_USER = "semoss_user";
	public static final String ANONYMOUS_USER_ID = "-1";
	public static final String AUTH_WHITELIST_FILE = "whitelist";
	
	// Theming
	public static final String THEMING_DB = "themes";
	
	//RDBMS specific
	public static final String USE_OUTER_JOINS = "USE_OUTER_JOINS";// if present and true use outer joins instead of inner joins
	public static final String USE_CONNECTION_POOLING = "USE_CONNECTION_POOLING";
	public static final String H2_BASE_CONNECTION_URL = "jdbc:h2:@" + Constants.BASE_FOLDER + "@" + System.getProperty("file.separator") + "@ENGINE" + Constants.ENGINE + "@"
			+ System.getProperty("file.separator") + "database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	
	//SOLR specific
	public static final String RELOAD_INSIGHTS = "RELOAD_INSIGHTS";
//	public static final String SOLR_RELOAD = "SOLR_RELOAD";
//	public static final String SOLR_EXPORT = "SOLR_EXPORT";
//	public static final String SOLR_SYSTEM_VAR_KEY = "solr.solr.home";
//	public static final String SOLR_HOME_DIR = "Solr";
	// this is used for both the index engine and solr enigne
//	public static final String SOLR_URL = "SOLR_BASE_URL";
//	public static final String SOLR_CORE_NAME = "SOLR_CORE_NAME";
	
	public static final String VALUE = "VALUE";
	public static final String NAME = "NAME";
	
	public static final String STORE = "STORE";
	public static final String INSIGHTS_DB = "RDBMS_INSIGHTS";
	public static final String DEFAULT_INSIGHT_DRIVER = "org.h2.Driver";
	public static final String DEFAULT_RDBMS_TYPE = "H2_DB";
	
	// prohibited headers
	public static final String PROBHIBITED_HEADERS = "PROBHIBITED_HEADERS";
	public static final String MARIADB_PORT = "MARIADB_PORT";
	public static final String SQL_Server_PORT = "SQLServer_PORT";
	
	// all frame limit
	public static final String FRAME_SIZE_LIMIT = "FRAME_SIZE_LIMIT";
	
	// h2 frame limit size before going on disk
	public static final String H2_IN_MEM_SIZE = "H2_IN_MEM_SIZE";
	// how much R memory to allocate
	public static final String R_MEM_LIMIT = "R_MEM_LIMIT";
	
	// is this server running locally
	public static final String LOCAL_DEPLOYMENT = "LOCAL_DEPLOYMENT";
	
	// if python is installed
	public static final String PYTHON = "PYTHON";
	public static final String USE_PYTHON = "USE_PYTHON";
	// which type of R connection to use
	public static final String USE_R = "USE_R";
	public static final String R_CONNECTION_JRI = "R_CONNECTION_JRI";
	
	// where google chrome is located for image capture
	public static final String GOOGLE_CHROME_HOME = "GOOGLE_CHROME_HOME";
	
	//Tinker engine
	public static final String TINKER_FILE = "TINKER_FILE";
	public static final String TINKER_DRIVER = "TINKER_DRIVER";
	
	// Working directories used for R
	public static final String R_BASE_FOLDER = "R";
	public static final String R_ANALYTICS_SCRIPTS_FOLDER = "AnalyticsRoutineScripts";
	public static final String R_USER_SCRIPTS_FOLDER = "UserScripts";
	public static final String R_MATCHING_FOLDER = "Matching";
	public static final String R_MATCHING_CSVS_FOLDER = "MatchingCsvs";
	public static final String R_MATCHING_PROP_FOLDER = "MatchingProp";
	public static final String R_MATCHING_REPO_FOLDER = "MatchingRepository";
	public static final String R_TEMP_FOLDER = "Temp";
	
	// Utility script with custom functions for R
	public static final String R_UTILITY_SCRIPT = "Utility.R";
	public static final String R_MATCHING_SCRIPT = "matching.R";
	
	// Function name to calculate locality sensitive hashing
	public static final String R_LSH_MATCHING_FUN = "run_lsh_matching";
	
	// Composite key constants
	public static final String COMPOSITE_KEY_SEPARATOR = ":";
	// TODO once local master is refactored, remove references to these constants
	public static final String COMPOSITE_KEY_TYPE = "COMPOSITE";
	public static final String META_KEY = "URI:KEY";
	
	public static final String DATABASE_FOLDER = "db";
	public static final String SEMOSS_EXTENSION = ".smss";
	
	// Concept Metadata Table
	public static final String CONCEPT_METADATA_TABLE = "CONCEPTMETADATA";
	public static final String KEY = "KEY";	
	public static final String LOCAL_CONCEPT_ID = "LOCALCONCEPTID";
	public static final String DESCRIPTION = "description";
	public static final String TAG = "tag";
	public static final String LINK = "link";
	
	// Metamodel keys
	public static final String NODE_PROP = "nodeProp";
	public static final String RELATION_PROP = "relationProp";
	public static final String RELATION = "relation";
	public static final String FROM_TABLE = "fromTable";
	public static final String TO_TABLE = "toTable";
	public static final String REL_NAME = "relName";
	public static final String DATA_TYPES = "dataTypes";
	public static final String START_ROW = "startRow";
	public static final String END_ROW = "endRow";
}
