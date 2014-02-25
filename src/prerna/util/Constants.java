/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.util;


/**
 * This class contains all of the constants referenced elsewhere in the code.
 */
public class Constants {
	
	public static final String SUBJECT = "SUBJECT";
	public static final String OBJECT = "OBJECT";
	public static final String TRANSPARENT = "TRANSPARENT";
	public static final String ENTER_TEXT = "Enter your search text here";
	public static final String ENTER_SEARCH_DISABLED_TEXT = "Search is disabled for faster processing";
	public static String DB_NAME_FIELD = "dbSelectorField";
	public static String IMPORT_FILE_FIELD = "importFileNameField";
	public static String BASE_URI_TEXT_FIELD = "customBaseURItextField";
	public static String IMPORT_COMBOBOX = "dbImportTypeComboBox";
	public static String IMPORT_TYPE_COMBOBOX = "loadingFormatComboBox";
	public static String IMPORT_CSV_IMPORT_LBL = "csvPropLbl";
	public static String IMPORT_CSV_FILE_BUTTON = "csvPropBrowseBtn";
	public static String IMPORT_CSV_FILE_FIELD = "csvPropFilenameField";
	public static String IMPORT_TYPE_LABEL = "lblDataInputFormat";
	public static String IMPORT_PANEL = "dbImportPanel";
	public static String IMPORT_ENTERDB_LABEL = "dbNameLbl";
	public static String IMPORT_FILE_LABEL = "selectionFileLbl";
	public static String IMPORT_BUTTON_BROWSE = "fileBrowseBtn";
	public static String IMPORT_BUTTON = "importButton";
	public static String MAP_BROWSE_BUTTON = "mapBrowseBtn";
	public static String DB_PROP_BROWSE_BUTTON = "dbPropBrowseButton";
	public static String QUESTION_BROWSE_BUTTON = "questionBrowseButton";
	public static String MAP_TEXT_FIELD = "importMapFileNameField";
	public static String DB_PROP_TEXT_FIELD = "dbPropFileNameField";
	public static String QUESTION_TEXT_FIELD = "questionFileNameField";
	public static String ADVANCED_IMPORT_OPTIONS_PANEL = "advancedImportOptionsPanel";
	public static String ADVANCED_IMPORT_OPTIONS_BUTTON = "btnShowAdvancedImportFeatures";
	public static String IMPORT_RDBMS_URL_LABEL = "lblDBImportURL";
	public static String IMPORT_RDBMS_URL_FIELD = "dbImportURLField";
	public static String IMPORT_RDBMS_DRIVER_LABEL = "lblDBImportDriverType";
	public static String IMPORT_RDBMS_DRIVER_COMBOBOX = "dbImportRDBMSDriverComboBox";
	public static String IMPORT_RDBMS_USERNAME_LABEL = "lblDBImportUsername";
	public static String IMPORT_RDBMS_USERNAME_FIELD = "dbImportUsernameField";
	public static String IMPORT_RDBMS_PW_LABEL = "lblDBImportPW";
	public static String IMPORT_RDBMS_PW_FIELD = "dbImportPWField";
	public static String TEST_RDBMS_CONNECTION = "btnTestRDBMSConnection";
	public static String IMPORT_MAP_LABEL = "lblselectCustomMap";
	public static String DBCM_Prop = "DBCM_Prop";
	public static String EMPTY = "@@";
	public static String LAYOUT = "LAYOUT";
	public static String VERTEX_NAME = "VERTEX_LABEL_PROPERTY";
	public static String VERTEX_TYPE = "VERTEX_TYPE_PROPERTY";
	public static String VERTEX_COLOR = "VERTEX_COLOR_PROPERTY";
	public static String GENERIC_IMAGE = "GENERIC";
	public static String PERSPECTIVE = "PERSPECTIVE";
	public static final String INEDGE_COUNT = "Inputs";
	public static final String OUTEDGE_COUNT = "Outputs";
	public static String PROCESS_CURRENT_DATE = "PROCESS_CURRENT_DATE";
	public static String PROCESS_CURRENT_USER = "PROCESS_CURRENT_USER";
	public static String CURRENT_PLAYSHEET = "layoutValue";
	
	//graphplaysheet option constants
	// layouts
	public static String GPSSudowl="GPS_SUDOWL_DEFAULT";
	public static String GPSSearch = "GPS_SEARCH_DEFAULT";
	public static String GPSProp = "GPS_PROPERTIES_DEFAULT";
	public static String highQualityExport = "GPSHighQualityExport";
	public static String sudowlCheck="sudowlCheck";
	public static String searchCheck = "searchCheck";
	public static String propertyCheck = "propertyCheck";
	public static String highQualityExportCheck = "highQualityExportCheck";
	
	
	//Used by POIReader
	public static String RELATION_URI_CONCATENATOR = ":"; //used in between the in node and out node for relation instance uris.
	public static String DEFAULT_NODE_CLASS = "Concept";
	public static String DEFAULT_RELATION_CLASS = "Relation";
	public static String SUBPROPERTY_URI = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
	public static String SUBCLASS_URI = "http://www.w3.org/2000/01/rdf-schema#subClassOf";	
	public static String CLASS_URI = "http://www.w3.org/2000/01/rdf-schema#Class";
	public static String DEFAULT_PROPERTY_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	public static String SEMOSS_URI = "SEMOSS_URI";
	public static String CLASS = "_CLASS";
	public static String TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	
	// layouts
	public static String FR="Fruchterman-Reingold";
	public static String KK = "Kamada-Kawai";
	public static String SPRING = "Spring-Layout";
	public static String SPRING2 = "Spring-Layout2";
	public static String CIRCLE_LAYOUT = "Circle-Layout";
	public static String ISO = "ISO-Layout";
	public static String TREE_LAYOUT = "Tree-Layout";
	public static String RADIAL_TREE_LAYOUT = "Radial-Tree-Layout";
	public static String BALLOON_LAYOUT = "Balloon Layout";
		
	public static String LEGEND = "_LEGEND";
	
	public static String DESCR = "DESCRIPTION";
	public static String QUERY = "QUERY";
	public static String CUSTOMIZE_SPARQL = "btnCustomSparql";
	public static String GET_CURRENT_SPARQL = "btnGetQuestionSparql";
	public static String SHOW_HINT = "btnShowHint";
	public static String SHOW_PLAYSHEETS_LIST = "btnShowPlaySheets";
	public static String ENGINE = "ENGINE";
	public static String ENGINES = "ENGINES";
	public static String ENGINE_PROPERTIES_FILE = "ENGINE_PROP";
	public static String ENGINE_TYPE = "ENGINE_TYPE";
	public static String LISTENERS="LISTENERS";
	public static String MODEL = "MODEL";
	public static String CONTROL = "CONTROLLER";
	public static String VIEW = "VIEW";
	public static String PARENT_VIEW = "PARENT_VIEW";
	public static String RIGHT_VIEW = "RIGHT_VIEW";
	public static String SPARQL_AREA_FIELD = "sparqlArea";
	public static String PARAM_PANEL_FIELD = "paramPanel";
	public static String QUESTION_LIST_FIELD = "questionSelector";
	public static String RIGHT_VIEW_FIELD = "rightView";
	public static String MAIN_FRAME = "playPane";
	public static String DESKTOP_PANE = "desktopPane";
	
	//colors
	public static String BROWN = "BROWN";
	public static String RED = "RED";
	public static String GREEN = "GREEN";
	public static String BLUE = "BLUE";
	public static String ORANGE = "ORANGE";
	public static String YELLOW = "YELLOW";
	public static String PURPLE = "PURPLE";
	public static String AQUA = "AQUA";
	public static String MAGENTA = "MAGENTA";
	
	//shapes
	public static String SQUARE ="SQUARE";
	public static String TRIANGLE = "TRIANGLE";
	public static String DIAMOND = "DIAMOND";
	public static String STAR = "STAR";
	public static String CIRCLE = "CIRCLE";
	public static String HEXAGON = "HEXAGON";
	public static String PENTAGON = "PENTAGON";
	
	public static String TYPE_QUERY = "TYPE_QUERY";
	public static String TCCALC_PROGRESS_BAR = "calcTCprogressBar";
	public static String TMCALC_PROGRESS_BAR = "calcTMprogressBar";
	public static String ENTITY = "entity";
	public static String REPO_LIST = "repoList";
	public static String FILTER_TABLE = "filterTable";
	public static String EDGE_TABLE = "edgeTable";
	public static String PROP_TABLE = "propertyTable";
	public static String FILTER_PANEL = "filterPanel";
	public static String PLAYSHEETS = "playsheetList";
	public static String APPEND = "appendButton";
	public static String PROP_URI = "PROP_URI";
	public static String PREDICATE_URI = "PREDICATE_URI";
	public static String EDGE_NAME = "EDGE_NAME";
	public static String EDGE_TYPE = "EDGE_TYPE";
	public static String OPTION = "OPTION"; // used by entity filler
	public static String FILTER = "FILTER_NAME";
	public static String EDGE_ADJUSTER_TABLE = "edgeAdjusterTable";
	public static String LABEL_TABLE = "labelTable";
	public static String TOOLTIP_TABLE = "tooltipTable";
	public static String BUSINESS_VALUE = "System/Business_Value";
	public static String CAPABILITY_BUSINESS_VALUE = "Capability/Business_Value";
	public static String BUSINESS_PROCESS_BUSINESS_VALUE = "BusinessProcess/Business_Value";
	public static String CALC_MATRIX = "_Matrix";
	public static String CALC_MATRIX_EXT_STAB = "_Matrix_Ext_Stab";
	public static String CALC_MATRIX_TECH_STD = "_Matrix_Tech_Std";
	public static String CALC_PROPERTY_LABEL = "_PropLabels";
	public static String CALC_COLUMN_LABELS = "_ColLabels";
	public static String CALC_ROW_LABELS = "_RowLabels";
	public static String CALC_EXAMPLE_EDGE = "_ExEdge";
	public static String URI = "URI";
	public static String CALC_NAMES_LIST = "_Names";
	public static String CALC_NAMES_TECH_STD_LIST = "_Names_Tech_Std";
	public static String GRID_VIEW = "prerna.ui.components.playsheets.GridPlaySheet";
	public static String PROP_HASH = "_PropHash";
	public static String TRAVERSE_JENA_MODEL = "traverseJenaModel";
	public static String UNDO_BOOLEAN = "undoBoolean";
	public static String UNDOBTN = "undoBtn";
	public static String TECH_MATURITY = "System/Tech_Maturity";
	public static String TRANSITION_COSTS = "TRANSITION_COSTS";
	public static String TM_LIFECYCLE = "_Tech_Maturity_Lifecycle";
	public static String TM_CATEGORY = "_Tech_Maturity_Category";
	public static String TC_OVERHEAD_ARRAY = "_TC_Overhead_Array";
	public static String TC_CORE_MATRIX = "_TC_Core_Matrix";
	public static String TC_SDLC_CORE_MATRIX = "_TC_SDLC_Core_Matrix";
	public static String RDF_FILE_NAME = "RDF_FILE_NAME";
	public static String RDF_FILE_TYPE = "RDF_FILE_TYPE";
	public static String RDF_FILE_BASE_URI = "RDF_FILE_BASE_URI";
	public static String OBJECT_PROP_TABLE = "objectPropertiesTable";
	public static String DATA_PROP_TABLE = "dataPropertiesTable";
	public static String OBJECT_PROP_STRING = "objectPropertiesString";
	public static String DATA_PROP_STRING = "dataPropertiesString";
	public static String COLOR_SHAPE_TABLE = "colorShapeTable";
	public static String SIZE_TABLE = "sizeTable";
	public static String EXTEND_TABLE = "extendTable";
	public static String SUBMIT_BUTTON = "submitButton";
	public static String TRANS_ALL_FRAME = "transAllFrame";
	public static String TRANS_ALL_SYSTEM_LABEL = "sysNoLabel";
	public static String TRANS_ALL_DATA_LABEL = "dataNoLabel";
	public static String TRANS_ALL_ICD_LABEL = "icdNoLabel";
	public static String TRANS_ALL_SYSTEM_AREA = "transAllSysArea";
	public static String TRANS_ALL_DATA_AREA = "transAllDataArea";
	public static String TRANS_ALL_ICD_AREA = "transAllICDArea";
	public static String TRANS_ALL_WSPRO_AREA = "transAllWSPArea";
	public static String TRANS_ALL_WSCON_AREA = "transAllWSCArea";
	public static String PLAYSHEET_COMBOBOXLIST = "playSheetComboBox";
	public static String SPARQLLABEL = "sparqlLbl";
	public static String BLANK_URL = "http://bornhere.com/noparent/blank/";
	public static String PPT_TRAINING_BUTTON = "pptTrainingBtn";
	public static String HTML_TRAINING_BUTTON = "htmlTrainingBtn";

	//Traverse Freely Queries
	public static String NEIGHBORHOOD_TYPE_QUERY = "NEIGHBORHOOD_TYPE_QUERY";
	public static String NEIGHBORHOOD_TYPE_QUERY_JENA = "NEIGHBORHOOD_TYPE_QUERY_JENA";
	public static String NEIGHBORHOOD_OBJECT_QUERY = "NEIGHBORHOOD_OBJECT_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_FINDER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY";
	public static String TRAVERSE_FREELY_QUERY = "TRAVERSE_FREELY_QUERY";
	public static String TRAVERSE_FREELY_QUERY_JENA = "TRAVERSE_FREELY_QUERY_JENA";
	public static String TRAVERSE_INSTANCE_FREELY_QUERY = "TRAVERSE_INSTANCE_FREELY_QUERY";
	public static String SUBJECT_TYPE_QUERY = "SUBJECT_TYPE_QUERY";
	public static String PREDICATE = "PREDICATE";
	public static String IGNORE_URI = "IGNORE_URI";
	
	//TAP Cost Transition Queries 
	public static String TRANSITION_COST_INSERT_WITH_OVERHEAD="TRANSITION_COST_INSERT_WITH_OVERHEAD";
	public static String TRANSITION_COST_INSERT_WITHOUT_OVERHEAD="TRANSITION_COST_INSERT_WITHOUT_OVERHEAD";
	public static String TRANSITION_COST_DELETE="TRANSITION_COST_DELETE";
	public static String TRANSITION_COST_INSERT_SITEGLITEM="TRANSITION_COST_INSERT_SITEGLITEM";
	public static String TRANSITION_QUERY_SEPARATOR="&";
	public static String TRANSITION_COST_INSERT_SUSTAINMENT="TRANSITION_COST_INSERT_SUSTAINMENT";
	public static String TRANSITION_COST_INSERT_TRAINING="TRANSITION_COST_INSERT_TRAINING";
	public static String TRANSITION_COST_INSERT_SEMANTICS="TRANSITION_COST_INSERT_SEMANTICS";
	public static String TRANSITION_DATA_FEDERATION_PHASE_INDEPENDENT="TRANSITION_DATA_FEDERATION_PHASE_INDEPENDENT";
	public static String TRANSITION_REPORT_COMBO_BOX="transCostReportSystemcomboBox";
	public static String TRANSITION_REPORT_TYPE_COMBO_BOX="TransReportTypecomboBox";
	public static String TRANSITION_REPORT_FORMAT_COMBO_BOX="TransReportFormatcomboBox";
	public static String TRANSITION_APPLY_OVERHEAD_RADIO ="rdbtnApplyTapOverhead";
	public static String TRANSITION_NOT_APPLY_OVERHEAD_RADIO = "rdbtnDoNotApplyOverhead";
	public static String TRANSITION_SERVICE_PANEL = "transitionServicePanel";
	public static String SOURCE_SELECT_PANEL = "sourceSelectPanel";
	public static String TRANSITION_CHECK_BOX_DATA_FED="chckbxDataFederationTransReport";
	public static String TRANSITION_CHECK_BOX_DATA_CONSUMER="chckbxDataConsumer";
	public static String TRANSITION_ITEM_GEN_BUTTON = "loadGenBtn";
	public static String TRANSITION_CHECK_BOX_BLU_PROVIDER="chckbxBLUprovider";
	public static String TRANSITION_CHECK_BOX_DATA_GENERIC="chckbxDataEsbImplementation";
	public static String TRANSITION_CHECK_BOX_BLU_GENERIC = "chckbxBluEsbImplementation";
	public static String TRANSITION_SYSTEM_DROP_DOWN_PANEL = "transReportSysDropDownPanel";
	public static String TRANSITION_GENERIC_BLU = "TRANSITION_GENERIC_BLU";
	public static String TRANSITION_GENERIC_DATA = "TRANSITION_GENERIC_DATA";
	public static String TRANSITION_SPECIFIC_DATA_CONSUMER = "TRANSITION_SPECIFIC_DATA_CONSUMER";
	public static String TRANSITION_DATA_FEDERATION = "TRANSITION_DATA_FEDERATION";
	public static String TRANSITION_BLU_PROVIDER = "TRANSITION_BLU_PROVIDER";
	public static String TRANSITION_SPECIFIC_SITE_CONSUMER = "TRANSITION_SPECIFIC_SITE_CONSUMER";
	public static String ADVANCED_TRANSITION_FUNCTIONS_PANEL = "advancedFunctionsPanel";
	public static String SERVICE_SELECTION_BUTTON = "serviceSelectionBtn";
	public static String SERVICE_SELECTION_PANE = "serviceSelectScrollPane";
	public static String SOURCE_SELECTION_PANE = "sourceSelectScrollPane";
	public static String TIER1_CHECKBOX = "tierCheck1";
	public static String TIER2_CHECKBOX = "tierCheck2";
	public static String TIER3_CHECKBOX = "tierCheck3";
	
	//Fact Sheet Report Queries
	public static String FACT_SHEET_REPORT_TYPE_COMBO_BOX = "FactSheetReportTypecomboBox";
	public static String FACT_SHEET_SYSTEM_DROP_DOWN_PANEL = "factSheetReportSysDropDownPanel";
	public static String FACT_SHEET_SYSTEM_SELECT_COMBO_BOX = "factSheetReportSyscomboBox";
	
	//Used by optimization organizer
	public static String TRANSITION_GENERIC_COSTS = "TRANSITION_GENERIC_COSTS";
	public static String TRANSITION_CONSUMER_COSTS = "TRANSITION_CONSUMER_COSTS";
	public static String TRANSITION_PROVIDER_COSTS = "TRANSITION_PROVIDER_COSTS";
	
	public static String UPDATE_SPARQL_AREA="customUpdateTextPane";
	public static String INSERT_SYS_SUSTAINMENT_BUDGET_BUTTON = "btnInsertBudgetProperty";
	public static String SYSTEM_SUSTAINMENT_BUDGET_INSERT_QUERY = "SYSTEM_SUSTAINMENT_BUDGET_INSERT_QUERY";
	
	//Distance Downstream
	public static String INSERT_DOWNSTREAM_BUTTON = "btnInsertDownstream";
	public static String DISTANCE_DOWNSTREAM_QUERY = "DISTANCE_DOWNSTREAM_QUERY";
	public static String SOA_ALPHA_VALUE_TEXT_BOX = "soaAlphaValueTextField";
	public static String APPRECIATION_TEXT_BOX = "appreciationValueTextField";
	public static String DEPRECIATION_TEXT_BOX = "depreciationValueTextField";
		
	//SOA Transition All
	public static String SOA_TRANSITION_ALL_DATA_QUERY="SOA_TRANSITION_ALL_DATA_QUERY";
	public static String SOA_TRANSITION_ALL_GENERIC_DATA_QUERY="SOA_TRANSITION_ALL_GENERIC_DATA_QUERY";
	public static String SOA_TRANSITION_ALL_GENERIC_BLU_QUERY="SOA_TRANSITION_ALL_GENERIC_BLU_QUERY";
	public static String SOA_TRANSITION_ALL_BLU_QUERY ="SOA_TRANSITION_ALL_BLU_QUERY";
	
	public static String DREAMER = "DREAMER";
	public static String ONTOLOGY = "ONTOLOGY";
	
	public static String PERSPECTIVE_SELECTOR = "perspectiveSelector";
	
	public static String BROWSER_TYPE = "BROWSER_TYPE";
	
	public static final String SPARQL_QUERY_ENDPOINT = "SPARQL_QUERY_ENDPOINT";
	public static final String SPARQL_UPDATE_ENDPOINT = "SPARQL_UPDATE_ENDPOINT";
	
	public static String DATA_LATENCY_WEEKS_TEXT = "dataLatencyMonthsTextField";
	public static String DATA_LATENCY_DAYS_TEXT = "dataLatencyDaysTextField";
	public static String DATA_LATENCY_HOURS_TEXT = "dataLatencyHoursTextField";
	
	public static final String HTML = "HTML";
	public static final String PROPERTY = "PROPERTY";
	
	//Load Sheet Export Panel
	public static final String EXPORT_LOAD_SHEET_SOURCE_COMBOBOX = "exportDataSourceComboBox";
	public static final String EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX = "subjectNodeTypeComboBox";
	public static final String EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX = "objectNodeTypeComboBox";
	public static final String EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX = "nodeRelationshipComboBox";
	public static final String EXPORT_LOAD_SHEET_MAX_LIMIT_MESSAGE = "lblMaxExportLimit";
	public static final String EXPORT_LOAD_SHEET_CLEAR_ALL_BUTTON = "btnClearAll";
	public static final String EXPORT_LOAD_SHEET_ADD_EXPORT_BUTTON = "btnAddExport";
	public static final int MAX_EXPORTS = 9;
	
	//Update Cost DB Panel
	public static final String CHANGED_DB_COMBOBOX = "changedDBComboBox";
	public static final String COST_DB_COMBOBOX = "costDBComboBox";
	
	public static final String COST_DB_BASE_URI_FIELD = "costDBBaseURIField";
	public static final String GLITEM_LOADING_SHEET = "LoadingSheets1.xlsx";
	public static final String WATCHERS = "WATCHERS";
	public static final String ENGINE_WATCHER = "ENGINE_WATCHER";
	
	public static final String GLITEM_CORE_LOADING_SHEET = "LoadingSheets1.xlsx";

	public static final String GLITEM_SITE_LOADING_SHEET = "Site_HWSW_GLItems.xlsx";
	public static final String OWL = "OWL";
	public static final String URL_PARAM = "URL_PARAM";
	public static final String PROPS = "PROPS";
	//public static final String TYPE_URI = "TYPE_URI";
	public static final String INSIGHT = "INSIGHT"; 
	public static final String ID = "ID";
	public static final String LABEL = "LABEL";
	public static final String OUTPUT = "OUTPUT";
	public static final String TYPE = "TYPE";
	public static final String SPARQL = "SPARQL";
	public static final String TAG = "TAG";
	
	public static final String ENGINE_WEB_WATCHER = "ENGINE_WEB_WATCHER";
	public static final String DEPEND = "DEPEND";


}
