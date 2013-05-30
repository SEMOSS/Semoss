	package prerna.util;


public class Constants {
	
	public static final String DEFAULT_SHAPE = "DEFAULT_SHAPE";
	public static final String DEFAULT_COLOR = "DEFAULT_COLOR";
	public static final String SUBJECT = "SUBJECT";
	public static final String OBJECT = "OBJECT";
	public static final String NEIGHBORHOOD_PREDICATE_TYPE_ALT2_PAINTER_QUERY = "NEIGHBORHOOD_PREDICATE_TYPE_ALT2_PAINTER_QUERY";
	public static final String NEIGHBORHOOD_OBJECT_TYPE_ALT2_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_TYPE_ALT2_PAINTER_QUERY";
	public static final String NEIGHBORHOOD_OBJECT_INSTANCE_ALT2_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_INSTANCE_ALT2_PAINTER_QUERY";
	public static final String TRANSPARENT = "TRANSPARENT";
	public static final String ENTER_TEXT = "Enter your search text here";
	public static String DB_NAME_FIELD = "dbSelectorField";
	public static String IMPORT_FILE_FIELD = "importFileNameField";
	public static String BASE_URI_TEXT_FIELD = "customBaseURItextField";
	public static String IMPORT_COMBOBOX = "dbImportTypeComboBox";
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
	public static String IMPORT_MAP_LABEL = "lblselectCustomMap";
	public static String DBCM_Prop = "DBCM_Prop";
	public static String EMPTY = "@@";
	public static String LAYOUT = "LAYOUT";
	public static String VERTEX_NAME = "VERTEX_LABEL_PROPERTY";
	public static String VERTEX_TYPE = "VERTEX_TYPE_PROPERTY";
	public static String GENERIC_IMAGE = "GENERIC";
	public static String PERSPECTIVE = "PERSPECTIVE";
	public static String EXTEND_PERSPECTIVE = "EXTEND_PERSPECTIVE";
	public static final String INEDGE_COUNT = "Inputs";
	public static final String OUTEDGE_COUNT = "Outputs";
	public static String PROCESS_CURRENT_DATE = "PROCESS_CURRENT_DATE";
	public static String PROCESS_CURRENT_USER = "PROCESS_CURRENT_USER";

	//Used by POIReader
	public static String RELATION_URI_CONCATENATOR = ":"; //used in between the in node and out node for relation instance uris.
	public static String DEFAULT_NODE_CLASS = "Concept";
	public static String DEFAULT_RELATION_CLASS = "Relation";
	public static String SUBPROPERTY_URI = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
	public static String SUBCLASS_URI = "http://www.w3.org/2000/01/rdf-schema#subClassOf";	
	public static String CLASS_URI = "http://www.w3.org/2000/01/rdf-schema#Class";
	public static String DEFAULT_PROPERTY_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	
	// layouts
	public static String FR="Fruchterman-Reingold";
	public static String KK = "Kamada-Kawaii";
	public static String SPRING = "Spring-Layout";
	public static String SPRING2 = "Spring-Layout2";
	public static String CIRCLE_LAYOUT = "Circle-Layout";
	public static String ISO = "ISO-Layout";
	public static String TREE_LAYOUT = "Tree-Layout";
	public static String RADIAL_TREE_LAYOUT = "Radial-Tree-Layout";
		
	public static String LEGEND = "_LEGEND";
	
	public static String DESCR = "DESCRIPTION";
	public static String QUERY = "QUERY";
	public static String SPARQLBTN = "showSparql";
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
	public static String EXTEND = "extendButton";
	public static String PROP_URI = "PROP_URI";
	public static String PREDICATE_URI = "PREDICATE_URI";
	public static String EDGE_NAME = "EDGE_NAME";
	public static String EDGE_TYPE = "EDGE_TYPE";
	public static String OPTION = "OPTION"; // used by entity filler
	public static String FILTER = "FILTER_NAME";
	public static String WEIGHT = "weight";
	public static String EDGE_ADJUSTER_TABLE = "edgeAdjusterTable";
	public static String LABEL_TABLE = "labelTable";
	public static String TOOLTIP_TABLE = "tooltipTable";
	public static String BUSINESS_VALUE = "System/Business_Value";
	public static String CALC_MATRIX = "_Matrix";
	public static String CALC_PROPERTY_LABEL = "_PropLabels";
	public static String CALC_COLUMN_LABELS = "_ColLabels";
	public static String CALC_ROW_LABELS = "_RowLabels";
	public static String CALC_EXAMPLE_EDGE = "_ExEdge";
	public static String URI = "URI";
	public static String DBCM_DATA_NEIGHBORHOOD = "DBCM_DATA_NEIGHBORHOOD";
	public static String DBCM_ICD_NEIGHBORHOOD = "DBCM_ICD_NEIGHBORHOOD";
	public static String DBCM_SW_NEIGHBORHOOD = "DBCM_SW_NEIGHBORHOOD";
	public static String DBCM_HW_NEIGHBORHOOD = "DBCM_HW_NEIGHBORHOOD";
	public static String CALC_NAMES_LIST = "_Names";
	public static String GRID_VIEW = "prerna.ui.components.GridPlaySheet";
	public static String PROP_HASH = "_PropHash";
	public static String TRAVERSE_LEVEL = "traverseLevel";
	public static String TRAVERSE_VECTOR = "traverseVector";
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
	public static String RADIO_GRAPH = "rdbtnGraph";
	public static String RADIO_GRID = "rdbtnGrid";
	public static String RADIO_RAW = "rdbtnRaw";
	public static String SPARQL = "sparqlLbl";
	public static String BLANK_URL = "http://bornhere.com/noparent/blank/";
	public static String PPT_TRAINING_BUTTON = "pptTrainingBtn";
	public static String HTML_TRAINING_BUTTON = "htmlTrainingBtn";
	
	public static String NEIGHBORHOOD_PREDICATE_QUERY = "NEIGHBORHOOD_PREDICATE_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT_QUERY = "NEIGHBORHOOD_PREDICATE_ALT_QUERY";

	public static String NEIGHBORHOOD_PREDICATE_TYPE_PAINTER_QUERY = "NEIGHBORHOOD_PREDICATE_TYPE_PAINTER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_PREDICATE_INSTANCE_PAINTER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_PREDICATE_INSTANCE_ALT_PAINTER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT2_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_PREDICATE_INSTANCE_ALT2_PAINTER_QUERY";

	public static String NEIGHBORHOOD_OBJECT_QUERY = "NEIGHBORHOOD_OBJECT_QUERY";
	public static String NEIGHBORHOOD_OBJECT_ALT_QUERY = "NEIGHBORHOOD_OBJECT_ALT_QUERY";
	public static String NEIGHBORHOOD_OBJECT_ALT2_QUERY = "NEIGHBORHOOD_OBJECT_ALT2_QUERY";

	
	public static String NEIGHBORHOOD_OBJECT_TYPE_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_TYPE_PAINTER_QUERY";
	public static String NEIGHBORHOOD_OBJECT_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_INSTANCE_PAINTER_QUERY";
	public static String NEIGHBORHOOD_OBJECT_ALT_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_INSTANCE_ALT_PAINTER_QUERY";
	public static String NEIGHBORHOOD_OBJECT_ALT2_INSTANCE_PAINTER_QUERY = "NEIGHBORHOOD_OBJECT_INSTANCE_ALT2_PAINTER_QUERY";

	public static String NEIGHBORHOOD_PREDICATE_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_FINDER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY";
	public static String NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY = "NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY";

	public static String TRAVERSE_FREELY_QUERY = "TRAVERSE_FREELY_QUERY";

	public static String PREDICATE = "PREDICATE";
	public static String IGNORE_URI = "IGNORE_URI";
	public static String UPDATE_SPARQL_AREA="customUpdateTextPane";
	public static String DISTANCE_DOWNSTREAM_QUERY = "DISTANCE_DOWNSTREAM_QUERY";
	
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

}
