//package prerna.engine.impl;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.StringTokenizer;
//import java.util.Vector;
//
//import org.apache.log4j.LogManager;
//import org.h2.store.fs.FileUtils;
//
//import prerna.engine.api.IEngine;
//import prerna.engine.api.ISelectStatement;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.rdbms.RDBMSNativeEngine;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.om.SEMOSSParam;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.PlaySheetRDFMapBasedEnum;
//import prerna.util.Utility;
//import prerna.util.sql.H2QueryUtil;
//
//public class InsightsConverter {
//
//	private static final org.apache.log4j.Logger LOGGER = LogManager.getLogger(InsightsConverter.class.getName());
//
//	private static final String QUESITON_QUERY = "SELECT DISTINCT "
//			+ "?INSIGHT_KEY ?INSIGHT_NAME ?PERSPECTIVE ?QUERY ?LAYOUT ?ORDER WHERE { "
//			+ "{?INSIGHT_KEY a <http://semoss.org/ontologies/Concept/Insight>} "
//			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/SPARQL> ?QUERY} "
//			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Layout> ?LAYOUT} "
//			+ "{?PERSPECTIVE <http://semoss.org/ontologies/Relation/Perspective:Insight> ?INSIGHT_KEY} "
//			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Label> ?INSIGHT_NAME} "
//			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Order> ?ORDER} "
//			+ "} ORDER BY ?INSIGHT_KEY";
//
//	private String smssLocation;
//	private String engineName;
//	private IEngine engine;
//	private RDFFileSesameEngine insightBaseXML;
//	private QuestionAdministrator questionAdmin;
//	
//	public void setEngine(IEngine coreEngine){
//		this.engineName = coreEngine.getEngineName();
//		this.engine = coreEngine;
//		this.questionAdmin = new QuestionAdministrator(coreEngine);
//	}
//
//	public static void main(String[] args) throws Exception {
//		Properties prop = new Properties();
//		prop.setProperty("BaseFolder", "C:/workspace/Semoss/db");
//		DIHelper.getInstance().setCoreProp(prop);
//		String engineName = "Movie_DB";
//		String smssLocation = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
//		BigDataEngine bd = new BigDataEngine();
//		bd.open(smssLocation); // TODO: does this work?
//		InsightsConverter test = new InsightsConverter();
//		//set paths
//		String xmlFilePath = "C:\\workspace\\Semoss\\db\\Movie_DB\\Movie_DB_Questions.XML";
////		test.createXMLEngine(xmlFilePath);
//		String baseFolder = "C:/workspace/Semoss/db";
////		test.setBaseFolder(baseFolder);
//		test.setEngineName(engineName);
//		test.setSMSSLocation(smssLocation);
//
//		//create rdbms engine
////		test.generateNewInsightsRDBMS();
//		//query xml file
//		test.loadQuestionsFromXML(xmlFilePath);
////		test.processParameterTableFromQuery();
//		//update smss location
//		test.updateSMSSFile();
//	}
//
//	public RDBMSNativeEngine generateNewInsightsRDBMS(String engineN) {
//		String dbProp = writePropFile(engineN);
//		RDBMSNativeEngine insightRDBMSEngine = new RDBMSNativeEngine();
//		insightRDBMSEngine.open(dbProp);
//		generateTables(insightRDBMSEngine);
//		
//		FileUtils.delete(dbProp);
//		return insightRDBMSEngine;
//	}
//
//	// TODO: do we really need this?
//	public String writePropFile(String engineName) {
//		H2QueryUtil queryUtil = new H2QueryUtil();
//		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
//		Properties prop = new Properties();
//		String connectionURL = "jdbc:h2:" + baseFolder + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + 
//				"insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
//		prop.put(Constants.CONNECTION_URL, connectionURL);
//		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
//		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
//		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
//		prop.put(Constants.TEMP_CONNECTION_URL, queryUtil.getTempConnectionURL());
//		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
//		prop.put("TEMP", "TRUE");
//
//		// write this to a file
//		String tempFile = baseFolder + "/db/" + engineName + "/conn.prop";
//
//		File file = null;
//		FileOutputStream fo = null;
//		try {
//			file = new File(tempFile);
//			fo = new FileOutputStream(file);
//			prop.store(fo, "Temporary Properties file for the RDBMS");
//		} catch (FileNotFoundException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(fo != null) {
//				try {
//					fo.close();
//				} catch (IOException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		return tempFile;
//	}
//
//
//	private void generateTables(RDBMSNativeEngine eng) {
//		// CREATE TABLE QUESTION_ID (ID INT, QUESTION_NAME VARCHAR(255), QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225), QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500))
//		String questionTableCreate = "CREATE TABLE QUESTION_ID ("
//				+ "ID INT, "
//				+ "QUESTION_NAME VARCHAR(255), "
//				+ "QUESTION_PERSPECTIVE VARCHAR(225), "
//				+ "QUESTION_LAYOUT VARCHAR(225), "
//				+ "QUESTION_ORDER INT, "
//				+ "QUESTION_DATA_MAKER VARCHAR(225), "
//				+ "QUESTION_MAKEUP CLOB, "
//				+ "QUESTION_PROPERTIES CLOB, "
//				+ "QUESTION_OWL CLOB, "
//				+ "QUESTION_IS_DB_QUERY BOOLEAN, "
//				+ "DATA_TABLE_ALIGN VARCHAR(500))";
//
//		eng.insertData(questionTableCreate);
//
//		// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)
//		String parameterTableCreate = "CREATE TABLE PARAMETER_ID ("
//				+ "PARAMETER_ID VARCHAR(255), "
//				+ "PARAMETER_LABEL VARCHAR(255), "
//				+ "PARAMETER_TYPE VARCHAR(225), "
//				+ "PARAMETER_DEPENDENCY VARCHAR(225), "
//				+ "PARAMETER_QUERY VARCHAR(2000), "
//				+ "PARAMETER_OPTIONS VARCHAR(2000), "
//				+ "PARAMETER_IS_DB_QUERY BOOLEAN, "
//				+ "PARAMETER_MULTI_SELECT BOOLEAN, "
//				+ "PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), "
//				+ "PARAMETER_VIEW_TYPE VARCHAR(255), "
//				+ "QUESTION_ID_FK INT)";
//
//		eng.insertData(parameterTableCreate);
//		
//		String feTableCreate = "CREATE TABLE UI ("
//				+ "QUESTION_ID_FK INT, "
//				+ "UI_DATA CLOB)";
//		
//		eng.insertData(feTableCreate);
//	}
//
//	public void loadQuestionsFromXML(String xmlFileLocation) {
//		createXMLEngine(xmlFileLocation);
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightBaseXML, QUESITON_QUERY);
//		String[] names = wrapper.getVariables();
////		int counter = 1;
//		List<String> allSheets = PlaySheetRDFMapBasedEnum.getAllSheetClasses();
//		while(wrapper.hasNext()) {
//			ISelectStatement ss = wrapper.next();
////			String INSIGHT_KEY = ss.getVar(names[0]) + "";
//			String INSIGHT_NAME = ss.getVar(names[1]) + "";
//			String PERSPECTIVE = ss.getVar(names[2]).toString().split(":")[1];
//			String QUERY = ss.getVar(names[3]) + "";
//			String LAYOUT = ss.getVar(names[4]) + "";
//			String ORDER = ss.getVar(names[5]) + "";
//
//			String dataMaker = getDataMaker(LAYOUT, allSheets);
//			LAYOUT = getPSname(LAYOUT);
//
//			//NEED TO CLEAN UP THE SINGLE QUOTES
//			List<SEMOSSParam> parameters = getXMLParameters(INSIGHT_NAME);
//
////			String NEW_INSIGHT_KEY = engineName + "_" + counter;
//			List<DataMakerComponent> comps = generateQueryComponents(this.engine, QUERY);
//			//need to add parameters as preTransformationFilters
////			appendParamsAsTransformations(comps, parameters); // THIS IS NOW TAKEN CARE OF BY QUESTION ADMINISTRATOR (because of the addViaText additions)
//			
//			this.questionAdmin.addQuestion(INSIGHT_NAME, PERSPECTIVE, comps, LAYOUT, ORDER, dataMaker, true, null, parameters, null);
//		}
//	}
//	
////	private void appendParamsAsTransformations(List<DataMakerComponent> comps, List<SEMOSSParam> parameters) {
////		if(parameters != null && !parameters.isEmpty()) {
////			// assume only one component
////			DataMakerComponent dmc = comps.get(0);
////			List<ISEMOSSTransformation> preTransList = new ArrayList<ISEMOSSTransformation>();
////			for(int i = 0; i < parameters.size(); i++) {
////				SEMOSSParam p = parameters.get(i);
////				ISEMOSSTransformation preTrans = new FilterTransformation();
////				Map<String, Object> props = new HashMap<String, Object>();
////				props.put(FilterTransformation.COLUMN_HEADER_KEY, p.getName());
////				preTrans.setProperties(props);
////				preTransList.add(preTrans);
////				p.setComponentFilterId(Insight.COMP + "0:" + Insight.PRE_TRANS + i);
////			}
////			dmc.setPreTrans(preTransList);
////		}
////	}
//
//	private String getPSname(String layout){
//		String psName = getLayoutFromPlaySheet(layout);
//		return psName;
//	}
//	
//	public static String getDataMaker(String layout, List<String> allSheets){
//		String dataMaker = "";
//		if((allSheets.contains(layout) || layout.equals("prerna.ui.components.specific.tap.InterfaceGraphPlaySheet") || layout.equals("prerna.ui.components.playsheets.GridScatterSheet") )
//				&& !layout.equals("prerna.ui.components.specific.ousd.RoadmapTimelineComboChartPlaySheet")
//				&& !layout.equals("prerna.ui.components.specific.tap.SysSimHeatMapSheet")) {
////			if(layout.equals("prerna.ui.components.playsheets.GraphPlaySheet") || layout.equals("prerna.ui.components.specific.tap.InterfaceGraphPlaySheet") || layout.equals("Graph")) {
//				dataMaker = "TinkerFrame";
////			} else if(!layout.equals("prerna.ui.components.playsheets.DualEngineGenericPlaySheet")) {
////				dataMaker = "BTreeDataFrame";
////			}
//		} else if (layout.equals("prerna.ui.components.specific.ousd.RoadmapTimelineComboChartPlaySheet")
//				|| layout.equals("prerna.ui.components.specific.tap.SysSimHeatMapSheet")
//				|| layout.equals("prerna.ui.components.specific.tap.PeoEisSysSimHeatMapSheet")
//				|| layout.equals("prerna.ui.components.specific.ousd.EnduringSysSimHeatMapSheet")
//				|| layout.equals("prerna.ui.components.specific.ousd.OUSDSysSimHeatMapSheet")
//				|| layout.equals("GraphDataModel")){
//			dataMaker = layout;
//		}
//		else {
//			dataMaker = "";
//		}
//		return dataMaker;
//	}
//	
//	private List<DataMakerComponent> generateQueryComponents(IEngine coreEngine, String query) {
//		List<DataMakerComponent> retList = new ArrayList<DataMakerComponent>();
//		DataMakerComponent comp = new DataMakerComponent(coreEngine, query);
//		retList.add(comp);
//		
//		return retList;
//	}
//
//	public void updateSMSSFile() {
//		FileOutputStream fileOut = null;
//		File file = new File(smssLocation);
//		List<String> content = new ArrayList<String>();
//
//		BufferedReader reader = null;
//		FileReader fr = null;
//		String locInFile = "ENGINE_TYPE";
//		try{
//			fr = new FileReader(file);
//			reader = new BufferedReader(fr);
//			String line;
//			while((line = reader.readLine()) != null){
//				content.add(line);
//			}
//
//			fileOut = new FileOutputStream(file);
//			for(int i=0; i<content.size(); i++){
//				byte[] contentInBytes = content.get(i).getBytes();
//				fileOut.write(contentInBytes);
//				fileOut.write("\n".getBytes());
//
//				if(content.get(i).contains(locInFile)){
//					String newProp = Constants.RDBMS_INSIGHTS + "\t" + "db/" + engineName + "/insights_database";
//					fileOut.write(newProp.getBytes());
//					fileOut.write("\n".getBytes());
//				}
//			}
//
//		} catch(IOException e){
//			classLogger.error(Constants.STACKTRACE, e);
//		} finally{
//			try{
//				reader.close();
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//
//			try{
//				fileOut.close();
//			} catch (IOException e){
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
//	}
//
//	private void createXMLEngine(String xmlPath) {
//		this.insightBaseXML = new RDFFileSesameEngine();
//		insightBaseXML.setFileName(xmlPath);
//		insightBaseXML.open(null);
//	}
//
//	public void setEngineName(String engineName) {
//		this.engineName = engineName;
//	}
//
//	public void setSMSSLocation(String smssLocation) {
//		this.smssLocation = smssLocation;
//	}
//
//	//from old abstract engine
//	//how to get params from xml
//	private Vector<SEMOSSParam> getXMLParameters(String label)
//	{
//		Vector <SEMOSSParam> retParam = new Vector<SEMOSSParam>();
//
//		String paramPred = "INSIGHT:PARAM";
//		String paramPredLabel = "PARAM:LABEL";
//		String queryPred = "PARAM:QUERY";
//		String hasDependPred = "PARAM:HAS:DEPEND";
//		String dependPred = "PARAM:DEPEND";
//		String typePred = "PARAM:TYPE";
//		String optionPred = "PARAM:OPTION";
//
//		String queryParamSparql = "SELECT DISTINCT ?paramLabel ?query ?option ?depend ?dependVar ?paramType ?param WHERE {"
//			+ "{?insightURI <" + "http://semoss.org/ontologies/Relation/Contains/Label" + "> ?insight}"
//			+ "{?insightURI <" + paramPred + "> ?param } "
//			+ "{?param <" + paramPredLabel + "> ?paramLabel } "
//			+ "{?param <" + typePred + "> ?paramType } "
//			+ "OPTIONAL {?param <" + queryPred + "> ?query } "
//			+ "OPTIONAL {?param <" + optionPred + "> ?option } "
//			+ "{?param <" + hasDependPred + "> ?depend } " 
//			+ "{?param <" + dependPred + "> ?dependVar } "
//			+ "} BINDINGS ?insight {(\""+label+"\")}";
//		
//		retParam = addSEMOSSParams(retParam,queryParamSparql);
//		
//		return retParam;
//	}
//
//	//from old abstract engine
//	//how to get params from xml
//	private Vector<SEMOSSParam> addSEMOSSParams(Vector<SEMOSSParam> retParam,String paramSparql) {
//		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightBaseXML, paramSparql);
//		wrap.execute();
//		// want only unique params. if a question name is duplicated, its possible to have multiple returned here
//		// really should switch this be insight ID based
//		Vector<String> usedLabels = new Vector<String>();
//			while(wrap.hasNext())
//			{
//				ISelectStatement bs = wrap.next();
//				String label = bs.getRawVar("paramLabel") + "";
//				if(!usedLabels.contains(label)){
//					usedLabels.add(label);
//					SEMOSSParam param = new SEMOSSParam();
//					param.setName(label);
//					
//					if(bs.getRawVar("paramType") != null)
//						param.setType(bs.getRawVar("paramType") +"");
//					if(bs.getRawVar("option") != null)
//						param.setOptions(bs.getRawVar("option") + "");
//					if(bs.getRawVar("query") != null)
//						param.setQuery(bs.getRawVar("query") + "");
//					if(bs.getRawVar("depend") != null)
//						param.setDepends(bs.getRawVar("depend") +"");
//					if(bs.getRawVar("dependVar") != null && !(bs.getRawVar("dependVar")+"").equals("\"None\"")) // really? is this in the xml?
//						param.addDependVar(bs.getRawVar("dependVar") +"");
//					if(bs.getRawVar("param") != null)
//						param.setParamID(bs.getRawVar("param") +"");
//					
//					retParam.addElement(param);
//					LOGGER.debug(param.getName() + param.getQuery() + param.isDepends() + param.getType());
//				}
//				
//			}
//			
//		return retParam;
//	}
//	/**
//	 * Load the perspectives for a specific engine.
//	 * 
//	 * @param List
//	 *            of properties
//	 */
//	public void loadQuestionsFromPropFile(Properties dreamerProp) {
//		
//		// this should load the properties from the specified as opposed to
//		// loading from core prop
//		// lastly the localprop needs to set up so that it can be swapped
//		String perspectives = (String) dreamerProp
//				.get(Constants.PERSPECTIVE);
//		LOGGER.info("Perspectives " + perspectives);
//		StringTokenizer tokens = new StringTokenizer(perspectives, ";");
//		List<String> allSheets = PlaySheetRDFMapBasedEnum.getAllSheetClasses();
//		while (tokens.hasMoreTokens()) {
//			String perspective = tokens.nextToken();
//
//			String key = perspective;
//			String qsList = dreamerProp.getProperty(key); // get the ; delimited
//			if (qsList != null) {
//				int count = 1;
//				StringTokenizer qsTokens = new StringTokenizer(qsList, ";");
//				while (qsTokens.hasMoreElements()) {
//					Map<String, String> dependMap = new HashMap<String, String>();
//					Map<String, String> queryMap = new HashMap<String, String>();
//					Map<String, String> optionMap = new HashMap<String, String>();
//					// get the question
//					String qsKey = qsTokens.nextToken();
//					
//					String qsDescr = dreamerProp.getProperty(qsKey);
//					String layoutName = dreamerProp.getProperty(qsKey + "_"
//							+ Constants.LAYOUT);
//					
//					String qsOrder = count + "";
//					
//					//qsDescr = count + ". " + qsDescr;
//
//					String query = dreamerProp.getProperty(qsKey + "_"
//							+ Constants.QUERY);
//					
//					String isDbQueryString = dreamerProp.getProperty(qsKey + "_"
//							+ Constants.QUERY + "_" + Constants.OWL);
//					
//					boolean isDbQuery = true;
//					if(isDbQueryString != null && !isDbQueryString.isEmpty()) {
//						isDbQuery = Boolean.parseBoolean(isDbQueryString);
//					}
//					
//					String description = dreamerProp.getProperty(qsKey + "_"
//							+ Constants.DESCR);
//
//					Map<String, String> paramHash = Utility.getParams(query);
//
//					Iterator<String> paramKeyIt = paramHash.keySet().iterator();
//
//					/*if(qsKey.equals("SysP15")){
//						int x = 0;
//					}*/
//					// loops through to get all param dependencies, queries and options
//					while (paramKeyIt.hasNext()) {
//						String param = paramKeyIt.next();
//						String paramKey = param.substring(0, param.indexOf("-"));
//						String type = param.substring(param.indexOf("-") + 1);
//						
////						String qsParamKey = engineName + ":" + perspective + ":" + qsKey + ":" + paramKey;
//						
//						// see if the param key has a query associated with it
//						// usually it is of the form qsKey + _ + paramKey + _ + Query
//						String parameterQueryKey = qsKey + "_" + paramKey + "_" + Constants.QUERY;
//						if(dreamerProp.containsKey(parameterQueryKey))
//						{
//							String parameterQueryValue = dreamerProp.getProperty(parameterQueryKey);
//							queryMap.put(paramKey, parameterQueryValue);
//							
//							// now also see if this query should be run on the OWL or DB
//							String parameterQueryIsDbQueryKey = qsKey + "_" + paramKey + "_" + Constants.DB + "_" + Constants.QUERY;
//							String paramQueryIsDbQueryString = dreamerProp.getProperty(parameterQueryIsDbQueryKey);
//							if(paramQueryIsDbQueryString != null && !paramQueryIsDbQueryString.isEmpty()) {
//								queryMap.put(paramKey + "_" + Constants.OWL, paramQueryIsDbQueryString);
//							}
//						}
//						// see if there is dependency
//						// dependency is of the form qsKey + _ + paramKey + _ + Depend
//						String parameterDependKey = qsKey + "_" + paramKey + "_" + Constants.DEPEND;
//						if(dreamerProp.containsKey(parameterDependKey))
//						{
//							// record this
//							// qsKey_paramkey  - qsKey:Depends - result
//							String parameterDependValue = dreamerProp.getProperty(parameterDependKey);
//							parameterDependKey = paramKey + "_" + Constants.DEPEND; //TODO: really...? We change the key right here? Is this right?? (legacy code from Davy in abstract engine) Need to test
//							StringTokenizer depTokens = new StringTokenizer(parameterDependValue, ";");
//							
//							//one parameter may have multiple dependencies separated by ;
//							while(depTokens.hasMoreElements())
//							{
//								String depToken = depTokens.nextToken();
//								dependMap.put(paramKey, depToken);
//							}
//						}
//						//see if there is option
//						// dependency is of the form qsKey + _ + paramKey + _ + Depend
//						String parameterOptionKey = type + "_" + Constants.OPTION;
//						if(dreamerProp.containsKey(parameterOptionKey))
//						{
////							System.out.println("TRUE");
//							String parameterOptionValue = dreamerProp.getProperty(parameterOptionKey);
//							optionMap.put(paramKey, parameterOptionValue);
//						}
//					}
//					List<DataMakerComponent> comps = generateQueryComponents(this.engine, query);
//					String layout = getPSname(layoutName);
//					String dataMaker = getDataMaker(layoutName, allSheets);
//					List<SEMOSSParam> parameters = getPropFileParameters(paramHash, dependMap, queryMap, optionMap);
////					appendParamsAsTransformations(comps, parameters); // THIS IS NOW TAKEN CARE OF BY QUESTION ADMINISTRATOR (because of the addViaText additions)
//					questionAdmin.addQuestion(qsDescr, perspective, comps, layout, qsOrder, dataMaker, isDbQuery, null, parameters, null);
//					count++;
//				}
//				LOGGER.debug("Loaded Perspective " + key);
//			}				
//		}
//	}
//	
//	private List<SEMOSSParam> getPropFileParameters(Map<String, String> paramHash, Map<String, String> dependMap, Map<String, String> queryMap, Map<String, String> optionMap){
//		List<SEMOSSParam> retList = new ArrayList<SEMOSSParam>();
//
//		Iterator<String> paramKeyIt = paramHash.keySet().iterator();
//		// for each param, just need to set depend and/or query or option
//		// or if neither of those, set the type
//		while (paramKeyIt.hasNext()) {
//			String fullParam = paramKeyIt.next();
//			String paramKey = fullParam.substring(0, fullParam.indexOf("-"));
//			String type = fullParam.substring(fullParam.indexOf("-") + 1);
//			SEMOSSParam paramObj = new SEMOSSParam();
//			if(queryMap.containsKey(paramKey)){
//				paramObj.setQuery(queryMap.get(paramKey));
//				if(dependMap.containsKey(paramKey)){ // can only depend if it has a custom query
//					paramObj.setDepends("true");
//					paramObj.addDependVar(dependMap.get(paramKey));
//				}
//				if(queryMap.containsKey(paramKey + "_" + Constants.OWL)) {
//					paramObj.setDbQuery(Boolean.parseBoolean(queryMap.get(paramKey + "_" + Constants.OWL)));
//				}
//			}
//			else if(optionMap.containsKey(paramKey)){
//				paramObj.setOptions(optionMap.get(paramKey));
//			}
//			else {
//				paramObj.setType(type);
//			}
//			paramObj.setName(paramKey);
//			retList.add(paramObj);
//		}
//		
//		return retList;
//	}
//	
//	public String getLayoutFromPlaySheet(String playsheet){
//		String layoutID = PlaySheetRDFMapBasedEnum.getIdFromClass(playsheet);
//		if(layoutID.isEmpty()){
//			switch(playsheet){
//			case "GraphDataModel":
//				layoutID = "Graph";
//				break;
//			case "prerna.ui.components.playsheets.DatasetSimilairtyColumnChartPlaySheet":
//				layoutID = "Column";
//				break;
//			case "prerna.ui.components.playsheets.BinnedColumnChartPlaySheet":
//				layoutID = "Column";
//				break;
//			case "prerna.ui.components.playsheets.ComparisonColumnChartPlaySheet":
//				layoutID = "Column";
//				break;
//			case "prerna.ui.components.playsheets.WekaClassificationPlaySheet":
//				layoutID = "Dendrogram";
//				break;
//			case "prerna.ui.components.playsheets.GridRAWPlaySheet":
//				layoutID = "Grid";
//				break;
//			case "prerna.ui.components.specific.tap.CapabilityTaskPlaysheet":
//				layoutID = "HeatMap";
//				break;
//			case "prerna.ui.components.specific.tap.VendorCapabilityTaskPlaysheet":
//				layoutID = "HeatMap";
//				break;
//			case "prerna.ui.components.playsheets.BinnedPieChartPlaySheet":
//				layoutID = "Pie";
//				break;
////			case "prerna.ui.components.specific.tap.HealthGridSheet"://
////				layoutID = "Scatter";
////				break;
//			case "prerna.ui.components.playsheets.OutlierPlaySheet":
//				layoutID = "Grid";
//				break;
//			case "prerna.ui.components.playsheets.GridScatterSheet":
//				layoutID = "Scatter";
//				break;
//			case "prerna.ui.components.playsheets.NumericalCorrelationVizPlaySheet":
//				layoutID = "ScatterplotMatrix";
//				break;
//			case "prerna.ui.components.playsheets.OutlierVizPlaySheet":
//				layoutID = "SingleAxisCluster";
//				break;
//			case "prerna.ui.components.specific.ousd.EnduringSysSimHeatMapSheet":
//				layoutID = "SystemSimilarity";
//				break;
//			case "prerna.ui.components.specific.ousd.OUSDSysSimHeatMapSheet":
//				layoutID = "SystemSimilarity";
//				break;
//			case "prerna.ui.components.specific.tap.PeoEisSysSimHeatMapSheet":
//				layoutID = "SystemSimilarity";
//				break;
//			default:
//				layoutID = playsheet;
//			}
//		}
//		
//		return layoutID;
//	}
//
//}
