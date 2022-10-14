package prerna.query.parsers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.query.parsers.ParamStruct.FILL_TYPE;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.filters.IQueryFilter;

public class ParamStructToJsonGenerator {

	private static final int COLLECT_SIZE = 150;
	private static final String COLLECT_STRING = "Collect(" + COLLECT_SIZE + ")";
	
	/**
	 * Generate the final json object for the saved insight
	 * @param insightName
	 * @param paramRecipe
	 * @param params
	 * @return
	 */
	public static List<Map<String, Object>> generateInsightJsonForParameters(String insightName, String paramRecipe, List<ParamStruct> params) {
		List<Map<String, Object>> finalJsonObject = new Vector<>();
		Map<String, Object> jsonObject = new LinkedHashMap<>();
		jsonObject.put("label", insightName);
		jsonObject.put("description", "Please select paramters for the insight");
		// the pixel recipe is the query
		jsonObject.put("query", paramRecipe);
		// add param list
		jsonObject.put("params", generateJsonParameters(params));
		// add execute
		jsonObject.put("execute", "button");
		// while we allow for array of jsons - in this case we only have 1 for saved insights
		finalJsonObject.add(jsonObject);
		
		return finalJsonObject;
	}
	
	/**
	 * Generate the parameter section of the JSON for the list of params
	 * @param params
	 * @return
	 */
	public static List<Map<String, Object>> generateJsonParameters(List<ParamStruct> params) {
		List<Map<String, Object>> paramList = new Vector<>();

		int infiniteScrollCounter = 0;
		for(ParamStruct param : params) {
			// grab the display type first
			// since that determines some default values
			final String DISPLAY_TYPE = getModelDisplay(param);
			boolean requireSearch = ParamStruct.APPEND_SEARCH_PARAM_TYPES.contains(DISPLAY_TYPE);
			boolean useSelectedValues = ParamStruct.APPEND_USE_SELECTED_VALUES_TYPES.contains(DISPLAY_TYPE);
			boolean quickSelect = ParamStruct.APPEND_QUICK_SELECT_TYPES.contains(DISPLAY_TYPE);
			
			// need to create each param object
			Map<String, Object> paramMap = new LinkedHashMap<>();
			paramMap.put("paramName", param.getParamName());
			paramMap.put("required", Boolean.parseBoolean(param.isRequired() + ""));
			paramMap.put("useSelectedValues", useSelectedValues);
			
			// nested map for view
			Map<String, Object> paramViewMap = new LinkedHashMap<>();
			paramViewMap.put("label", param.getModelLabel());
			paramViewMap.put("displayType", DISPLAY_TYPE);
			// nested attributes map within nested view map
			Map<String, Boolean> paramViewAttrMap = new LinkedHashMap<>();
			// we do this based on the display type
			// and not the boolean since not exposed on UI
//			paramViewAttrMap.put("searchable", Boolean.parseBoolean(param.isSearchable() + ""));
			paramViewAttrMap.put("searchable", requireSearch);
			paramViewAttrMap.put("multiple", Boolean.parseBoolean(param.isMultiple() + ""));
			paramViewAttrMap.put("quickselect", quickSelect);
			paramViewMap.put("attributes", paramViewAttrMap);
			// add view
			paramMap.put("view", paramViewMap);
			
			// nested map for model
			Map<String, Object> modelMap = new LinkedHashMap<>();
			FILL_TYPE fillType = param.getFillType();
			if(fillType == FILL_TYPE.MANUAL) {
				modelMap.put("defaultOptions", param.getManualChoices());
			} else {
				// sending empty default options for FE
				modelMap.put("defaultOptions", new Object[0]);
				if(requireSearch) {
					// create search as well
					// and add the paramList
					String searchParamName = param.getParamName() + "_Search";
					Map<String, Object> paramSearchMap = new LinkedHashMap<>();
					paramSearchMap.put("paramName", searchParamName);
					paramSearchMap.put("view", false);
					Map<String, String> paramSearchModel = new LinkedHashMap<>();
					paramSearchModel.put("defaultValue", "");
					paramSearchMap.put("model", paramSearchModel);
					paramList.add(paramSearchMap);

					// store a variable for the infinite 
					String infiniteVar = "infinite"+infiniteScrollCounter++;
					// add to model map the parts that connect
					modelMap.put("infiniteQuery", infiniteVar + " | Collect(20)");
					modelMap.put("searchParam", searchParamName);
					modelMap.put("dependsOn", new String[]{searchParamName});
					
					// now we just need to define the query supporting this model
					String modelQuery = param.getModelQuery();
					if(ParamStruct.REQUIRE_MODEL_TYPES.contains(DISPLAY_TYPE)) {
						String paramQ = null;
						
						if( (modelQuery == null || modelQuery.isEmpty()) && param.getFillType() == FILL_TYPE.QUERY) {
							throw new IllegalArgumentException("Cannot save a param where the model for the parameter "
									+ "is not defined and the param is a query fill type");
						}
						
						if(modelQuery == null || modelQuery.isEmpty()) {
							// we will generate the model
							// we will use the information from the first details to generate this
							ParamStructDetails detailParam = param.getDetailsList().get(0);
							String sourceString = detailParam.getImportSource();
							String physicalQs = detailParam.getTableName() + "__" + detailParam.getColumnName();
							
							paramQ = "(" + infiniteVar + " = " + sourceString + " | Select(" + physicalQs + ")"
									+ "| ModifyParamQuery(\"<" + searchParamName + ">\")"
									+ "| Sort(columns=[" + physicalQs + "], sort=['ASC'])"
									+ "| Iterate()"
									+ ") "
									+ "| " + ParamStructToJsonGenerator.COLLECT_STRING + ";";
						} else {
							// different structure
							// for sqs or hqs
							// or pixel / query
							String base = null;
							if(param.getDatabaseId() != null && !param.getDatabaseId().isEmpty()
									&& !"FAKE_ENGINE".equals(param.getDatabaseId())) {
								base = "(" + infiniteVar + " = Database(\"" + param.getDatabaseId() + "\") | ";
							} else {
								ParamStructDetails detailParam = param.getDetailsList().get(0);
								String sourceString = detailParam.getImportSource();
								base = "(" + infiniteVar + " = " + sourceString + " | ";
							}

							if(param.getFillType() == FILL_TYPE.PIXEL) {
								// model query is only select/filter/joins that are not part of search
								paramQ = base + modelQuery
										+ " | ModifyParamQuery(\"<" + searchParamName + ">\")"
										+ " | Iterate()"
										+ ") "
										+ "| " + ParamStructToJsonGenerator.COLLECT_STRING + ";";  
							} else {
								// modelQuery is the query itself
								paramQ = base + "Query(\"<encode>" + modelQuery + "</encode>\")"
										+ " | ModifyParamQuery(\"<" + searchParamName + ">\")"
										+ " | Iterate()"
										+ ") "
										+ "| " + ParamStructToJsonGenerator.COLLECT_STRING + ";";  
							}
						}
						// add the paramQ
						if(paramQ != null) {
							modelMap.put("query", paramQ);
						}
					} else {
						// okay - dont require it based on type
						// but if FE sends me something
						// i will put it in...
						if(modelQuery != null && !modelQuery.isEmpty()) {
							modelMap.put("query", modelQuery);
						}
					}
				}
			}
			Object defaultValue = param.getDefaultValue();
			if(defaultValue != null) {
				// we require an array for default value for certain 
				// display types
				// so going to enforce that
				if(ParamStruct.DEFAULT_VALUE_IS_ARRAY.contains(DISPLAY_TYPE)) {
					List<Object> defaultValArray = null;
					if(defaultValue instanceof List) {
						defaultValArray = (List<Object>) defaultValue;
					} else {
						defaultValArray = new Vector<>(1);
						defaultValArray.add(defaultValue);
					}
					modelMap.put("defaultValue", defaultValArray);
				} else {
					// make sure it is not an array
					Object defaultVal = null;
					if(defaultValue instanceof List) {
						defaultVal = ((List<Object>) defaultValue).get(0);
					} else {
						defaultVal = defaultValue;
					}
					modelMap.put("defaultValue", defaultVal);
				}
			}
			// add single quote if present
			for(ParamStructDetails detail : param.getDetailsList()) {
				if(detail.getQuote() == QUOTE.SINGLE) {
					modelMap.put("quoteType", QUOTE.SINGLE);
					break;
				} else if(detail.getQuote() == QUOTE.NO) {
					modelMap.put("quoteType", QUOTE.NO);
					break;
				} else {
					modelMap.put("quoteType", QUOTE.DOUBLE);
				}
			}
			
			// add the model to the param
			paramMap.put("model", modelMap);

			// add param map into list
			paramList.add(paramMap);
		}
		
		return paramList;
	}
	
	/**
	 * Get the model display name
	 * @param param
	 * @return
	 */
	private static String getModelDisplay(ParamStruct param) {
		String modelDisplay = param.getModelDisplay();
		if(modelDisplay != null && !modelDisplay.isEmpty()) {
			// model display is passed from the user
			return modelDisplay;
		}
		
		// logic is as follows
		// if there is a numeric comparison
		// set it to number
		// otherwise use checklist
		List<ParamStructDetails> details = param.getDetailsList();
		boolean hasNumericOperator = false;
		for(ParamStructDetails detail : details) {
			String operator = detail.getOperator();
			if(!hasNumericOperator) {
				hasNumericOperator = IQueryFilter.comparatorIsNumeric(operator);
			}
		}
		
		if(hasNumericOperator) {
			return "number";
		}
		
		// default to checklist
		return "checklist";
	}
	
	
	/**
	 * Test method
	 * @param args
	 */
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:\\workspace3\\Semoss_Dev\\RDF_Map.prop");
//		
//		String[] recipe = new String[]{
//				"AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ;",
//				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;",
//				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
//				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
//				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
//				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;", 
//				"Database ( database = [ \"995cf169-6b44-4a42-b75c-af12f9f45c36\" ] ) | Select ( DIABETES__AGE , DIABETES__BP_1D , DIABETES__BP_1S , DIABETES__BP_2D , DIABETES__BP_2S , DIABETES__CHOL , DIABETES__DIABETES_UNIQUE_ROW_ID , DIABETES__DRUG , DIABETES__END_DATE , DIABETES__FRAME , DIABETES__GENDER , DIABETES__GLYHB , DIABETES__HDL , DIABETES__HEIGHT , DIABETES__HIP , DIABETES__LOCATION , DIABETES__PATIENT , DIABETES__RATIO , DIABETES__STAB_GLU , DIABETES__START_DATE , DIABETES__TIME_PPN , DIABETES__WAIST , DIABETES__WEIGHT ) .as ( [ AGE , BP_1D , BP_1S , BP_2D , BP_2S , CHOL , DIABETES_UNIQUE_ROW_ID , DRUG , END_DATE , FRAME , GENDER , GLYHB , HDL , HEIGHT , HIP , LOCATION , PATIENT , RATIO , STAB_GLU , START_DATE , TIME_PPN , WAIST , WEIGHT ] ) | Filter ( ( ( DIABETES__AGE > [ 50 ] ) ) ) | Distinct ( false ) | Import ( frame = [ CreateFrame ( frameType = [ PY ] , override = [ true ] ) .as ( [ \"Diabetes_FRAME916484\" ] ) ] ) ;",
//				"Database ( database = [ \"995cf169-6b44-4a42-b75c-af12f9f45c36\" ] ) | Query (\"<encode>SELECT * FROM DIABETES WHERE AGE > 25</encode>\") | Distinct ( false ) | Import ( frame = [ CreateFrame ( frameType = [ PY ] , override = [ true ] ) .as ( [ \"Diabetes_FRAME555555\" ] ) ] ) ;",
//				"META | PositionInsightRecipeStep ( positionMap = [ { \"auto\" : false , \"top\" : 24 , \"left\" : 24 } ] ) ;",
//				"META | SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"order\":0,\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});",
//		};
//		int recipeLength = recipe.length;
//		String[] ids = new String[recipeLength];
//		for(int i = 0; i < recipeLength; i++) {
//			ids[i] = i+"";
//		}
//		
//		List<ParamStruct> params = new Vector<>();
//		// param
//		{
//			ParamStruct pStruct = new ParamStruct();
//			params.add(pStruct);
//			pStruct.setDefaultValue("MALE");
//			pStruct.setParamName("Genre");
//			pStruct.setFillType(FILL_TYPE.PIXEL);
//			pStruct.setModelQuery("");
//			pStruct.setModelDisplay("typeahead");
//			pStruct.setMultiple(true);
//			pStruct.setRequired(true);
//			pStruct.setSearchable(true);
//			pStruct.setModelLabel("Fill in Genre");
//			{
//				ParamStructDetails details = new ParamStructDetails();
//				pStruct.addParamStructDetails(details);
//				details.setPixelId("6");
//				details.setPixelString(recipe[6]);
//				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
//				details.setTableName("DIABETES");
//				details.setColumnName("GENRE");
//				details.setOperator("==");
//				details.setLevel(LEVEL.COLUMN);
//				details.setQuote(QUOTE.DOUBLE);
//			}
//		}
//		// param
//		{
//			ParamStruct pStruct = new ParamStruct();
//			params.add(pStruct);
//			pStruct.setDefaultValue(null);
//			pStruct.setParamName("AGE");
//			pStruct.setFillType(FILL_TYPE.PIXEL);
//			pStruct.setModelQuery("");
//			pStruct.setModelDisplay("number");
//			pStruct.setMultiple(true);
//			pStruct.setRequired(true);
//			pStruct.setSearchable(true);
//			pStruct.setModelLabel("Fill in AGE");
//			{
//				ParamStructDetails details = new ParamStructDetails();
//				pStruct.addParamStructDetails(details);
//				details.setPixelId("6");
//				details.setPixelString(recipe[6]);
//				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
//				details.setTableName("DIABETES");
//				details.setColumnName("AGE");
//				details.setOperator(">");
//				details.setLevel(LEVEL.OPERATOR);
//				details.setQuote(QUOTE.DOUBLE);
//			}
//		}
//		// param
//		{
//			ParamStruct pStruct = new ParamStruct();
//			params.add(pStruct);
//			pStruct.setDefaultValue(null);
//			pStruct.setParamName("AGE2");
//			pStruct.setFillType(FILL_TYPE.PIXEL);
//			pStruct.setModelQuery("");
//			pStruct.setModelDisplay("checklist");
//			pStruct.setMultiple(true);
//			pStruct.setRequired(true);
//			pStruct.setSearchable(true);
//			pStruct.setModelLabel("Fill in AGE");
//			{
//				ParamStructDetails details = new ParamStructDetails();
//				pStruct.addParamStructDetails(details);
//				details.setPixelId("7");
//				details.setPixelString(recipe[7]);
//				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
//				details.setTableName("DIABETES");
//				details.setColumnName("AGE");
//				details.setOperator(">");
//				details.setCurrentValue(new Long(25));
//				details.setLevel(LEVEL.OPERATOR);
//				details.setQuote(QUOTE.DOUBLE);
//			}
//		}
//		
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//
//		System.out.println(gson.toJson(generateJsonParameters(params)));
//	}
	
}
