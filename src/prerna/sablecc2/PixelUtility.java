package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.pipeline.PipelineOperation;
import prerna.sablecc2.pipeline.PipelineTranslation;
import prerna.sablecc2.translations.DatasourceTranslation;
import prerna.sablecc2.translations.ParameterizeSaveRecipeTranslation;
import prerna.sablecc2.translations.ReplaceDatasourceTranslation;

public class PixelUtility {

	private static final Logger logger = LogManager.getLogger(PixelUtility.class);

	private static final String STACKTRACE = "StackTrace: ";
	
	/**
	 * 
	 * @param pixelExpression
	 * @return
	 * 
	 * Returns true if the pixel is a valid pixel that can be executed
	 * Returns false otherwise
	 * @throws IOException 
	 * @throws LexerException 
	 * @throws ParserException 
	 */
	public static Set<String> validatePixel(String pixelExpression) throws ParserException, LexerException, IOException {
		return PixelRunner.validatePixel(pixelExpression);
	}
	
	/**
	 * 
	 * @param pixelExpression
	 * @return
	 * @throws ParserException
	 * @throws LexerException
	 * @throws IOException
	 * 
	 * Returns a list of the parsed pixels from the expression
	 */
	public static List<String> parsePixel(String pixelExpression) throws ParserException, LexerException, IOException {
		return PixelRunner.parsePixel(pixelExpression);
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 * 
	 * Returns the noun for a STRING or NUMBER ONLY
	 * if value is an instanceof another object IllegalArgumentException will be thrown
	 */
	public static NounMetadata getNoun(Object value) {
		NounMetadata noun = null;
		if(value instanceof Number) {
			noun = new NounMetadata(((Number)value).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if(value instanceof String) {
			if(isLiteral((String)value)) {
				//we have a literal
				String literal = removeSurroundingQuotes((String)value);
				noun = new NounMetadata(literal, PixelDataType.CONST_STRING);
			} else {
				// try to convert to a number
				try {
					double doubleValue = Double.parseDouble(value.toString().trim());
					noun = new NounMetadata(doubleValue, PixelDataType.CONST_DECIMAL);
				} catch(NumberFormatException e) {
					// confirmed that it is not a double
					// and that we have a column
					noun = new NounMetadata(value.toString().trim(),PixelDataType.COLUMN);
				}
			}
		} else {
			throw new IllegalArgumentException("Value must be a number or string!");
		}
		
		return noun;
	}
	
	public static String generatePixelString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	public static String generatePixelString(String assignment, Object value) {
		return generatePixelString(assignment, value.toString());	
	}
	
	
	/**
	 * 
	 * @param planner
	 * @param pixelString
	 * 
	 * Adds a pkslString to the planner via lazy translation
	 */
	public static void addPixelToTranslation(DepthFirstAdapter translation, String pixelString) {
		try {
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(pixelString.getBytes("UTF-8")), "UTF-8"))));
			Start tree = p.parse();
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException ioe) {
			logger.error("FAILED ON :::: " + pixelString);
			logger.error(STACKTRACE, ioe);
		} catch(Exception e) {
			logger.error("FAILED ON :::: " + pixelString);
			logger.error(STACKTRACE, e);
		}
	}
	
	/**
	 * @param literal
	 * @return
	 * 
	 * input: 'literal' OR "literal"
	 * output: literal
	 * 
	 * input: 'literal OR "literal
	 * output: literal
	 * 
	 * input: literal' OR literal"
	 * output: literal
	 * 
	 * input: literal
	 * output: literal
	 */
	public static String removeSurroundingQuotes(String literal) {
		literal = literal.trim();
		if(literal.startsWith("\"") || literal.startsWith("'")) {
			literal = literal.substring(1); //remove the first quote
		}
		
		if(literal.endsWith("\"") || literal.endsWith("'")) {
			if(literal.length() == 1) {
				literal = "";
			} else {
				literal = literal.substring(0, literal.length()-1); //remove the end quote
			}
		}
		
		return literal;
	}
	
	/**
	 * 
	 * @param literal
	 * @return
	 * 
	 * Determines if the string is a literal
	 */
	public static boolean isLiteral(String literal) {
		literal = literal.trim();
		return ((literal.startsWith("\"") || literal.startsWith("'")) && (literal.endsWith("\"") || literal.endsWith("'")));
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixels
	 * @return
	 */
	public static boolean hasParam(String[] pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.hasParam(recipe);
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixels
	 * @return
	 */
	public static boolean hasParam(List<String> pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.hasParam(recipe);
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixel
	 * @return
	 */
	public static boolean hasParam(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		try {
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
			InsightParamTranslation translation = new InsightParamTranslation();
			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.hasParam();
		} catch (ParserException | LexerException | IOException e) {
			logger.error(STACKTRACE, e);
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			logger.info(eMessage);
		}
		return false;
	}
	
	/**
	 * Check if recipe is for a dashboard
	 * @param pixels
	 * @return
	 */
	public static boolean isDashboard(String[] pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isDashboard(recipe);
	}
	
	/**
	 * Check if recipe is for a dashboard
	 * @param pixels
	 * @return
	 */
	public static boolean isDashboard(List<String> pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.isDashboard(recipe);
	}
	
	/**
	 * Check if recipe is for a dashboard
	 * @param pixel
	 * @return
	 */
	public static boolean isDashboard(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		try {
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
			DashboardRecipeTranslation translation = new DashboardRecipeTranslation();
			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.isDashboard();
		} catch (ParserException | LexerException | IOException e) {
			logger.error(STACKTRACE, e);
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			logger.info(eMessage);
		}
		return false;
	}
	
	/**
	 * 
	 * @param pixel
	 * @return {into, values}
	 */
	public static Object[] getFormWidgetInputs(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new ArrayList<String>(), new HashMap<String, String>());
		Object[] ret = new Object[2];
		try {
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
			FormWidgetTranslation translation = new FormWidgetTranslation();
			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			ret[0] = translation.getInto();
			ret[1] =translation.getValues();
			return ret;
		} catch (ParserException | LexerException | IOException e) {
			logger.error(STACKTRACE, e);
		}
		return null;
	}
	
	/**
	 * 
	 * @param expression
	 * @param encodedTextToOriginal
	 * @return
	 * 
	 * Returns the string of the NOT encoded expression
	 * allows us to get the expression in its not encoded form from a mapping of the expression to what it looked like originally
	 */
	public static String recreateOriginalPixelExpression(String expression, List<String> encodingList, Map<String, String> encodedTextToOriginal) {
		if(encodedTextToOriginal == null || encodedTextToOriginal.isEmpty()) {
			return expression;
		}
		// loop through and see if any encodedText portions have been modified
		// if they have, try and replace them back so it looks pretty for the FE
		Collections.sort(encodingList, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int l1 = o1.length();
				int l2 = o2.length();
				
				if(l1 > l2) {
					return -1;
				} else if(l1 < l2) {
					return 1;
				}
				
				return 0;
			}
		});
		
		Iterator<String> iterator = encodingList.iterator();
		while(iterator.hasNext()) {
			String encodedText = iterator.next();
			if(expression.contains(encodedText)) {
				expression = expression.replaceFirst(Pattern.quote(encodedText), Matcher.quoteReplacement(encodedTextToOriginal.get(encodedText)));
				iterator.remove();
			}
		}
		return expression;
	}
	
	/**
	 * Get the data sources within the full expression
	 * @param expression
	 * @return
	 */
	public static List<Map<String, Object>> getDatasourcesMetadata(User user, String expression) {
		/*
		 * Using a translation object to go through and figure out all 
		 * the datasources and how we would want to manipulate
		 * and change them as people swap the data but want to use the same
		 * routine / analysis
		 */
		
		Insight in = new Insight();
		in.setUser(user);
		DatasourceTranslation translation = new DatasourceTranslation(in);
		try {
			expression = PixelPreProcessor.preProcessPixel(expression, new ArrayList<String>(), new HashMap<String, String>());
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(expression.getBytes("UTF-8")), "UTF-8"), expression.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			logger.error(STACKTRACE, e);
		}

		return translation.getDatasourcePixels();
	}
	
	/**
	 * Modify the file datasources in a given recipe
	 * @param fullRecipe					String containing the original recipe to change
	 * @param replacementOptions			List of maps containing "index" and "pixel" which represents the pixel step to change 
	 * 										and the new pixel to put in its place
	 * 										If no index is found and the size of the list is 1, we will replace the first datasource
	 * @return
	 */
	public static List<String> modifyInsightDatasource(Insight in, String fullRecipe, List<Map<String, Object>> replacementOptions) {
		ReplaceDatasourceTranslation translation = new ReplaceDatasourceTranslation(in);
		translation.setReplacements(replacementOptions);
		try {
			fullRecipe = PixelPreProcessor.preProcessPixel(fullRecipe, translation.encodingList, translation.encodedToOriginal);
			Parser p = new Parser(
					new Lexer(
							new PushbackReader(
									new InputStreamReader(
											new ByteArrayInputStream(fullRecipe.getBytes("UTF-8")), "UTF-8"), fullRecipe.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			logger.error(STACKTRACE, e);
		}
		
		return translation.getPixels();
	}
	
	/**
	 * Determine if an operation op type is an error that requires user input and then a re-run of the insight
	 * @param opTypes
	 * @return
	 */
	public static boolean autoExecuteAfterUserInput(List<PixelOperationType> opTypes) {
		if(opTypes.contains(PixelOperationType.LOGGIN_REQUIRED_ERROR)) {
			return true;
		}
		return false;
	}
	
	/**
	 * Add parameters into an existing recipe
	 * @param recipe
	 * @param params
	 * @return
	 */
	public static String getParameterizedRecipe(User user, String[] recipe, List<Map<String, Object>> paramsMap, String insightName) {
		int numParams = paramsMap.size();
		List<String> params = new ArrayList<>(numParams);
		for(Map<String, Object> pMap : paramsMap) {
			String pName = (String) pMap.get("paramName");
			if(pName == null || pName.isEmpty()) {
				throw new IllegalArgumentException("Parameter list must all contain 'paramName'");
			}
			params.add(pName);
		}
		
		Insight in = new Insight();
		in.setUser(user);
		ParameterizeSaveRecipeTranslation translation = new ParameterizeSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);
		
		// loop through recipe
		for(String expression : recipe) {
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList, translation.encodedToOriginal);
				Parser p = new Parser(
						new Lexer(
								new PushbackReader(
										new InputStreamReader(
												new ByteArrayInputStream(expression.getBytes("UTF-8")), "UTF-8"), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				logger.error(STACKTRACE, e);
			}
			
		}
		
		List<String> newRecipe = translation.getPixels();
		// since i am already adding a AddPanel(0) at the start of this recipe
		// if it is contained as the first index inside newRecipe
		// i will remove it
		String first = newRecipe.get(0);
		first = first.replace(" ", "").trim();
		if(first.equals("AddPanel(0);")) {
			newRecipe.remove(0);
		}
		
		StringBuilder fullRecipe = new StringBuilder();
		for(String s : newRecipe) {
			fullRecipe.append(s.trim());
		}
		Map<String, Map<String, String>> processedParams = translation.getParamToSource();
		
		List<Map<String, Object>> vec = new Vector<>();
		Map<String, Object> map = new LinkedHashMap<>();
		// recipe is the query
		map.put("query", fullRecipe.toString());
		map.put("label", insightName);
		map.put("description", "Please select paramters for the insight");
		// add params
		List<Map<String, Object>> paramList = new Vector<>();
		for(int i = 0; i < numParams; i++) {
			Map<String, Object> pMap = paramsMap.get(i);
			boolean keepSearch = keepSearchParameter(pMap);
			String param = (String) pMap.get("paramName");
			
			// and now for the param itself
			Map<String, Object> paramMap = new LinkedHashMap<>();
			paramMap.put("paramName", param);
			paramMap.put("required", true);
			paramMap.put("useSelectedValues", true);
			// nested map for view
			Map<String, Object> paramViewMap = new LinkedHashMap<>();
			paramViewMap.put("displayType", "checklist");
			paramViewMap.put("label", "Select an Instance Of The Parameter : " + param);
			// nested attributes map within nested view map
			Map<String, Boolean> paramViewAttrMap = new LinkedHashMap<>();
			paramViewAttrMap.put("searchable", true);
			paramViewAttrMap.put("multiple", true);
			paramViewAttrMap.put("quickselect", true);
			paramViewMap.put("attributes", paramViewAttrMap);
			// add view
			paramMap.put("view", paramViewMap);
			// nested map for model
			Map<String, Object> modelMap = new LinkedHashMap<>();
			Map<String, String> processedParam = processedParams.get(param);
			String physicalQs = processedParam.get("qs");
			String paramQ = "(infinite = " + processedParam.get("source") + " | Select(" + physicalQs 
					+ ") | Filter(" + physicalQs + " ?like \"<" + param + "_Search>\") | Sort(columns=[" 
					+ physicalQs + "], sort=[asc]) | Iterate()) | Collect(20);";  
			modelMap.put("query", paramQ);
			if(keepSearch) {
				modelMap.put("infiniteQuery", "infinite | Collect(20)");
				modelMap.put("searchParam", param + "_Search");
				modelMap.put("dependsOn", new String[]{param + "_Search"});
			}
			paramMap.put("model", modelMap);
			
			if(keepSearch) {
				// each param gets its own search
				Map<String, Object> paramSearchMap = new LinkedHashMap<>();
				paramSearchMap.put("paramName", param + "_Search");
				paramSearchMap.put("view", false);
				Map<String, String> paramSearchModel = new LinkedHashMap<>();
				paramSearchModel.put("defaultValue", "");
				paramSearchMap.put("model", paramSearchModel);
				paramList.add(paramSearchMap);
			}
			
			// now merge the existing pMap into the default values
			recursivelyMergeMaps(paramMap, pMap);
			// add to the param list
			paramList.add(paramMap);
		}
		// add param list
		map.put("params", paramList);
		// add execute
		map.put("execute", "button");
		vec.add(map);
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		return "AddPanel(0); Panel (0) | SetPanelView(\"param\", \"<encode> {\"json\":" + gson.toJson(vec) + "}</encode>\");";
	}
	
	/**
	 * Recursively join two maps together based on the keys
	 * @param mainMap
	 * @param newMap
	 */
	private static void recursivelyMergeMaps(Map<String, Object> mainMap, Map<String, Object> newMap) {
		if(newMap != null) {
			for(String key : newMap.keySet()) {
				if(mainMap.containsKey(key)) {
					// we have an overlap
					// lets see if the children are both maps
					boolean newKeyIsMap = (newMap.get(key) instanceof Map);
					boolean existingKeyIsMap = (mainMap.get(key) instanceof Map);
					if(newKeyIsMap && existingKeyIsMap) {
						// recursively go through and try to add
						recursivelyMergeMaps( (Map) mainMap.get(key), (Map) newMap.get(key));
					} else {
						// both are not maps
						// just override
						mainMap.put(key, newMap.get(key));
					}
				} else {
					// brand new key
					// put all into the main map
					mainMap.put(key, newMap.get(key));
				}
			}
		}
	}
	
	/**
	 * Determine if we need the search in the parameter list
	 * @param map
	 * @return
	 */
	private static boolean keepSearchParameter(Map<String, Object> map) {
		final String MODEL_KEY = "model";
		final String DEFAULT_OPTIONS_KEY = "defaultOptions";
		
		if(map.containsKey(MODEL_KEY)) {
			Map<String, Object> innerMap = (Map<String, Object>) map.get(MODEL_KEY);
			if(innerMap.containsKey(DEFAULT_OPTIONS_KEY)) {
				return false;
			}
		}
		
		return true;
	}

	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Get the pipeline operations for a given pixel
	 * @param in
	 * @param pixel
	 * @return
	 */
	public static List<List<PipelineOperation>> generatePipeline(Insight in, String pixel) {
		PipelineTranslation translation = null;
		List<String> encodingList = new Vector<>();
		Map<String, String> encodedTextToOriginal = new HashMap<>();
		try {
			pixel = PixelPreProcessor.preProcessPixel(pixel.trim(), encodingList, encodedTextToOriginal);
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
			translation = new PipelineTranslation(in);

			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch(SemossPixelException e) {
			if(!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (ParserException | LexerException | IOException e) {
			// we only need to catch invalid syntax here
			// other exceptions are caught in lazy translation
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			throw new IllegalArgumentException(eMessage, e);
		}

		if (translation == null) {
			throw new NullPointerException("Pipeline translation should not be null here.");
		}

		return translation.getAllRoutines();
	}
	
	
	
	/////////////////////////////////////////////////////////////////////////////////////

	
	/*
	 * LEGACY
	 * 
	 */
	
//	/**
//	 * Process the noun metadata for consumption on the FE
//	 * @param noun
//	 * @return
//	 */
//	public static Map<String, Object> processNounMetadata(NounMetadata noun) {
//		Map<String, Object> ret = new HashMap<String, Object>();
//		if(noun.getNounType() == PixelDataType.FRAME) {
//			// if we have a frame
//			// return the table name of the frame
//			// FE needs this to create proper QS
//			// this has no meaning for graphs
//			Map<String, String> frameData = new HashMap<String, String>();
//			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
//			frameData.put("type", FrameFactory.getFrameType(frame));
//			String name = frame.getTableName();
//			if(name != null) {
//				frameData.put("name", name);
//			}
//			ret.put("output", frameData);
//			ret.put("operationType", noun.getOpType());
//			
//			// add additional outputs
//			List<Map<String, Object>> additionalOutputList = new Vector<Map<String, Object>>();
//			List<NounMetadata> addReturns = noun.getAdditionalReturn();
//			int numOutputs = addReturns.size();
//			for(int i = 0; i < numOutputs; i++) {
//				additionalOutputList.add(processNounMetadata(addReturns.get(i)));
//			}
//			if(!additionalOutputList.isEmpty()) {
//				ret.put("additionalOutput", additionalOutputList);
//			}
//			
//			// add message
//			if(noun.getExplanation() != null && !noun.getExplanation().isEmpty()) {
//				ret.put("message", noun.getExplanation());
//			}
//		} else if(noun.getNounType() == PixelDataType.CODE || noun.getNounType() == PixelDataType.TASK_LIST) {
//			// code is a tough one to process
//			// since many operations could have been performed
//			// we need to loop through a set of noun meta datas to output
//			ret.put("operationType", noun.getOpType());
//			List<Map<String, Object>> outputList = new Vector<Map<String, Object>>();
//			List<NounMetadata> codeOutputs = (List<NounMetadata>) noun.getValue();
//			int numOutputs = codeOutputs.size();
//			for(int i = 0; i < numOutputs; i++) {
//				outputList.add(processNounMetadata(codeOutputs.get(i)));
//			}
//			ret.put("output", outputList);
//		} else {
//			ret.put("output", noun.getValue());
//			ret.put("operationType", noun.getOpType());
//		}
//		return ret;
//	}
}
