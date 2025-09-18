package prerna.reactor.frame;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.om.Insight;
import prerna.engine.impl.model.LLMReactor;
import prerna.engine.impl.model.responses.AskModelEngineResponse;

public class FrameToGraphReactor extends AbstractRFrameReactor {

	private static final String ENGINE_KEY = "engine";
	private static final String COMMAND_KEY = "command";
    private static final String CONTEXT_KEY = "context";
    private static final String USE_HISTORY_KEY = "use_history";
    private static final String CATEGORICAL="CATEGORICAL";
    private static final String NUMERICAL="NUMERICAL";
    private static final String TEMPORAL="TEMPORAL";
    private static final String USER_INPUT="userInput";
	private static final Logger classLogger = LogManager.getLogger(FrameToGraphReactor.class);
    
    public FrameToGraphReactor() {
		this.keysToGet = new String[] { 
			ReactorKeysEnum.FRAME.getKey(), 
			ReactorKeysEnum.MODEL.getKey(), 
			USER_INPUT, 
			ReactorKeysEnum.INSIGHT_NAME.getKey(), 
		};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Check required R packages and libraries
//		String[] packages = new String[] {"igraph"};
//		this.rJavaTranslator.checkPackages(packages);
//		this.rJavaTranslator.executeEmptyR("library(igraph)");
		
		///////// DATA PARSING ///////////
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
	    ITableDataFrame sourceFrame = getFrame();
		
		// 5 types of Frames: Native, Python, Grid (SQL), R and Tinker Frames
		if (sourceFrame instanceof NativeFrame) {
			NativeFrame sourceNFrame = (NativeFrame) sourceFrame;
		} else if (sourceFrame instanceof PandasFrame) {
			PandasFrame sourcePFrame = (PandasFrame) sourceFrame;
		} else if (sourceFrame instanceof TinkerFrame) {
			// TODO: Implement Grid, R, and Tinker Frame instances
			System.out.println("TODO: Implement Grid and R instances");
		} else {
			System.err.println("Unsupported frame type for query execution");
		}
		
//		String projectID = ""; //however you want to get it
//		Insight insight = this.insight;
//		if (insight != null) {
//		    projectID = this.insight.getContextProjectId();
//		    if (projectID == null || projectID.trim().isEmpty()) {
//		        throw new IllegalArgumentException(
//		                "Project does not exist or user does not have access to this project");
//		    }
//		}
 
//		String assetsDir = AssetUtility.getProjectAssetFolder(projectID).replace("\\", "/");
		
	   String userInput = this.keyValue.get(this.keysToGet[2]);
		
	   String CONTEXT = "\"You are a data visualization expert. Your task is to create a Vega-Lite JSON specification based on the raw data provided and choosing from a list of templates.";
			   
	   if (userInput != null) {
		   CONTEXT += " \n"		  
		    	  	+ "First, carefully consider the user's input:\n"
		    	  	+ "- If the user specifies a type of graph, use that graph type. If not specified, select from a template from the list that best fits the data.\n"
		    	  	// + "- If the user specifies a color scheme, apply that color scheme. \n"
		    	  	// + "- If the user requests showing or comparing only specific data columns, data types, or subsets of the data, ensure the final graph reflects exactly those selections.\n"
		    	  	// + "- Interpret any specific user instructions carefully. For example, if the user says: �Show me a graph comparing column A and column B only,� or �Plot a bar chart showing sales over time with blue tones,� incorporate these instructions fully.\n"
		    	  	+ "- If the user asks to omit anything that is currently present in the template, ensure it is removed in the final output. Else do not remove the relevant data\n"
					+ "- Interpret any specific user instructions carefully."
					+ "- Do not ignore any part of the user's prompt regarding data filtering, graph type, color scheme, or other preferences.\n\n"
	   				+ " Most importantly ONLY return the Vega JSON. Do not include any text, explanation, markdown, or notes.\n"
					;
				}

	   
	   // TODO: Clean the user input 
	   String PROMPT = "";
	   if (userInput != null) { 
		   PROMPT = "Here is the user input:\n\"" + userInput + "\"\n";
	   }
	   
	   PROMPT += "Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data by selecting the graph template that best fits the data and user's needs.\"\n"
    	    + " Your task is to fill in the data directly into the values section. Additionally, add values to the placeholder values  "
	  		+ " \n"
    	    // + "            Your output must include:\n"
    	    // + "            1. \"**The complete Vega-Lite JSON spec** only � do not include any explanation, commentary, irregular quotation marks in data values, or code blocks.\"\n"
    	    // + "            2. \"Ensure the spec includes appropriate settings for:\"\n"
    	    // + "               \"- `mark` type (e.g., bar, line, point, area, etc.)\"\n"
    	    // + "               \"- `encoding` for x and y axes (use fields and types from the data)\"\n"
    	    // + "               \"- Optional: tooltips, color, and other enhancements to improve clarity\"\n"
    	    // + "            3. \"Use reasonable assumptions if the chart type is not specified.\"\n"
    	    + " \"Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.\"\n";
		    // if (userInput != null) { 
		    // 	PROMPT += "5. \"The most meaningful and suprising patterns, insights, or anomalies possible in the dataset.\"\n";
			// }
   		PROMPT += "            Guidelines:\n"
		    + "            \"- Avoid complex transforms unless specified in the user's prompt\"\n"
		    // + "            \"- Choose the chart type from the appropriate chart family (temporal, categorical, hierarchical, relational, spatial) that best fits the data and user instructions.\"\n"
		    + "            \"- Add axis titles based on the field names.\"\n"
		    + " \n"
			+ " Here are the templates you can choose from: \n"
	   		+ getVegaBarChartTemplate() + "\n"
	   		+ getVegaLineChartTemplate() + "\n"
	   		+ getVegaPieChartTemplate() + "\n"
	   		+ " Here also some template specific guidelines to follow: \n"
			+ " For PieCharts: do NOT remove the transform nor the signals section."
			;


		///////// MODEL ///////////
		String QUESTION = PROMPT + buildVegaPrompt(sourceFrame);
		
		System.out.println("DEBUG: ");
		System.out.println(CONTEXT);
		System.out.println(QUESTION);
		
		
		AskModelEngineResponse modelResponse = callLLM(
            CONTEXT, QUESTION
//            this.insight.getInsightFolder() // TODO: Unsure about this
        );
        
        if (modelResponse != null) {
            System.out.println("LLM Response: " + modelResponse.getResponse());
            // Process the response as needed (e.g., parse steps, adjust graph configuration, etc.)
        } else {
            System.err.println("No valid response from LLM model call");
        }
		
		return new NounMetadata(modelResponse.getResponse(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
	
	protected ITableDataFrame getFrame() {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.getKey());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			List<Object> frameInputs = grs.getValuesOfType(PixelDataType.FRAME);
			if (!frameInputs.isEmpty()) {
				return (ITableDataFrame) frameInputs.get(0);
			}
		}
		
		List<NounMetadata> curNouns = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(curNouns != null && !curNouns.isEmpty()) {
			return (ITableDataFrame) curNouns.get(0).getValue();
		}
		
		// else, grab the default frame from the insight
		// put this into the noun store
		// so that we can pull it for other pipeline
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		if (defaultFrame != null) {
			this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
			return defaultFrame;
		}

		throw new NullPointerException("No frame found");
		
	}
	
	protected List<String> getFrameColumns() {
		List<String> columns = new Vector<String>();

		GenRowStruct sourceColGrs = this.store.getNoun(this.keysToGet[2]);
		if (sourceColGrs != null && !sourceColGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < sourceColGrs.size(); selectIndex++) {
				String column = sourceColGrs.get(selectIndex) + "";
				columns.add(column);
			}
			return columns;
		}
		throw new IllegalArgumentException("Must define the frame columns");
	}

	/**
	 * Prompt building
	 * 
	 */
	private String buildVegaPrompt(ITableDataFrame sourceFrame) {
		StringBuilder promptBuilder = new StringBuilder();

		String[] headers = sourceFrame.getColumnHeaders();
		int frameSize = (int) sourceFrame.size(sourceFrame.getName());
		int rowCount = frameSize / headers.length;
		System.out.println("rowCount: " + rowCount);
		
		// TODO: Make this work for frames greater than size 20. Test how many rows we can insert into prompt.
		int sampleRows = Math.min(rowCount, 20);

		// Partition headers into numerical and categorical based on the first non-null cell in row 0
		List<String> numericalHeaders = new ArrayList<>();
        List<String> categoricalHeaders = new ArrayList<>();
        List<String> temporalHeaders = new ArrayList<>();
        
        
        String[] dataTypes = getColumnDataType(sourceFrame);
        
        // Filter headers by data type (Numerical, Categorical, or Temporal). Categorical is default
        for (int i = 0; i < dataTypes.length; i++) {
            if (dataTypes[i].equals(NUMERICAL)) {
                numericalHeaders.add(headers[i]);
                System.out.println("Numeric: " + dataTypes[i]);
            } else if (dataTypes[i].equals(TEMPORAL)) {
            	temporalHeaders.add(headers[i]);
            	System.out.println("Temporal: " + dataTypes[i]);
            } else {
                categoricalHeaders.add(headers[i]);
                System.out.println("Categorical: " + dataTypes[i]);
            }
        }

        // Begin JSON template
        
        
        System.out.println("Starting to embed data into Vega Prompt: " + promptBuilder.toString());

//        if (!categoricalHeaders.isEmpty()) {
//            // Embed categorical data
//        	promptBuilder.append("    \"categorical\": [\n");
//            
//            // Each row entry in [] should start with '{' and end with '}'
//            for (int r = 0; r < sampleRows; r++) {
//            	promptBuilder.append("      {");
//                for (int i = 0; i < categoricalHeaders.size(); i++) {
//                	// Get the corresponding numericRowData for the current header
//                    String header = categoricalHeaders.get(i);
//                    Double[] categoricalRowData = sourceFrame.getColumnAsNumeric(header);
//                    
//                    // "Fruit" : "Apple"
//                    promptBuilder.append("\"").append(header).append("\": \"").append(categoricalRowData[r]).append("\"");
//                    if (i < categoricalHeaders.size() - 1) {
//                        promptBuilder.append(", ");
//                    }
//                }
//                
//                // Handles inserting ',' after each row entry
//                promptBuilder.append("}");
//                if (r < sampleRows - 1) {
//                    promptBuilder.append(",\n");
//                } else {
//                    promptBuilder.append("\n");
//                }
//            }
//            promptBuilder.append("    ],\n");
//        }
//            
//        if (!numericalHeaders.isEmpty()) {
//            // Embed numerical data
//            promptBuilder.append("    \"numerical\": [\n");
//            
//            // Each row entry in [] should start with '{' and end with '}'
//            for (int r = 0; r < sampleRows; r++) {
//                promptBuilder.append("      {");
//                for (int i = 0; i < numericalHeaders.size(); i++) {
//                	// Get the corresponding numericRowData for the current header
//                    String header = numericalHeaders.get(i);
//                    Object[] numericRowData = sourceFrame.getColumn(header);
//                    
//                    // "Temperature" : 21
//                    promptBuilder.append("\"").append(header).append("\": ").append(numericRowData[r]);
//                    if (i < numericalHeaders.size() - 1) {
//                        promptBuilder.append(", ");
//                    }
//                }
//                promptBuilder.append("}");
//                
//                // Handles inserting ',' after each row entry
//                if (r < sampleRows - 1) {
//                    promptBuilder.append(",\n");
//                } else {
//                    promptBuilder.append("\n");
//                }
//            }
//            promptBuilder.append("    ]\n");
//        }
//        
//        if (!temporalHeaders.isEmpty()) {
//            // Embed numerical data
//            promptBuilder.append("    \"numerical\": [\n");
//            
//            // Each row entry in [] should start with '{' and end with '}'
//            for (int r = 0; r < sampleRows; r++) {
//                promptBuilder.append("      {");
//                for (int i = 0; i < numericalHeaders.size(); i++) {
//                	// Get the corresponding numericRowData for the current header
//                    String header = numericalHeaders.get(i);
//                    Object[] numericRowData = sourceFrame.getColumn(header);
//                    
//                    // "Temperature" : 21
//                    promptBuilder.append("\"").append(header).append("\": ").append(numericRowData[r]);
//                    if (i < numericalHeaders.size() - 1) {
//                        promptBuilder.append(", ");
//                    }
//                }
//                promptBuilder.append("}");
//                
//                // Handles inserting ',' after each row entry
//                if (r < sampleRows - 1) {
//                    promptBuilder.append(",\n");
//                } else {
//                    promptBuilder.append("\n");
//                }
//            }
//            promptBuilder.append("    ]\n");
//        }

        // Fallback: if all columns are of one type, embed data under "values"
        promptBuilder.append("\n    \"values\": [\n");
        for (int r = 0; r < sampleRows; r++) {
            promptBuilder.append("      {");
            for (int i = 0; i < headers.length; i++) {
            	String header = headers[i];
                Object[] rowData = sourceFrame.getColumn(header);
                
                // TODO: Move this into outer for loop
                // Instead of breaking out of the loop when rowData is too short,
                // check if the column has a cell at index r.
                if (rowData.length <= r) {
                    // If out of bounds, output an empty value.
                    promptBuilder.append("\"").append(header).append("\": \"\"");
                } else {
                    Object cell = rowData[r];
                    System.out.println("Cell value: " + cell + " at row: " + r);
                    
                    // If we have only numerical data, omit quotes. Otherwise, include quotes.
                    if (cell instanceof SemossDate) {
                        // Use the date's toString() or a custom format.
                        promptBuilder.append("\"").append(header).append("\": \"").append(cell.toString()).append("\"");
                    } else if (numericalHeaders.isEmpty() || !(cell instanceof Number)) {
                        promptBuilder.append("\"").append(header).append("\": \"").append(cell.toString()).append("\"");
                    } else {
                        Number num = (Number) cell;
                        promptBuilder.append("\"").append(header).append("\": ").append(num.doubleValue());
                    }
                }
                
                if (i < headers.length - 1) {
                    promptBuilder.append(", ");
                }
            }
            promptBuilder.append("}");
            if (r < sampleRows - 1) {
                promptBuilder.append(",\n");
            } else {
                promptBuilder.append("\n");
            }
        }
        
        promptBuilder.append("    ]\n}");

        // Minimal encoding: for demonstration use first categorical as x and first numerical as y (if available)
        // Waiting on prompt for mark, encoding, x field and x type. y field and y type. 
        // e.g. Country and Ordinal, or Population and quantitiative
        
		System.out.println("Final Vega Prompt: " + promptBuilder.toString());
        return promptBuilder.toString();
	}

	/**
     * Calls the model engine using LLMReactor and returns its response.
     */
    @SuppressWarnings("unchecked")
	private AskModelEngineResponse callLLM(String question, String context) {
        String modelId = (String) this.keyValue.get(this.keysToGet[1]);
        
        //TODO: Change this to prompt???
        Map<String, String> keyValue = new HashMap<>();
        keyValue.put(ENGINE_KEY, modelId);
        keyValue.put(COMMAND_KEY, question);
        keyValue.put(CONTEXT_KEY, context);
        keyValue.put(USE_HISTORY_KEY, "true");

        // Instantiate and prepare the reactor
        LLMReactor reactor = new LLMReactor();
        reactor.keyValue = keyValue;
//        reactor.insight = this.insight;
//        reactor.user = insight.getUser();

        // Execute the reactor
        NounMetadata result = reactor.execute();
        Map<String, Object> output = null;
//        if (result.getPixelDataType() == PixelDataType.MAP) { TODO: Check if output is a MAP type
            output = (Map<String, Object>) result.getValue();
//        }
        
        // Build and return the response object
        AskModelEngineResponse<?> response = AskModelEngineResponse.fromMap(output);
//        AskModelEngineResponse<?> response = AskModelEngineResponse.fromObject(result.getValue());
        
        System.out.println("OUTPUT: ");
        System.out.println((String) output.get("response"));
        System.out.println((String) output.get("messageId"));
        System.out.println((String) output.get("roomId"));
        System.out.println(output);
//        return new NounMetadata(response, PixelDataType.CONST_STRING);
        
//        if (output != null) {
//            // If the response is a list, convert or handle as required
//            Object resp = output.get("response");
//            if (resp instanceof List) {
//                // This example assumes response is a single step string
//            	@SuppressWarnings("unchecked")
//				List<String> jsonStrings = ((List<String>) resp);
//                response.setResponse(jsonStrings.get(0));
//            } else if (resp instanceof String) {
//                response.setResponse((String) resp);
//            } else {
//                System.err.println("Unexpected type for model response");
//            }
//            response.setMessageId((String) output.get("messageId"));
//            response.setRoomId((String) output.get("roomId"));
//        }
        return response;
    }
    
    /*
     * Determines whether the row data in a column are categorical, numerical, or temporal
     * */
    public String[] getColumnDataType(ITableDataFrame sourceFrame) {
    	 
		String[] headers = sourceFrame.getColumnHeaders();
		String[] headerDataTypes = new String[headers.length];
		
		for (int i = 0; i < headers.length; i++) {
			String header = headers[i];
			boolean isNumerical = false;
			boolean isCategorical = false;
			boolean isTemporal = false;
			Object[] rowData = sourceFrame.getColumn(header);
			for (int j = 0; j < rowData.length; j++) {
				System.out.println("Cell value: " + rowData[j]);
				if (rowData[j] instanceof Integer || rowData[j] instanceof Double) {
					isNumerical = true;
				} else if (rowData[j] instanceof SemossDate) {
					isTemporal=true;
				} else if (rowData[j] instanceof String || rowData[j] instanceof Boolean) {
					isCategorical = true;
				}
				
				if(j == rowData.length - 1) {
					if(isNumerical && isCategorical) {
						headerDataTypes[i]=CATEGORICAL;
						break;
					}
					else if(isTemporal && isCategorical) {
						headerDataTypes[i]=CATEGORICAL;
						break;
					}
					else if(isNumerical)
						headerDataTypes[i]=NUMERICAL;
					else if(isTemporal)
						headerDataTypes[i]=TEMPORAL;
					else if(isCategorical)
						headerDataTypes[i]=CATEGORICAL;
				}
 
			}
		}
		return headerDataTypes;
	}
	
	public String getName()
	{
		return "FrameToGraph";
	}
	
	@Override
	public String getReactorDescription() {
		return "Converts a Frame into a Vega Block JSON spec template. The data field will be empty or partially filled out";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.FRAME.getKey())) {
	        return "This is a required value that takes in the frame id.";
	    } else if(key.equals(ReactorKeysEnum.MODEL.getKey())) {
	        return "This is a required value that takes in the model id";
	    } else if(key.equals(USER_INPUT)) {
	    	return "This is an optional field to steer the model's graph generation behavior tailored to the user's prompt.";
	    }
	    return super.getDescriptionForKey(key);
	}

	private String getVegaBarChartTemplate () {
		return " \n" +
		"Bar Chart Template: {\n" +
			"  \"description\": \"placeholder\",\n" +
			"  \"width\": \"placeholder\",\n" +
			"  \"height\": \"placeholder\",\n" +
			"  \"padding\": \"placeholder\",\n" +
			"\n" +
			"  \"data\": [\n" +
			"    {\n" +
			"      \"name\": \"table\",\n" +
			"      \"values\": []\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"signals\": [\n" +
			"    {\n" +
			"      \"name\": \"tooltip\",\n" +
			"      \"value\": {},\n" +
			"      \"on\": [\n" +
			"        {\"events\": \"rect:pointerover\", \"update\": \"datum\"},\n" +
			"        {\"events\": \"rect:pointerout\",  \"update\": \"{}\"}\n" +
			"      ]\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"scales\": [\n" +
			"    {\n" +
			"      \"name\": \"xscale\",\n" +
			"      \"type\": \"band\",\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"placeholder\"},\n" +
			"      \"range\": \"width\",\n" +
			"      \"padding\": 0.05,\n" +
			"      \"round\": true\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"yscale\",\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"placeholder\"},\n" +
			"      \"nice\": true,\n" +
			"      \"range\": \"height\"\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"axes\": [\n" +
			"    { \"orient\": \"bottom\", \"scale\": \"xscale\" },\n" +
			"    { \"orient\": \"left\", \"scale\": \"yscale\" }\n" +
			"  ],\n" +
			"\n" +
			"  \"marks\": [\n" +
			"    {\n" +
			"      \"type\": \"rect\",\n" +
			"      \"from\": {\"data\":\"table\"},\n" +
			"      \"encode\": {\n" +
			"        \"enter\": {\n" +
			"          \"x\": {\"scale\": \"xscale\", \"field\": \"placeholder\"},\n" +
			"          \"width\": {\"scale\": \"xscale\", \"band\": 1},\n" +
			"          \"y\": {\"scale\": \"yscale\", \"field\": \"placeholder\"},\n" +
			"          \"y2\": {\"scale\": \"yscale\", \"value\": 0}\n" +
			"        },\n" +
			"        \"update\": {\n" +
			"          \"fill\": {\"value\": \"steelblue\"}\n" +
			"        },\n" +
			"        \"hover\": {\n" +
			"          \"fill\": {\"value\": \"red\"}\n" +
			"        }\n" +
			"      }\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"text\",\n" +
			"      \"encode\": {\n" +
			"        \"enter\": {\n" +
			"          \"align\": {\"value\": \"center\"},\n" +
			"          \"baseline\": {\"value\": \"bottom\"},\n" +
			"          \"fill\": {\"value\": \"#333\"}\n" +
			"        },\n" +
			"        \"update\": {\n" +
			"          \"x\": {\"scale\": \"xscale\", \"signal\": \"tooltip.category\", \"band\": 0.5},\n" +
			"          \"y\": {\"scale\": \"yscale\", \"signal\": \"tooltip.amount\", \"offset\": -2},\n" +
			"          \"text\": {\"signal\": \"tooltip.amount\"},\n" +
			"          \"fillOpacity\": [\n" +
			"            {\"test\": \"datum === tooltip\", \"value\": 0},\n" +
			"            {\"value\": 1}\n" +
			"          ]\n" +
			"        }\n" +
			"      }\n" +
			"    }\n" +
			"  ]\n" +
			"}";
	}

	private String getVegaLineChartTemplate () {
		return " \n" +
		"Line Chart Template: {\n" +
			"  \"description\": \"placeholder\",\n" +
			"  \"width\": \"placeholder\",\n" +
			"  \"height\": \"placeholder\",\n" +
			"  \"padding\": \"placeholder\",\n" +
			"\n" +
			"  \"signals\": [\n" +
			"    {\n" +
			"      \"name\": \"interpolate\",\n" +
			"      \"value\": \"linear\"\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"data\": [\n" +
			"    {\n" +
			"      \"name\": \"table\",\n" +
			"      \"values\": []\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"scales\": [\n" +
			"    {\n" +
			"      \"name\": \"x\",\n" +
			"      \"type\": \"point\",\n" +
			"      \"range\": \"width\",\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"placeholder\"}\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"y\",\n" +
			"      \"type\": \"linear\",\n" +
			"      \"range\": \"height\",\n" +
			"      \"nice\": true,\n" +
			"      \"zero\": true,\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"placeholder\"}\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"color\",\n" +
			"      \"type\": \"ordinal\",\n" +
			"      \"range\": \"category\",\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"c\"}\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"axes\": [\n" +
			"    {\"orient\": \"bottom\", \"scale\": \"x\"},\n" +
			"    {\"orient\": \"left\", \"scale\": \"y\"}\n" +
			"  ],\n" +
			"\n" +
			"  \"marks\": [\n" +
			"    {\n" +
			"      \"type\": \"group\",\n" +
			"      \"from\": {\n" +
			"        \"facet\": {\n" +
			"          \"name\": \"series\",\n" +
			"          \"data\": \"table\",\n" +
			"          \"groupby\": \"c\"\n" +
			"        }\n" +
			"      },\n" +
			"      \"marks\": [\n" +
			"        {\n" +
			"          \"type\": \"line\",\n" +
			"          \"from\": {\"data\": \"series\"},\n" +
			"          \"encode\": {\n" +
			"            \"enter\": {\n" +
			"              \"x\": {\"scale\": \"x\", \"field\": \"x\"},\n" +
			"              \"y\": {\"scale\": \"y\", \"field\": \"y\"},\n" +
			"              \"stroke\": {\"scale\": \"color\", \"field\": \"c\"},\n" +
			"              \"strokeWidth\": {\"value\": 2}\n" +
			"            },\n" +
			"            \"update\": {\n" +
			"              \"interpolate\": {\"signal\": \"interpolate\"},\n" +
			"              \"strokeOpacity\": {\"value\": 1}\n" +
			"            },\n" +
			"            \"hover\": {\n" +
			"              \"strokeOpacity\": {\"value\": 0.5}\n" +
			"            }\n" +
			"          }\n" +
			"        }\n" +
			"      ]\n" +
			"    }\n" +
			"  ]\n" +
			"}";
	}

	private String getVegaPieChartTemplate (){
		return " \n" +
		" Pie Chart Template: {\n" +
			"  \"description\": \"placeholder\",\n" +
			"  \"width\": \"placeholder\",\n" +
			"  \"height\": \"placeholder\",\n" +
			"  \"autosize\": \"none\",\n" +
			
			"  \"data\": [\n" +
			"    {\n" +
			"      \"name\": \"table\",\n" +
			"      \"transform\": [\n" +
			"        {\n" +
			"          \"type\": \"pie\",\n" +
			"          \"field\": \"placeholder\",\n" +
			"          \"startAngle\": {\"signal\": \"startAngle\"},\n" +
			"          \"endAngle\": {\"signal\": \"endAngle\"},\n" +
			"          \"sort\": {\"signal\": \"sort\"}\n" +
			"        }\n" +
			"      ]\n" +
			"      \"values\": [],\n" +
			
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"scales\": [\n" +
			"    {\n" +
			"      \"name\": \"color\",\n" +
			"      \"type\": \"ordinal\",\n" +
			"      \"domain\": {\"data\": \"table\", \"field\": \"id\"},\n" +
			"      \"range\": {\"scheme\": \"category20\"}\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  // DO NOT ALTER/REMOVE THESE SIGNALS\n" +
			"  \"signals\": [\n" +
			"    {\n" +
			"      \"name\": \"startAngle\", \"value\": 0\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"endAngle\", \"value\": 6.29\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"padAngle\", \"value\": 0\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"innerRadius\", \"value\": 0\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"cornerRadius\", \"value\": 0\n" +
			"    },\n" +
			"    {\n" +
			"      \"name\": \"sort\", \"value\": false\n" +
			"    }\n" +
			"  ],\n" +
			"\n" +
			"  \"marks\": [\n" +
			"    {\n" +
			"      \"type\": \"arc\",\n" +
			"      \"from\": {\"data\": \"table\"},\n" +
			"      \"encode\": {\n" +
			"        \"enter\": {\n" +
			"          \"fill\": {\"scale\": \"color\", \"field\": \"id\"},\n" +
			"          \"x\": {\"signal\": \"width / 2\"},\n" +
			"          \"y\": {\"signal\": \"height / 2\"}\n" +
			"        },\n" +
			"        \"update\": {\n" +
			"          \"startAngle\": {\"field\": \"startAngle\"},\n" +
			"          \"endAngle\": {\"field\": \"endAngle\"},\n" +
			"          \"padAngle\": {\"signal\": \"padAngle\"},\n" +
			"          \"innerRadius\": {\"signal\": \"innerRadius\"},\n" +
			"          \"outerRadius\": {\"signal\": \"width / 2\"},\n" +
			"          \"cornerRadius\": {\"signal\": \"cornerRadius\"}\n" +
			"        }\n" +
			"      }\n" +
			"    }\n" +
			"  ]\n" +
			"}";
	}
}
