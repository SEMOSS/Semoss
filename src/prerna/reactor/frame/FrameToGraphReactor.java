package prerna.reactor.frame;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

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

import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.Array;
import java.util.ArrayList;
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
    
    private static final String PROMPT = 
      "            \"You are a data visualization expert. Your task is to create a Vega-Lite JSON specification based on the raw data provided.\\n\\n\" +\n"
    + " \n"
    + "            \"Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data.\\n\\n\" +\n"
    + " \n"
    + "            \"Your output must include:\\n\" +\n"
    + "            \"1. **The complete Vega-Lite JSON spec** only — do not include any explanation, commentary, or code blocks.\\n\" +\n"
    + "            \"2. Ensure the spec includes appropriate settings for:\\n\" +\n"
    + "            \"   - `mark` type (e.g., bar, line, point, area, etc.)\\n\" +\n"
    + "            \"   - `encoding` for x and y axes (use fields and types from the data)\\n\" +\n"
    + "            \"   - Optional: tooltips, color, and other enhancements to improve clarity\\n\" +\n"
    + "            \"3. Use reasonable assumptions if the chart type is not specified.\\n\" +\n"
    + "            \"4. Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.\\n\\n\" +\n"
    + " \n"
    + "            \"Guidelines:\\n\" +\n"
    + "            \"- Avoid complex transforms unless required.\\n\" +\n"
    + "            \"- If the data is time-based, use a line or area chart with appropriate time formatting.\\n\" +\n"
    + "            \"- If comparing categories, use bar or grouped bar charts.\\n\" +\n"
    + "            \"- Add axis titles based on the field names.\\n\\n\" +\n"
    + " \n"
    + "            \"Only return the Vega JSON. Do not include any text, markdown, or notes.\\n\\n\" +\n"
    + " \n"
    + "            Here is the raw data:\\n\\n +\n"
    + " \n"
    + "            context;\n"
    + " \n"
    + " \n"
    + " \n"
    + "    } ";

	public FrameToGraphReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.MODEL.getKey(), "userInput" };
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
		String sql = "SELECT * FROM " + sourceFrame.getName();  
		
		// 5 types of Frames: Native, Python, Grid (SQL), R and Tinker Frames
		if (sourceFrame instanceof NativeFrame) {
		NativeFrame sourceNFrame = (NativeFrame) sourceFrame;
		Object result = sourceNFrame.querySQL(sql);
		System.out.println("NativeFrame query result: " + result);
		} else if (sourceFrame instanceof PandasFrame) {
			PandasFrame sourcePFrame = (PandasFrame) sourceFrame;
			Object result = sourcePFrame.querySQL(sql);
			System.out.println("PandasFrame query result: " + result);
		} else if (sourceFrame instanceof TinkerFrame) {
			// TODO: Implement Grid, R, and Tinker Frame instances
			System.out.println("TODO: Implement Grid and R instances");
		} else {
			System.err.println("Unsupported frame type for query execution");
		}

		///////// MODEL ///////////
        AskModelEngineResponse modelResponse = callLLM(
            "Generate graph steps from frame data: " + buildVegaPrompt(sourceFrame)
//            this.insight.getInsightFolder() // TODO: Unsure about this
        );
        
        if (modelResponse != null) {
            System.out.println("LLM Response: " + modelResponse.getResponse());
            // Process the response as needed (e.g., parse steps, adjust graph configuration, etc.)
        } else {
            System.err.println("No valid response from LLM model call");
        }
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
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
		int rowCount = (int) sourceFrame.size(sourceFrame.getName());
		
		// TODO: Make this work for frames greater than size 10. Test how many rows we can insert into prompt.
		int sampleRows = Math.min(rowCount, 10);
		StringBuilder sb = new StringBuilder();

		// Partition headers into numerical and categorical based on the first non-null cell in row 0
		List<String> numericalHeaders = new ArrayList<>();
        List<String> categoricalHeaders = new ArrayList<>();
        List<String> temporalHeaders = new ArrayList<>();
        
        
        String[] dataTypes = getColumnDataType(sourceFrame);
        
        for (int i = 0; i < dataTypes.length ; i++) {
            if (dataTypes[i].equals(NUMERICAL)) {
            	
//            }
//                	= sourceFrame.getColumn(currentHeader);
//                	for (int i = 0; i < ; i++) {
//                		
//                	}
                	
                numericalHeaders.add(headers[i]);
            } else if (dataTypes[i].equals(TEMPORAL)) {
            	temporalHeaders.add(headers[i]);
            } else {
                categoricalHeaders.add(headers[i]);
            }
        }

        // Begin JSON template
        promptBuilder.append("  \"Here is the raw data:\": {\n");
       
        
        
        System.out.println("Starting to embed data into Vega Prompt: " + promptBuilder.toString());

        /**
         * UNCOMMENT WHEN IS_NUMER_DATA function is working
         * 
        if (!categoricalHeaders.isEmpty()) {
            // Embed categorical data
        	promptBuilder.append("    \"categorical\": [\n");
            
            // Each row entry in [] should start with '{' and end with '}'
            for (int r = 0; r < sampleRows; r++) {
            	promptBuilder.append("      {");
                for (int i = 0; i < categoricalHeaders.size(); i++) {
                	// Get the corresponding numericRowData for the current header
                    String header = categoricalHeaders.get(i);
                    Double[] categoricalRowData = sourceFrame.getColumnAsNumeric(header);
                    
                    // "Fruit" : "Apple"
                    promptBuilder.append("\"").append(header).append("\": \"").append(categoricalRowData[r]).append("\"");
                    if (i < categoricalHeaders.size() - 1) {
                        promptBuilder.append(", ");
                    }
                }
                
                // Handles inserting ',' after each row entry
                promptBuilder.append("}");
                if (r < sampleRows - 1) {
                    promptBuilder.append(",\n");
                } else {
                    promptBuilder.append("\n");
                }
            }
            promptBuilder.append("    ],\n");
            
        }
            
        if (!numericalHeaders.isEmpty()) {
            // Embed numerical data
            promptBuilder.append("    \"numerical\": [\n");
            
            // Each row entry in [] should start with '{' and end with '}'
            for (int r = 0; r < sampleRows; r++) {
                promptBuilder.append("      {");
                for (int i = 0; i < numericalHeaders.size(); i++) {
                	// Get the corresponding numericRowData for the current header
                    String header = numericalHeaders.get(i);
                    Object[] numericRowData = sourceFrame.getColumn(header);
                    
                    // "Temperature" : 21
                    promptBuilder.append("\"").append(header).append("\": ").append(numericRowData[r]);
                    if (i < numericalHeaders.size() - 1) {
                        promptBuilder.append(", ");
                    }
                }
                promptBuilder.append("}");
                
                // Handles inserting ',' after each row entry
                if (r < sampleRows - 1) {
                    promptBuilder.append(",\n");
                } else {
                    promptBuilder.append("\n");
                }
            }
            promptBuilder.append("    ]\n");
        }
        
        */
        

        // Fallback: if all columns are of one type, embed data under "values"
        promptBuilder.append("    \"values\": [\n");
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
                    System.out.println("Cell value: " + cell);
                    
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
        
        promptBuilder.append("    ]\n");

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
	private AskModelEngineResponse callLLM(String question) {
        String modelId = (String) this.keyValue.get(this.keysToGet[1]);
        
        //TODO: Change this to prompt???
        String context = "You are a data visualization expert. Your task is to create a Vega-Lite JSON specification based on the raw data provided.\n\n" +
 
            "Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data.\n\n" +
 
            "Your output must include:\n" +
            "1. **The complete Vega-Lite JSON spec** only — do not include any explanation, commentary, or code blocks.\n" +
            "2. Ensure the spec includes appropriate settings for:\n" +
            "   - `mark` type (e.g., bar, line, point, area, etc.)\n" +
            "   - `encoding` for x and y axes (use fields and types from the data)\n" +
            "   - Optional: tooltips, color, and other enhancements to improve clarity\n" +
            "3. Use reasonable assumptions if the chart type is not specified.\n" +
            "4. Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.\n\n" +
 
            "Guidelines:\n" +
            "- Avoid complex transforms unless required.\n" +
            "- If the data is time-based, use a line or area chart with appropriate time formatting.\n" +
            "- If comparing categories, use bar or grouped bar charts.\n" +
            "- Add axis titles based on the field names.\n\n" +
 
            "Only return the Vega JSON. Do not include any text, markdown, or notes.\n\n";

        Map<String, String> keyValue = new HashMap<>();
        keyValue.put(ENGINE_KEY, modelId);
        keyValue.put(COMMAND_KEY, question);
        if (context != null) {
            keyValue.put(CONTEXT_KEY, context);
        }
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
     * 
     * 
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
		System.out.print("HELLO");
		return headerDataTypes;
	}
	
	public String getName()
	{
		return "FrameToGraph";
	}
	
//	public String getReactorDescription() {
//		
//	}
	
//	public String getDescriptionForKey() {
//		
//	}
}
