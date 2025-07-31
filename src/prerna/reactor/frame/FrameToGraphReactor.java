package prerna.reactor.frame;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
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
import java.util.ArrayList;
import prerna.om.Insight;

import prerna.engine.impl.model.responses.AskModelEngineResponse;

public class FrameToGraphReactor extends AbstractRFrameReactor {

	private static final String ENGINE_KEY = "ENGINE";
    private static final String COMMAND_KEY = "COMMAND";
    private static final String CONTEXT_KEY = "CONTEXT";
    private static final String USE_HISTORY_KEY = "USE_HISTORY";
    
    private static final String PROMPT = 
    "Here is the metadata extracted from a data frame: • Categorical fields: [Country, City] • Numerical fields: [Population, GDP]." +
    "Based on this, determine the best Vega-Lite mark (e.g. bar, line, point, etc.) and encoding for a chart. Choose the x and y fields, indicate their types, and output a complete valid Vega-Lite JSON specification." + 
    "I only want valid JSON with no extra commentary.";

	public FrameToGraphReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.MODEL.getKey(), "userInput" };
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// Check required R packages and libraries
		String[] packages = new String[] {"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		
		///////// DATA PARSING ///////////
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
	    ITableDataFrame sourceFrame = getFrame();
		String sql = "SELECT * FROM " + sourceFrame.getName();  
		
		String[] pckgs = new String[] {"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		
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
		Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("use_history", "true");
        
        // Using the new helper method to call the model
//        AskModelEngineResponse modelResponse = callLLM(
//            "Generate graph steps from frame data: " + buildVegaPrompt(sourceFrame),
//            "FrameToGraph context",
//            this.insight.getInsightFolder(), // TODO: Unsure about this
//            modelParams
//        );
        
//        if (modelResponse != null) {
//            System.out.println("LLM Response: " + modelResponse.getResponse());
//            // Process the response as needed (e.g., parse steps, adjust graph configuration, etc.)
//        } else {
//            System.err.println("No valid response from LLM model call");
//        }
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}
	
	protected ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		throw new IllegalArgumentException("Must define the frame frame");
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
        String[] columnHeaders = sourceFrame.getColumnHeaders();
        
        for (int i = 0; i < columnHeaders.length ; i++) {
        	String currentHeader = columnHeaders[i];
            if (sourceFrame.isNumeric(currentHeader)) {
//                	= sourceFrame.getColumn(currentHeader);
//                	for (int i = 0; i < ; i++) {
//                		
//                	}
                numericalHeaders.add(currentHeader);
            } else {
                categoricalHeaders.add(currentHeader);
            }
        }

        // Begin JSON template
        sb.append("{\n");
        sb.append("  \"$schema\": \"https://vega.github.io/schema/vega-lite/v5.json\",\n");
        sb.append("  \"description\": \"A chart generated from frame data\",\n");
        sb.append("  \"data\": {\n");

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
        

        // Fallback: if all columns are of one type, embed data under "values"
        promptBuilder.append("    \"values\": [\n");
        for (int r = 0; r < sampleRows; r++) {
            promptBuilder.append("      {");
            for (int i = 0; i < headers.length; i++) {
            	String header = numericalHeaders.get(i);
                Object[] rowData = sourceFrame.getColumn(header);
                Object cell = rowData[i];
                
                // If we have only numerical data, omit quotes. Otherwise, include quotes.
                if (numericalHeaders.isEmpty() || !(cell instanceof Number)) {
                    promptBuilder.append("\"").append(header).append("\": \"").append((String) cell).append("\"");
                } else {
                    promptBuilder.append("\"").append(header).append("\": ").append((Double) cell);
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
        
		System.out.println("Vega Prompt: " + promptBuilder.toString());
        return sb.toString();
	}

	/**
     * Calls the model engine using LLMReactor and returns its response.
     */
    private AskModelEngineResponse callLLM(String question, String context, Insight insight, Map<String, Object> parameters) {
        Map<String, String> keyValue = new HashMap<>();
        keyValue.put(ENGINE_KEY, this.getType().name());
        keyValue.put(COMMAND_KEY, question);
        if (context != null) {
            keyValue.put(CONTEXT_KEY, context);
        }
        keyValue.put(USE_HISTORY_KEY, parameters.getOrDefault("use_history", "true").toString());

        // Instantiate and prepare the reactor
        LLMReactor reactor = new LLMReactor();
        reactor.keyValue = keyValue;
        reactor.insight = insight;
        reactor.user = insight.getUser();

        // Execute the reactor
        NounMetadata result = reactor.execute();
        Map<String, Object> output = null;
        if (result.getPixelDataType() == PixelDataType.MAP) {
            output = (Map<String, Object>) result.getValue();
        }
        
        // Build and return the response object
        AskModelEngineResponse response = new AskModelEngineResponse();
        if (output != null) {
            // If the response is a list, convert or handle as required
            Object resp = output.get("response");
            if (resp instanceof List) {
                // This example assumes response is a single step string
                response.setResponse(((List<String>) resp).get(0));
            } else if (resp instanceof String) {
                response.setResponse((String) resp);
            } else {
                System.err.println("Unexpected type for model response");
            }
            response.setMessageId((String) output.get("messageId"));
            response.setRoomId((String) output.get("roomId"));
        }
        return response;
    }
    
    private isNumericData() {
    	
    }
	
	public String getName()
	{
		return "FrameToGraph";
	}
}
