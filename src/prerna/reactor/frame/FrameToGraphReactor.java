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

public class FrameToGraphReactor extends AbstractRFrameReactor {

	private static final String ENGINE_KEY = "ENGINE";
    private static final String COMMAND_KEY = "COMMAND";
    private static final String CONTEXT_KEY = "CONTEXT";
    private static final String USE_HISTORY_KEY = "USE_HISTORY";

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
		
		String sql = "SELECT * FROM " + sourceFrame.getName();  

		promptData.append(promptDataHeader);
		promptData.append(promptDataBody);
		
		String[] packages = new String[] {"igraph"};
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
			System.out.println("TODO: Implement Grid, R, and Tinker Frame instances");
		} else {
			System.err.println("Unsupported frame type for query execution");
		}

		///////// MODEL ///////////
		Map<String, Object> modelParams = new HashMap<>();
        modelParams.put("use_history", "true");
        Insight currentInsight = getCurrentInsight(); // Implement retrieval as needed
        
        // Using the new helper method to call the model
        AskModelEngineResponse modelResponse = callLLM(
            "Generate graph steps from frame data: " + promptData.toString(),
            "FrameToGraph context",
            this.store.getInsight();,
            modelParams
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
		
		// Start a simple Vega JSON template
		promptBuilder.append("{\n");
		promptBuilder.append("  \"$schema\": \"https://vega.github.io/schema/vega-lite/v5.json\",\n"); // Change version as needed
		promptBuilder.append("  \"description\": \"A chart generated from frame data\",\n");
		
		// Embed data from the frame (sample only a few rows)
		promptBuilder.append("  \"data\": {\n");
		promptBuilder.append("    \"values\": [\n");
		
		int rowCount = sourceFrame.getRowCount();
		int sampleRows = Math.min(rowCount, 3);
		String[] headers = sourceFrame.getColumnHeaders();
		
		for (int r = 0; r < sampleRows; r++) {
			promptBuilder.append("      {");
			for (int h = 0; h < headers.length; h++) {
				Object cell = sourceFrame.getCellValue(r, headers[h]);
				promptBuilder.append("\"").append(headers[h]).append("\": \"").append(cell).append("\"");
				if (h < headers.length - 1) {
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
		promptBuilder.append("  },\n");
		
		// Add encoding and mark settings (you can modify this template as needed)
		promptBuilder.append("  \"mark\": \"bar\",\n");
		promptBuilder.append("  \"encoding\": {\n");
		promptBuilder.append("    \"x\": { \"field\": \"").append(headers[0]).append("\", \"type\": \"ordinal\" },\n");
		promptBuilder.append("    \"y\": { \"field\": \"").append(headers[1]).append("\", \"type\": \"quantitative\" }\n");
		promptBuilder.append("  }\n");
		promptBuilder.append("}");

		System.out.println("PROMPT: " promptBuilder.toString());
		
		return promptBuilder.toString();
	}

	/**
     * Calls the model engine using LLMReactor and returns its response.
     */
    private AskModelEngineResponse callLLM(String question, String context, Insight insight, Map<String, Object> parameters) {
        Map<String, String> keyValue = new HashMap<>();
        keyValue.put(ENGINE_KEY, this.getModelType().name());
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
	
	public String getName()
	{
		return "FrameToGraph";
	}
}
