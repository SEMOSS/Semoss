
package prerna.reactor.frame;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;
import java.util.HashMap;

public class FrameToGraphReactor extends AbstractReactor {
    
    private static final Logger logger = LogManager.getLogger(FrameToGraphReactor.class);
    
    private static final String CATEGORICAL="CATEGORICAL";
    private static final String NUMERICAL="NUMERICAL";
    private static final String TEMPORAL="TEMPORAL";
    private static final String USER_INPUT="userInput";
    private static final float TEMPERATURE = 0.2f;
    private static final String USE_HISTORY = "true";
    
    public FrameToGraphReactor() {
        this.keysToGet = new String[] { 
            ReactorKeysEnum.FRAME.getKey(), 
            ReactorKeysEnum.MODEL.getKey(), 
            USER_INPUT, 
        };
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        ///////// DATA PARSING ///////////
        ITableDataFrame sourceFrame = getSelectedFrame();
        String userInput = this.keyValue.get(USER_INPUT);
        String CONTEXT = "\"You are a data visualization expert. Map the headers in the provided frame payload to the values when adding the data to a single, complete and valid Vega-Lite v5 JSON specification using a template from the list of templates.";
        String PROMPT_TEMPLATE = "";
       ////////// USER INPUT ////////
       if (userInput != null) {
           CONTEXT += " \n"       
                    + "First, carefully consider the user's input:\n"
                    + "- If the user specifies a type of graph, use that graph type. If not specified, select from a template from the list that best fits the data.\n"
                    + "- If the user asks to omit anything that is currently present in the template, ensure it is removed in the final output. Else do not remove the relevant data\n"
                    + "- Interpret any specific user instructions carefully."
                    + "- When generating the JSON spec, always include the \"data\": { \"values\": [] } field and ensure the values array is empty. Do not populate it with any data."
                    + "- Do not ignore any part of the user's prompt regarding data filtering, graph type, color scheme, or other preferences.\n"
                    + " Most importantly ONLY return the Vega JSON. Do not include any text, explanation, markdown, or notes.\n"
                    ;
           PROMPT_TEMPLATE = "Here is the user input:\n\"" + userInput + "\"\n";
        }
       
        PROMPT_TEMPLATE += "Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data by selecting the graph template that best fits the data and user's needs.\"\n"
            + " Your task is to map the headers to the values section when adding the data to the JSON Spec. Additionally, add values to the placeholder values  "
            + " \n"
            + " \"Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.\"\n";
        PROMPT_TEMPLATE += "Guidelines:\n"
            + "\"- Avoid complex transforms unless specified in the user's prompt\"\n"
            + "\"- Add axis titles based on the field names.\"\n"
            + " \n"
            + "Here are the templates you can choose from: \n"
            + getVegaBarChartTemplate() + "\n"
            + getVegaLineChartTemplate() + "\n"
            + getVegaPieChartTemplate() + "\n"
            + " Here also some template specific guidelines to follow: \n"
            + " For PieCharts: do NOT remove the transform nor the signals section."
            ;
        ///////// MODEL ///////////
        String FINAL_PROMPT = PROMPT_TEMPLATE + buildMetadataPromptSection(sourceFrame);
        
        String modelResponse = callLLM(
            CONTEXT, FINAL_PROMPT
        );
        
        if (modelResponse != null) {
            logger.info("LLM Response: " + modelResponse);
        } else {
            throw new SemossPixelException("No valid response from model API call");
        }
        
        return new NounMetadata(modelResponse, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    }
    
    /**
     * Returns the selected frame by frame ID, or defaults to the insight's primary frame.
     * If neither is available, throws a SemossPixelException.
     *
     * @return the ITableDataFrame to visualize
     * @throws SemossPixelException if no frame available
     */
    private ITableDataFrame getSelectedFrame() {
        String selectedFrame = this.keyValue.get(PixelDataType.FRAME.getKey());
        
        // Get the first frame with the specified frame name
        if (selectedFrame == null || selectedFrame.trim().isEmpty()) {
            logger.warn("Frame ID '" + selectedFrame + "' not found, falling back to default frame.");
        } else {
            VarStore varStore = this.insight.getVarStore();
            for(String k : varStore.getKeys()) {
                NounMetadata noun = varStore.get(k);
                if(noun.getNounType() == PixelDataType.FRAME) {
                    ITableDataFrame frame = (ITableDataFrame) noun.getValue();
                    String frameName = frame.getOriginalName();
                    if (frameName.equals(selectedFrame)) {
                        return frame;
                    };
                }
            }   
        }
        
        // else, grab the default frame from the insight
        // put this into the noun store
        // so that we can pull it for other pipeline
        ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
        if (defaultFrame != null) {
            this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
            logger.info("Returning default frame from insight.");
            return defaultFrame;
        }
        throw new SemossPixelException("Frame not found for key: " + selectedFrame + " and no default frame available in insight.");
    }
    /**
     * Builds the JSON metadata section describing a data frame's columns and their types.
     * This section is appended into the LLM prompt and is used to inform downstream
     * visualization or data-processing components about the available column structure.
     *
     * @param sourceFrame The data frame whose column headers and types are to be described.
     * @return A formatted JSON string listing each column's name and its inferred type
     *         ("CATEGORICAL", "NUMERICAL", or "TEMPORAL").
     *
     * Example output:
     *   "columns": [
     *     { "name": "Month", "type": "CATEGORICAL" },
     *     { "name": "Revenue", "type": "NUMERICAL" }
     *   ]
     */
    private String buildMetadataPromptSection(ITableDataFrame sourceFrame) {
        StringBuilder promptBuilder = new StringBuilder();
        String[] headers = sourceFrame.getColumnHeaders();
        String[] dataTypes = getColumnDataType(sourceFrame);
        
        promptBuilder.append("    \"columns\": [\n");
        for (int i = 0; i < headers.length; i++) {
            promptBuilder.append("      {\n");
            promptBuilder.append("        \"name\": \"").append(headers[i]).append("\",\n");
            promptBuilder.append("        \"type\": \"").append(dataTypes[i]).append("\"\n");
            promptBuilder.append("      }");
            if (i < headers.length - 1) {
                promptBuilder.append(",");
            }
            promptBuilder.append("\n");
        }
        promptBuilder.append("    ]");
        return promptBuilder.toString();
    }
    /**
     * Invokes the model engine with a specific LLM context and prompt.
     * This method prepares additional model parameters, sends the constructed prompts
     * to the model via IModelEngine, and retrieves the string output for use in the reactor.
     *
     * @param context  The description or guiding context to be passed to the LLM.
     * @param question The formatted question or main prompt for the LLM.
     * @return The string response generated by the large language model.
     * @throws SemossPixelException if the model engine response is null or otherwise invalid.
     */
    @SuppressWarnings("unchecked")
    private String callLLM(String context, String question) {
        String modelId = (String) this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
        
        HashMap<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("use_history", USE_HISTORY);
        paramMap.put("temperature", TEMPERATURE);
        
        IModelEngine modelEngine = Utility.getModel(modelId);
        AskModelEngineResponse<?> modelResponse = modelEngine.ask(question, context, this.insight, paramMap);
        String response = null;
        
        if (modelResponse != null) {
            response = (String) modelResponse.getResponse();
        } else {
            throw new SemossPixelException("No valid response from LLM model call");
        }
        
        return response;
    }
    
    /**
     * Infers the data type of each column in the given data frame by sampling its values.
     * Returns an array mapping each column name to one of three types:
     * "CATEGORICAL", "NUMERICAL", or "TEMPORAL". Logic is as follows:
     * - Columns containing only numbers (Integer/Double) are tagged as "NUMERICAL"
     * - Columns with only SemossDate instances are "TEMPORAL"
     * - All others (including strings, booleans, or mixed) are "CATEGORICAL"
     * If mixed numerical/categorical or temporal/categorical data are found,
     * "CATEGORICAL" is favored to prevent accidental quantification.
     *
     * @param sourceFrame The data frame whose columns should be typed.
     * @return String[] Array of data type identifiers for each column (order matches headers).
     */
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
            "      \"name\": \"placeholder\",\n" +
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
            "      \"name\": \"placeholder\",\n" +
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
        "\n" +
        "  \"data\": [\n" +
        "    {\n" +
        "      \"name\": \"placeholder\",\n" +
        "      \"values\": [],\n" +
        "      \"transform\": [\n" +
        "        {\n" +
        "          \"type\": \"pie\",\n" +
        "          \"field\": \"placeholder\",\n" +
        "          \"startAngle\": {\"signal\": \"startAngle\"},\n" +
        "          \"endAngle\": {\"signal\": \"endAngle\"},\n" +
        "          \"sort\": {\"signal\": \"sort\"}\n" +
        "        }\n" +
        "      ]\n" +
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
        "    { \"name\": \"startAngle\", \"value\": 0 },\n" +
        "    { \"name\": \"endAngle\", \"value\": 6.29 },\n" +
        "    { \"name\": \"padAngle\", \"value\": 0 },\n" +
        "    { \"name\": \"innerRadius\", \"value\": 0 },\n" +
        "    { \"name\": \"cornerRadius\", \"value\": 0 },\n" +
        "    { \"name\": \"sort\", \"value\": false }\n" +
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
    
    public String getName() {
        return "FrameToGraph";
    }
    
    @Override
    public String getReactorDescription() {
        return "Converts a Frame into a Vega Block JSON spec template. The data field will be empty but sometimes partially filled out";
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
}
