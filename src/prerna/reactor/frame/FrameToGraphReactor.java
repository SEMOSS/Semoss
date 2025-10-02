
package prerna.reactor.frame;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import org.apache.logging.log4j.LogManager;
import prerna.util.Utility;
import prerna.sablecc2.om.execptions.SemossPixelException;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class FrameToGraphReactor extends AbstractRFrameReactor {
	
	private static final Logger logger = LogManager.getLogger(FrameToGraphReactor.class);
	
    private static final String CATEGORICAL="CATEGORICAL";
    private static final String NUMERICAL="NUMERICAL";
    private static final String TEMPORAL="TEMPORAL";
    private static final String USER_INPUT="userInput";
	private static final String DATA_STRING = "DATA_STRING";
	private static final int ROW_LIMIT = 75;
    
    public FrameToGraphReactor() {
		this.keysToGet = new String[] { 
			ReactorKeysEnum.FRAME.getKey(), 
			ReactorKeysEnum.MODEL.getKey(), 
			USER_INPUT, 
			DATA_STRING
		};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		///////// DATA PARSING ///////////
		GenRowStruct frameGrs = this.store.getNoun(ReactorKeysEnum.FRAME.getKey());
	    ITableDataFrame sourceFrame = getFrame();
//		fetchRows(sourceFrame, ROW_LIMIT);
	
		String userInput = this.keyValue.get(USER_INPUT);
		String encodedPayload = this.keyValue.get(DATA_STRING);
		String decodedPayload = encodedPayload;
   
		try {
			decodedPayload = URLDecoder.decode(encodedPayload, StandardCharsets.UTF_8.name());
			logger.info("The payload: " + decodedPayload);
			userInput = URLDecoder.decode(userInput, StandardCharsets.UTF_8.name());
			
		} catch (UnsupportedEncodingException e) {
			throw new SemossPixelException("Unable to decode the payload contents");
		}

	   String CONTEXT = "\"You are a data visualization expert. Map the headers in the provided frame payload to the values when adding the data to a single, complete and valid Vega-Lite v5 JSON specification using a template from the list of templates.";
       String PROMPT_TEMPLATE = "";

       ////////// USER INPUT ////////
       if (userInput != null) {
           CONTEXT += " \n"       
                    + "First, carefully consider the user's input:\n"
                    + "- If the user specifies a type of graph, use that graph type. If not specified, select from a template from the list that best fits the data.\n"
                    // + "- If the user specifies a color scheme, apply that color scheme. \n"
                    // + "- If the user requests showing or comparing only specific data columns, data types, or subsets of the data, ensure the final graph reflects exactly those selections.\n"
                    // + "- Interpret any specific user instructions carefully. For example, if the user says: Show me a graph comparing column A and column B only, or Plot a bar chart showing sales over time with blue tones, incorporate these instructions fully.\n"
                    + "- If the user asks to omit anything that is currently present in the template, ensure it is removed in the final output. Else do not remove the relevant data\n"
                    + "- Interpret any specific user instructions carefully."
                    + "- Do not ignore any part of the user's prompt regarding data filtering, graph type, color scheme, or other preferences.\n\n"
                    + " Most importantly ONLY return the Vega JSON. Do not include any text, explanation, markdown, or notes.\n"
                    ;

           PROMPT_TEMPLATE = "Here is the user input:\n\"" + userInput + "\"\n";
        }
       
        PROMPT_TEMPLATE += "Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data by selecting the graph template that best fits the data and user's needs.\"\n"
            + " Your task is to map the headers to the values section when adding the data to the JSON Spec. Additionally, add values to the placeholder values  "
            + " \n"
            // + "            Your output must include:\n"
            // + "            1. \"**The complete Vega-Lite JSON spec** only do not include any explanation, commentary, irregular quotation marks in data values, or code blocks.\"\n"
            // + "            2. \"Ensure the spec includes appropriate settings for:\"\n"
            // + "               \"- `mark` type (e.g., bar, line, point, area, etc.)\"\n"
            // + "               \"- `encoding` for x and y axes (use fields and types from the data)\"\n"
            // + "               \"- Optional: tooltips, color, and other enhancements to improve clarity\"\n"
            // + "            3. \"Use reasonable assumptions if the chart type is not specified.\"\n"
            + " \"Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.\"\n";
            // if (userInput != null) { 
            //  PROMPT += "5. \"The most meaningful and suprising patterns, insights, or anomalies possible in the dataset.\"\n";
            // }
        PROMPT_TEMPLATE += "Guidelines:\n"
            + "\"- Avoid complex transforms unless specified in the user's prompt\"\n"
            // + "\"- Choose the chart type from the appropriate chart family (temporal, categorical, hierarchical, relational, spatial) that best fits the data and user instructions.\"\n"
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
		String FINAL_PROMPT = "Frame payload: " 
				+ decodedPayload 
				+ PROMPT_TEMPLATE
//				+ buildVegaPrompt(sourceFrame)
				;
		
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
	 */
	private String buildVegaPrompt(ITableDataFrame sourceFrame) {
		StringBuilder promptBuilder = new StringBuilder();

		String[] headers = sourceFrame.getColumnHeaders();
		int frameSize = (int) sourceFrame.size(sourceFrame.getName());
		int rowCount = frameSize / headers.length;
		
		// Will exceed the token limit if it surpasses 75 rows 
		int sampleRows = Math.min(rowCount, ROW_LIMIT);

        // Begin JSON template
        // Fallback: if all columns are of one type, embed data under "values"
        promptBuilder.append("\n    \"values\": [\n");
        for (int r = 0; r < sampleRows; r++) {
            promptBuilder.append("      {");
			List<Map<String, Object>> rows = fetchRows(sourceFrame, sampleRows);
			Map<String, Object> row = rows.get(r);
            for (int i = 0; i < headers.length; i++) {
            	String header = headers[i];
				Object cell = row.get(header);
                
				if (cell == null) {
                    promptBuilder.append("\"").append(header).append("\": \"\"");
                } else if (cell instanceof SemossDate) {
                    promptBuilder.append("\"").append(header).append("\": \"").append(cell.toString()).append("\"");
                } else if (cell instanceof Number) {
                    Number num = (Number) cell;
                    promptBuilder.append("\"").append(header).append("\": ").append(num.doubleValue());
                } else {
                    // default: quote string-like values
                    promptBuilder.append("\"").append(header).append("\": \"").append(cell.toString().replace("\"","'")).append("\"");
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
        return promptBuilder.toString();
	}

	/**
     * Calls the model engine using LLMReactor and returns its response.
     */
    @SuppressWarnings("unchecked")
	private String callLLM(String context, String question) {
        String modelId = (String) this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
        

        HashMap<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("use_history", "true");
        paramMap.put("temperature", "0.2");
        
        logger.info("Start Model API Call at: " + LocalDateTime.now());
        IModelEngine modelEngine = Utility.getModel(modelId);
        AskModelEngineResponse<?> modelResponse = modelEngine.ask(question, context, this.insight, paramMap);
        String response = null;
        logger.info("End Model API Call at: " + LocalDateTime.now());
        
        if (modelResponse != null) {
            response = (String) modelResponse.getResponse();
        } else {
            throw new SemossPixelException("No valid response from LLM model call");
        }
        
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
//				System.out.println("Cell value: " + rowData[j]);
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

	private List<Map<String, Object>> fetchRows(ITableDataFrame sourceFrame, int limit) {
        List<Map<String, Object>> rows = new ArrayList<>();
        String[] headers = sourceFrame.getColumnHeaders();
        String sql = "SELECT * FROM " + sourceFrame.getName() + " LIMIT " + limit;
        try {
            Object result = null;
            // try native query first
            if (sourceFrame instanceof NativeFrame) {
                result = ((NativeFrame) sourceFrame).querySQL(sql);
            } else if (sourceFrame instanceof PandasFrame) {
                result = ((PandasFrame) sourceFrame).querySQL(sql);
            } else {
                // fall back to constructing rows from columns (best-effort)
                int rowCount = (int) sourceFrame.size(sourceFrame.getName());
                int sampleRows = Math.min(rowCount, limit);
                for (int r = 0; r < sampleRows; r++) {
                    Map<String, Object> row = new HashMap<>();
                    for (String h : headers) {
                        Object[] col = sourceFrame.getColumn(h);
                        row.put(h, (col != null && col.length > r) ? col[r] : null);
                    }
                    rows.add(row);
                }
                return rows;
            }

            // normalize result shapes
            if (result instanceof List) {
                List<?> list = (List<?>) result;
                if (!list.isEmpty()) {
                    Object first = list.get(0);
                    if (first instanceof Map) {
                        for (Object o : list) {
                            rows.add((Map<String, Object>) o);
                        }
                        return rows;
                    } else if (first instanceof Object[]) {
                        for (Object o : list) {
                            Object[] arr = (Object[]) o;
                            Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < headers.length && i < arr.length; i++) {
                                m.put(headers[i], arr[i]);
                            }
                            rows.add(m);
                        }
                        return rows;
                    }
                }
            } else if (result instanceof Object[][]) {
                Object[][] table = (Object[][]) result;
                for (int r = 0; r < Math.min(table.length, limit); r++) {
                    Map<String, Object> m = new HashMap<>();
                    for (int i = 0; i < headers.length && i < table[r].length; i++) {
                        m.put(headers[i], table[r][i]);
                    }
                    rows.add(m);
                }
                return rows;
            }
        } catch (SemossPixelException e) {
            throw new SemossPixelException("Error executing SQL on frame " + sourceFrame.getName() + ": " + e.getMessage());
        }
        return rows;
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
    
	public String getName() {
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
}
