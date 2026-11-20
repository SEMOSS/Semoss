/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.frame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ConvertFrameToVegaVizReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(ConvertFrameToVegaVizReactor.class);

	private static final String CATEGORICAL = "CATEGORICAL";
	private static final String NUMERICAL = "NUMERICAL";
	private static final String TEMPORAL = "TEMPORAL";
	private static final String USER_INPUT = "userInput";
	private static final float TEMPERATURE = 0.2f;
	private static final String USE_HISTORY = "true";

	public ConvertFrameToVegaVizReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.MODEL.getKey(), USER_INPUT, };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		///////// DATA PARSING ///////////
		ITableDataFrame sourceFrame = getSelectedFrame();
		String userInput = this.keyValue.get(USER_INPUT);
		String CONTEXT = "\"You are a data visualization expert. Map the headers in the provided frame payload to the values when adding the data to a single, complete and valid Vega-Lite v5 JSON specification using a template from the list of templates.";
		String PROMPT_TEMPLATE = "";

		String userPrompt = buildUserPrompt(userInput);
		String instructions = buildRequestInstructions(getVegaBarChartTemplate(), getVegaLineChartTemplate(),
				getVegaPieChartTemplate());

		PROMPT_TEMPLATE = userPrompt + instructions;

		String FINAL_PROMPT = PROMPT_TEMPLATE + buildMetadataPromptSection(sourceFrame);

		String modelResponse = callLLM(CONTEXT, FINAL_PROMPT);

		if (modelResponse != null) {
			classLogger.info("LLM Response: " + modelResponse);
		} else {
			throw new SemossPixelException("No valid response from model API call");
		}

		return new NounMetadata(modelResponse, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	private String buildUserPrompt(String userInput) {
		if (userInput == null || userInput.isEmpty()) {
			return "";
		}
		return """
				First, carefully consider the user's input:
				- If the user specifies a type of graph, use that graph type. If not specified, select from a template from the list that best fits the data.
				- If the user asks to omit anything that is currently present in the template, ensure it is removed in the final output. Else do not remove the relevant data.
				- Interpret any specific user instructions carefully.
				- When generating the JSON spec, always include the "data": { "values": [] } field and ensure the values array is empty. Do not populate it with any data.
				- Do not ignore any part of the user's prompt regarding data filtering, graph type, color scheme, or other preferences.
				Most importantly ONLY return the Vega JSON. Do not include any text, explanation, markdown, or notes.

				Here is the user input:
				"%s"
				"""
				.formatted(userInput);
	}

	private String buildRequestInstructions(String barTemplate, String lineTemplate, String pieTemplate) {
		return """
				Please generate a valid Vega-Lite chart specification in JSON format that accurately and clearly visualizes the given data by selecting the graph template that best fits the data and user's needs.
				Your task is to map the headers to the values section when adding the data to the JSON Spec. Additionally, add values to the placeholder values.
				Ensure the JSON is valid and can be used directly with a Vega-Lite renderer.
				Guidelines:
				- Avoid complex transforms unless specified in the user's prompt.
				- Add axis titles based on the field names.

				Here are the templates you can choose from:
				%s
				%s
				%s

				Here also some template specific guidelines to follow:
				For PieCharts: do NOT remove the transform nor the signals section.
				"""
				.formatted(barTemplate, lineTemplate, pieTemplate);
	}

	/**
	 * Returns the selected frame by frame ID, or defaults to the insight's primary
	 * frame. If neither is available, throws a SemossPixelException.
	 *
	 * @return the ITableDataFrame to visualize
	 * @throws SemossPixelException if no frame available
	 */
	private ITableDataFrame getSelectedFrame() {
		String selectedFrame = this.keyValue.get(PixelDataType.FRAME.getKey());

		// Get the first frame with the specified frame name
		if (selectedFrame == null || selectedFrame.trim().isEmpty()) {
			classLogger.warn("Frame ID '" + selectedFrame + "' not found, falling back to default frame.");
		} else {
			VarStore varStore = this.insight.getVarStore();
			for (String k : varStore.getKeys()) {
				NounMetadata noun = varStore.get(k);
				if (noun.getNounType() == PixelDataType.FRAME) {
					ITableDataFrame frame = (ITableDataFrame) noun.getValue();
					String frameName = frame.getOriginalName();
					if (frameName.equals(selectedFrame)) {
						return frame;
					}
					;
				}
			}
		}

		// else, grab the default frame from the insight
		// put this into the noun store
		// so that we can pull it for other pipeline
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		if (defaultFrame != null) {
			this.store.makeNoun(ReactorKeysEnum.FRAME.getKey())
					.add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
			classLogger.info("Returning default frame from insight.");
			return defaultFrame;
		}
		throw new SemossPixelException(
				"Frame not found for key: " + selectedFrame + " and no default frame available in insight.");
	}

	/**
	 * Builds the JSON metadata section describing a data frame's columns and their
	 * types. This section is appended into the LLM prompt and is used to inform
	 * downstream visualization or data-processing components about the available
	 * column structure.
	 *
	 * @param sourceFrame The data frame whose column headers and types are to be
	 *                    described.
	 * @return A formatted JSON string listing each column's name and its inferred
	 *         type ("CATEGORICAL", "NUMERICAL", or "TEMPORAL").
	 *
	 *         Example output: "columns": [ { "name": "Month", "type": "CATEGORICAL"
	 *         }, { "name": "Revenue", "type": "NUMERICAL" } ]
	 */
	private String buildMetadataPromptSection(ITableDataFrame sourceFrame) {
		String[] headers = sourceFrame.getColumnHeaders();
		String[] dataTypes = getColumnDataType(sourceFrame);

		List<String> columns = new ArrayList<>();
		for (int i = 0; i < headers.length; i++) {
			columns.add(String.format("      {\n        \"name\": \"%s\",\n        \"type\": \"%s\"\n      }",
					headers[i], dataTypes[i]));
		}

		return "    \"columns\": [\n" + String.join(",\n", columns) + "\n    ]";
	}

	/**
	 * Invokes the model engine with a specific LLM context and prompt. This method
	 * prepares additional model parameters, sends the constructed prompts to the
	 * model via IModelEngine, and retrieves the string output for use in the
	 * reactor.
	 *
	 * @param context  The description or guiding context to be passed to the LLM.
	 * @param question The formatted question or main prompt for the LLM.
	 * @return The string response generated by the large language model.
	 * @throws SemossPixelException if the model engine response is null or
	 *                              otherwise invalid.
	 */
	@SuppressWarnings("unchecked")
	private String callLLM(String context, String question) {

		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		if (modelId == null || modelId.trim().isEmpty()) {
			throw new SemossPixelException("Model id is required");
		}

		if (!AbstractSecurityUtils.ignoreDatabase(modelId) && !AbstractSecurityUtils.containsEngineId(modelId)) {
			throw new SemossPixelException("Model not registered in security DB: " + modelId);
		}
		if (AbstractSecurityUtils.adminOnlyEngineAddAccess(modelId)) {
			throw new SemossPixelException("Insufficient permissions to use model: " + modelId);
		}

		//////// TEMPORARY ROOM SETUP ////////
		Room room = new Room();
		room.setId(this.keyValue.get(PixelDataType.FRAME.getKey()).toString()); // Use frame id for the room id
		room.setInsight(this.insight);
		// room.setModelId((String) this.keyValue.get(ReactorKeysEnum.MODEL.getKey()));
		// // try this?
		HashMap<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("use_history", USE_HISTORY);
		paramMap.put("temperature", TEMPERATURE);

		IModelEngine modelEngine = Utility.getModel(modelId);
		InputMessage msg = InputMessage.builder(room).withSystemPrompt(context).withText(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();
		AskModelEngineResponse<?> modelResponse = room.ask(msg, modelEngine).getModelEngineResponse();
		String response = null;

		if (modelResponse != null) {
			response = (String) modelResponse.getResponse();
		} else {
			throw new SemossPixelException("No valid response from LLM model call");
		}

		return response;
	}

	/**
	 * Infers the data type of each column in the given data frame by sampling its
	 * values. Returns an array mapping each column name to one of three types:
	 * "CATEGORICAL", "NUMERICAL", or "TEMPORAL". Logic is as follows: - Columns
	 * containing only numbers (Integer/Double) are tagged as "NUMERICAL" - Columns
	 * with only SemossDate instances are "TEMPORAL" - All others (including
	 * strings, booleans, or mixed) are "CATEGORICAL" If mixed numerical/categorical
	 * or temporal/categorical data are found, "CATEGORICAL" is favored to prevent
	 * accidental quantification.
	 *
	 * @param sourceFrame The data frame whose columns should be typed.
	 * @return String[] Array of data type identifiers for each column (order
	 *         matches headers).
	 */
	public String[] getColumnDataType(ITableDataFrame sourceFrame) {
		String[] headers = sourceFrame.getColumnHeaders();
		String[] headerDataTypes = new String[headers.length];

		for (int i = 0; i < headers.length; i++) {
			Object[] columnData = sourceFrame.getColumn(headers[i]);
			Set<String> detectedTypes = new HashSet<>();
			for (Object cell : columnData) {
				detectedTypes.add(detectType(cell));
			}
			// Decide column type by precedence: CATEGORICAL > NUMERICAL > TEMPORAL
			if (detectedTypes.contains(CATEGORICAL)) {
				headerDataTypes[i] = CATEGORICAL;
			} else if (detectedTypes.contains(NUMERICAL)) {
				headerDataTypes[i] = NUMERICAL;
			} else {
				headerDataTypes[i] = TEMPORAL;
			}
		}
		return headerDataTypes;
	}

	private String detectType(Object cell) {
		if (cell instanceof Integer || cell instanceof Double) {
			return NUMERICAL;
		}
		if (cell instanceof SemossDate) {
			return TEMPORAL;
		}
		return CATEGORICAL;
	}

	private String getVegaBarChartTemplate() {
		return """
				{
				  "description": "placeholder",
				  "width": "placeholder",
				  "height": "placeholder",
				  "padding": "placeholder",
				  "data": [
				    {
				      "name": "placeholder",
				      "values": []
				    }
				  ],
				  "signals": [
				    {
				      "name": "tooltip",
				      "value": {},
				      "on": [
				        {"events": "rect:pointerover", "update": "datum"},
				        {"events": "rect:pointerout",  "update": "{}"}
				      ]
				    }
				  ],
				  "scales": [
				    {
				      "name": "xscale",
				      "type": "band",
				      "domain": {"data": "table", "field": "placeholder"},
				      "range": "width",
				      "padding": 0.05,
				      "round": true
				    },
				    {
				      "name": "yscale",
				      "domain": {"data": "table", "field": "placeholder"},
				      "nice": true,
				      "range": "height"
				    }
				  ],
				  "axes": [
				    { "orient": "bottom", "scale": "xscale" },
				    { "orient": "left", "scale": "yscale" }
				  ],
				  "marks": [
				    {
				      "type": "rect",
				      "from": {"data":"table"},
				      "encode": {
				        "enter": {
				          "x": {"scale": "xscale", "field": "placeholder"},
				          "width": {"scale": "xscale", "band": 1},
				          "y": {"scale": "yscale", "field": "placeholder"},
				          "y2": {"scale": "yscale", "value": 0}
				        },
				        "update": {
				          "fill": {"value": "steelblue"}
				        },
				        "hover": {
				          "fill": {"value": "red"}
				        }
				      }
				    },
				    {
				      "type": "text",
				      "encode": {
				        "enter": {
				          "align": {"value": "center"},
				          "baseline": {"value": "bottom"},
				          "fill": {"value": "#333"}
				        },
				        "update": {
				          "x": {"scale": "xscale", "signal": "tooltip.category", "band": 0.5},
				          "y": {"scale": "yscale", "signal": "tooltip.amount", "offset": -2},
				          "text": {"signal": "tooltip.amount"},
				          "fillOpacity": [
				            {"test": "datum === tooltip", "value": 0},
				            {"value": 1}
				          ]
				        }
				      }
				    }
				  ]
				}
				""";
	}

	private String getVegaLineChartTemplate() {
		return """
				{
				  "description": "placeholder",
				  "width": "placeholder",
				  "height": "placeholder",
				  "padding": "placeholder",
				  "signals": [
				    {
				      "name": "interpolate",
				      "value": "linear"
				    }
				  ],
				  "data": [
				    {
				      "name": "placeholder",
				      "values": []
				    }
				  ],
				  "scales": [
				    {
				      "name": "x",
				      "type": "point",
				      "range": "width",
				      "domain": {"data": "table", "field": "placeholder"}
				    },
				    {
				      "name": "y",
				      "type": "linear",
				      "range": "height",
				      "nice": true,
				      "zero": true,
				      "domain": {"data": "table", "field": "placeholder"}
				    },
				    {
				      "name": "color",
				      "type": "ordinal",
				      "range": "category",
				      "domain": {"data": "table", "field": "c"}
				    }
				  ],
				  "axes": [
				    {"orient": "bottom", "scale": "x"},
				    {"orient": "left", "scale": "y"}
				  ],
				  "marks": [
				    {
				      "type": "group",
				      "from": {
				        "facet": {
				          "name": "series",
				          "data": "table",
				          "groupby": "c"
				        }
				      },
				      "marks": [
				        {
				          "type": "line",
				          "from": {"data": "series"},
				          "encode": {
				            "enter": {
				              "x": {"scale": "x", "field": "x"},
				              "y": {"scale": "y", "field": "y"},
				              "stroke": {"scale": "color", "field": "c"},
				              "strokeWidth": {"value": 2}
				            },
				            "update": {
				              "interpolate": {"signal": "interpolate"},
				              "strokeOpacity": {"value": 1}
				            },
				            "hover": {
				              "strokeOpacity": {"value": 0.5}
				            }
				          }
				        }
				      ]
				    }
				  ]
				}
				""";
	}

	private String getVegaPieChartTemplate() {
		return """
				      {
				  "description": "placeholder",
				  "width": placeholder,
				  "height": placeholder,
				  "autosize": "none",
				  "signals": [
				    { "name": "startAngle", "value": 0 },
				    { "name": "endAngle", "value": 6.29 },
				    { "name": "padAngle", "value": 0 },
				    { "name": "innerRadius", "value": 0 },
				    { "name": "cornerRadius", "value": 0 },
				    { "name": "sort", "value": false }
				  ],
				  "data": [
				    {
				      "name": "placeholder",
				      "values": [],
				      "transform": [
				        {
				          "type": "pie",
				          "field": "placeholder",
				          "startAngle": {"signal": "startAngle"},
				          "endAngle": {"signal": "endAngle"},
				          "sort": {"signal": "sort"}
				        }
				      ]
				    }
				  ],
				  "scales": [
				    {
				      "name": "color",
				      "type": "ordinal",
				      "domain": {"data": "table", "field": "id"},
				      "range": {"scheme": "category20"}
				    }
				  ],
				  "marks": [
				    {
				      "type": "arc",
				      "from": {"data": "table"},
				      "encode": {
				        "enter": {
				          "fill": {"scale": "color", "field": "id"},
				          "x": {"signal": "width / 2"},
				          "y": {"signal": "height / 2"}
				        },
				        "update": {
				          "startAngle": {"field": "startAngle"},
				          "endAngle": {"field": "endAngle"},
				          "padAngle": {"signal": "padAngle"},
				          "innerRadius": {"signal": "innerRadius"},
				          "outerRadius": {"signal": "width / 2"},
				          "cornerRadius": {"signal": "cornerRadius"}
				        }
				      }
				    }
				  ]
				}
				     """;
	}

	@Override
	public String getName() {
		return "FrameToGraph";
	}

	@Override
	public String getReactorDescription() {
		return "Converts a Frame into a Vega Block JSON spec template. The data field will be empty but sometimes partially filled out";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FRAME.getKey())) {
			return "This is a required value that takes in the frame id.";
		} else if (key.equals(ReactorKeysEnum.MODEL.getKey())) {
			return "This is a required value that takes in the model id";
		} else if (key.equals(USER_INPUT)) {
			return "This is an optional field to steer the model's graph generation behavior tailored to the user's prompt.";
		}
		return super.getDescriptionForKey(key);
	}
}
