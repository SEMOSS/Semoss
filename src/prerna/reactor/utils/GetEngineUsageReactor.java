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
package prerna.reactor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetEngineUsageReactor extends AbstractReactor {

	private static final String TYPE = "type";
	private static final String LABEL = "label";
	private static final String CODE = "code";
	private static final String PARAM_INFO = "parameters";

	private static final String INTRODUCTION = "introduction";
	private static final String PYTHON = "python";
	private static final String JAVA = "java";
	private static final String PIXEL = "pixel";
	private static final String LANGCHAIN = "LANGCHAIN";
	private static final String OPENAI = "OPENAI";
	private static final String ANTHROPIC = "ANTHROPIC";
	private static final String OLLAMA = "OLLAMA";
	private static final String ENGINE_ID_PLACEHOLDER = "<engineid>";
	private static final String API_ENDPOINT_PLACEHOLDER = "<apiendpoint>";
	private static final String OPENAI_ENDPOINT_PLACEHOLDER = "<openaiendpoint>";
	private static final String ANTHROPIC_ENDPOINT_PLACEHOLDER = "<anthropicendpoint>";
	private static final String OLLAMA_ENDPOINT_PLACEHOLDER = "<ollamaendpoint>";

	private static final String INTRODUCTION_LABEL = "Introduction";
	private static final String PIXEL_LABEL = "How to use in REST via Pixel";
	private static final String PYTHON_LABEL = "How to use in Python";
	private static final String JAVA_LABEL = "How to use in Java";
	private static final String LANGCHAIN_LABEL = "How to use with LangChain API";
	private static final String OPENAI_LABEL = "How to use externally with OpenAI API";
	private static final String ANTHROPIC_LABEL = "How to use externally with Anthropic API";
	private static final String OLLAMA_LABEL = "How to use externally with Ollama API";

	private static final String SAMPLE_ENGINE_ID = "SAMPLE_ENGINE_ID";

	// Shared platform primer appended to every engine's Introduction section so the
	// orientation text lives in one place instead of being repeated per channel.
	private static final String PLATFORM_INTRODUCTION = """
			**How to reach this engine**
			<br/>
			<br/>

			**REST via Pixel** - submit Pixel (the platform's server-side scripting language) to the `runPixel` REST endpoint. It returns a JSON envelope where each executed expression's result is at `pixelReturn[i].output`.
			<br/>
			**Python** - classes that generate the Pixel and unwraps the JSON response into a plain Python dict.
			<br/>
			<br/>

			A note on `<encode>` in Pixel: wrapping a string argument in `<encode>...</encode>` (for example `command = "<encode>She said "hi" to O'Brien</encode>"`) URL-encodes that text before parsing, so inner quotes and special characters do not need escaping. It is entirely optional. If you are programmatically generating Pixel, it is usually simpler and less error-prone to skip `<encode>` and instead escape inner double quotes with `\\"` and reserve `<encode>` for cases where escaping by hand would be tedious.
			<br/>
			<br/>

			Each section lists the common operations and shows the return structure.
			""";

	private static class EngineSelection {
		private final String engineId;
		private final IEngine.CATALOG_TYPE engineType;

		private EngineSelection(String engineId, IEngine.CATALOG_TYPE engineType) {
			this.engineId = engineId;
			this.engineType = engineType;
		}
	}

	public GetEngineUsageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.TYPE.getKey() };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		EngineSelection selection = resolveEngineSelection();
		List<Map<String, Object>> output = getUsageForEngineType(selection.engineType, selection.engineId);
		return new NounMetadata(output, PixelDataType.VECTOR);
	}

	private EngineSelection resolveEngineSelection() {
		String engineId = this.keyValue.get(this.keysToGet[0]);
		IEngine.CATALOG_TYPE engineType = null;
		if (engineId != null && !engineId.isEmpty()) {
			Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
			engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		} else {
			String engineTypeStr = this.keyValue.get(this.keysToGet[1]);
			if (engineTypeStr != null && !engineTypeStr.isEmpty()) {
				try {
					engineType = IEngine.CATALOG_TYPE.valueOf(engineTypeStr.toUpperCase());
					engineId = SAMPLE_ENGINE_ID;
				} catch (IllegalArgumentException e) {
					// do nothing
				}
			}
		}

		if (engineType == null) {
			throw new IllegalArgumentException("Must provide a valid engine id or a valid engine type");
		}
		return new EngineSelection(engineId, engineType);
	}

	private List<Map<String, Object>> getUsageForEngineType(IEngine.CATALOG_TYPE engineType, String engineId) {
		switch (engineType) {
		case DATABASE:
			return getDatabaseUsage(engineId);
		case STORAGE:
			return getStorageUsage(engineId);
		case MODEL:
			return getModelUsage(engineId);
		case VECTOR:
			return getVectorUsage(engineId);
		case FUNCTION:
			return getFunctionUsage(engineId);
		default:
			return getPendingUsage();
		}
	}

	private List<Map<String, Object>> getModelUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						A **Model** engine wraps one specific LLM (chat + embeddings) behind a single, consistent interface, so provider differences (OpenAI, Anthropic, Ollama, and others) are abstracted away and you interact with all of them the same way.
						<br/>
						<br/>

						Model/chat calls are *stateful* by default: a **room** object is created with each call which holds the conversation history, so follow-up calls can build on earlier turns. Pass a `roomId` (Pixel) / `room_id` (Python) to keep a thread going, or omit it and the current insight id will be used as the room identifier.
						<br/>
						<br/>

						Responses use schemaVersion 2: `response` is the convenience concatenated text, while `parts` is the full ordered content (text, tool_call, media, ...) that can mix modalities in a single turn. `messageType` (`CHAT`, `TOOL`, `IMAGE`) summarizes the turn.
						<br/>
						<br/>

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						Setup Room ID (Optional - this will default to the current insight id if not provided)

						Use `roomId` when you want follow-up calls to share the same conversation history.

						```
						myRoom = UUID();
						```

						Basic Generation

						```
						LLM(engine = "<engineid>", command = "<encode>Sample Question</encode>", paramValues=[{'max_completion_tokens':2000,'temperature':0.3}]);
						```

						Example Output

						Pixel runs through the `runPixel` REST endpoint, which wraps every result in an envelope. The model answer is nested at `pixelReturn[0].output`. Skip `isMeta = true` entries (bookkeeping), and treat an `operationType` of `["ERROR"]` as a failure where `output` holds the error message. Reuse the top-level `insightID` and the output's `roomId` to continue the conversation.

						```json
						{
						    "insightID": "8b419eaf-df7d-4a7f-869e-8d7d59bbfde8",
						    "pixelReturn": [
						        {
						            "pixelId": "3",
						            "pixelExpression": "LLM ( engine = [\\"<engineid>\\"] , command = [\\"Sample Question\\"] ) ;",
						            "isMeta": false,
						            "timeToRun": 842,
						            "output": {
						                "schemaVersion": 2,
						                "messageType": "CHAT",
						                "io": "OUTPUT",
						                "response": "The Los Angeles Dodgers won the World Series in 2020.",
						                "parts": [
						                    {
						                        "type": "TEXT",
						                        "text": "The Los Angeles Dodgers won the World Series in 2020."
						                    }
						                ],
						                "messageId": "0a80c2ce-76f9-4466-b2a2-8455e4cab34a",
						                "roomId": "28261853-0e41-49b0-8a50-df34e8c62a19",
						                "numberOfTokensInPrompt": 12,
						                "numberOfTokensInResponse": 11,
						                "numberOfCacheCreationTokens": 12
						            },
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						Understanding `output.parts` (schemaVersion 2)

						`response` is the concatenated text kept for convenience, but the full ordered content is in `parts` - an array that can mix modalities in a single turn (for example text + tool_call, or text + image). `io` is `OUTPUT` for a response, and `messageType` summarizes the turn (`CHAT`, `TOOL`, or `IMAGE`). Each part carries a `type` discriminator:

						- `TEXT` - `{"type":"TEXT","text":"...","uiText":"..."}` (`uiText` is a display-friendly variant; defaults to `text`).
						- `THINKING` - `{"type":"THINKING","thinking":"..."}` model reasoning trace.
						- `TOOL_CALL` - `{"type":"TOOL_CALL","toolCall":{"id":"call_1","type":"function","name":"get_weather","arguments":{"city":"Paris"}}}` a tool the model wants to run.
						- `TOOL_RESULT` - `{"type":"TOOL_RESULT","toolResult":{"toolCallId":"call_1","toolName":"get_weather","output":"...","toolParameterValues":{"city":"Paris"},"toolStatus":"success","serverTool":true}}` the result of a (server-run) tool.
						- `MEDIA` - `{"type":"MEDIA","mediaInfo":{"fileName":"chart.png","mimeType":"image/png","sourceUrl":"...","base64Data":"..."}}` an image/audio/file part.
						- `SYSTEM` - `{"type":"SYSTEM","prompt":"..."}` a system-prompt part.

						Token fields may also include `numberOfCacheCreationTokens`, `numberOfCacheReadTokens`, and `numberOfThinkingTokens`. Multi-part example (`messageType` `TOOL`), where the model emits text and then requests a tool:

						```json
						"parts": [
						    {"type": "TEXT", "text": "Let me check the weather for you."},
						    {"type": "TOOL_CALL", "toolCall": {"id": "call_1", "type": "function", "name": "get_weather", "arguments": {"city": "Paris"}}}
						]
						```

						Generation with Image

						```
						LLM(engine = "<engineid>", roomId = "my_room_id", command = "<encode>Sample Question With Image</encode>", url = "https://your_image_url.com");
						LLM(engine = "<engineid>", roomId = "my_room_id", command = "<encode>Sample Question With Image</encode>", image = "myImage.png");
						```

						Example Output

						Same envelope as Basic Generation - `pixelReturn[0].output` is a `CHAT` map with `response`, `parts`, `messageId`, `roomId`, `messageType`, and token counts. A model that returns an image instead sets `messageType` to `IMAGE` and carries the image in a `MEDIA` part.

						Generation with ChatML

						Pass a full prompt array to fully control conversation history for that call.

						```
						LLM(engine = "<engineid>", command = "<encode>ignore</encode>", paramValues=[
						    {"full_prompt":[
						        {"role":"system", "content": "You are a helpful assistant."},
						        {"role": "user", "content": "Who won the world series in 2020?"},
						        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
						        {"role": "user", "content": "Where was it played?"}
						    ],
						    'max_completion_tokens':2000,
						    'temperature':0.3
						}]);
						```

						Example Output

						Same `CHAT` envelope as Basic Generation.

						Embeddings

						```
						Embeddings(engine = "<engineid>", values = ["Sample String 1", "Sample String 2"], paramValues=[{}]);
						```

						Example Output

						Same envelope; `output` holds one vector per input string under `response`.

						```json
						{
						    "insightID": "8b419eaf-df7d-4a7f-869e-8d7d59bbfde8",
						    "pixelReturn": [
						        {
						            "pixelId": "4",
						            "pixelExpression": "Embeddings ( engine = [\\"<engineid>\\"] , values = [\\"Sample String 1\\" , \\"Sample String 2\\"] ) ;",
						            "isMeta": false,
						            "timeToRun": 128,
						            "output": {
						                "response": [
						                    [0.007663827, -0.030877046, -0.035327386],
						                    [0.012112318, -0.041237816, -0.006112934]
						                ],
						                "numberOfTokensInPrompt": 8,
						                "numberOfTokensInResponse": 0
						            },
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						Additional parameters: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)
						""",
				engineId);

		addUsage(usage, PYTHON, PYTHON_LABEL,
				"""
						Method Parameters<br/>
						`command` (str): prompt sent to the model.<br/>
						`room_id` (Optional[str]): conversation identifier.<br/>
						`context` (Optional[str]): system prompt context.<br/>
						`image` (Optional[List]): base64 image payload(s).<br/>
						`url` (Optional[List]): image URL(s).<br/>
						`use_history` (Optional[bool]): include history for this call.<br/>
						`param_dict` (Optional[Dict]): model/provider parameters.<br/>
						`insight_id` (Optional[str]): insight identifier.<br/>
						<br/>

						Getting Started

						```python
						# ModelEngine is the Semoss SDK wrapper for a configured LLM engine.
						# Use it to send prompts, continue conversations, and request embeddings.
						from ai_server import ModelEngine
						model = ModelEngine(engine_id = "<engineid>")
						```

						Text Generation

						```python
						prompt = 'Sample Question'
						output = model.ask(command = prompt, param_dict={'max_completion_tokens':2000,'temperature':0.3})
						```

						Example Output

						`ModelEngine` unwraps the `runPixel` envelope and returns `pixelReturn[0].output` directly - a dict.
						```python
						{
						    'schemaVersion': 2,
						    'messageType': 'CHAT',
						    'io': 'OUTPUT',
						    'response': 'The Los Angeles Dodgers won the World Series in 2020.',
						    'parts': [
						        {'type': 'TEXT', 'text': 'The Los Angeles Dodgers won the World Series in 2020.'}
						    ],
						    'messageId': '0a80c2ce-76f9-4466-b2a2-8455e4cab34a',
						    'roomId': '28261853-0e41-49b0-8a50-df34e8c62a19',
						    'numberOfTokensInPrompt': 12,
						    'numberOfTokensInResponse': 11,
						    'numberOfCacheCreationTokens': 12
						}
						```

						Understanding `parts` (schemaVersion 2)

						`response` is the `overall` return from the model and can be used for convenience, but the full ordered content is in `parts` - an array that can mix modalities in a single turn (for example text + tool_call, or text + image). `io` is `OUTPUT` for a response, and `messageType` summarizes the turn (`CHAT`, `TOOL`, or `IMAGE`). Each part carries a `type` discriminator:

						- `TEXT` - `{'type':'TEXT','text':'...','uiText':'...'}` (`uiText` is a display-friendly variant; defaults to `text`).
						- `THINKING` - `{'type':'THINKING','thinking':'...'}` model reasoning trace.
						- `TOOL_CALL` - `{'type':'TOOL_CALL','toolCall':{'id':'call_1','type':'function','name':'get_weather','arguments':{'city':'Paris'}}}` a tool the model wants to run.
						- `TOOL_RESULT` - `{'type':'TOOL_RESULT','toolResult':{'toolCallId':'call_1','toolName':'get_weather','output':'...','toolParameterValues':{'city':'Paris'},'toolStatus':'success','serverTool':True}}` the result of a (server-run) tool.
						- `MEDIA` - `{'type':'MEDIA','mediaInfo':{'fileName':'chart.png','mimeType':'image/png','sourceUrl':'...','base64Data':'...'}}` an image/audio/file part.
						- `SYSTEM` - `{'type':'SYSTEM','prompt':'...'}` a system-prompt part.

						Token fields may also include `numberOfCacheCreationTokens`, `numberOfCacheReadTokens`, and `numberOfThinkingTokens`. Multi-part example (`messageType` `TOOL`), where the model emits text and then requests a tool:

						```python
						'parts': [
						    {'type': 'TEXT', 'text': 'Let me check the weather for you.'},
						    {'type': 'TOOL_CALL', 'toolCall': {'id': 'call_1', 'type': 'function', 'name': 'get_weather', 'arguments': {'city': 'Paris'}}}
						]
						```

						Generation with Image / Vision

						Use only for models that support image input.

						```python
						prompt = 'Sample Command With Image'
						output = model.ask(command = prompt, url=['https://your_image_url.com'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
						output = model.ask(command = prompt, image=['base64_of_image'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
						```

						Example Output

						Same `CHAT` dict as Text Generation (`response`, `messageId`, `roomId`, `messageType`, token counts).

						Continue Conversation with Room ID

						```python
						prompt = 'Sample Question'
						room_id = 'my_room_id'
						output = model.ask(command = prompt, room_id = room_id, param_dict={'max_completion_tokens':2000,'temperature':0.3})
						```

						Example Output

						Same `CHAT` dict as Text Generation, with `roomId` echoing the `room_id` you passed in.

						Structured Outputs

						Use for models that support schema-constrained generation.

						```python
						prompt = 'Sample Command With Structured Output'
						json_schema = {
						    "type": "object",
						    "properties": {
						        "sample_property": {
						            "type": "array",
						            "items": {
						                "type": "object",
						                "properties": {
						                    "sample_property_1": {"type": "string"},
						                    "sample_property_2": {"type": "string"}
						                },
						                "required": ["sample_property_1", "sample_property_2"]
						            }
						        }
						    },
						    "required": ["sample_property"]
						}
						output = model.ask(command = prompt, param_dict={"schema": json_schema})
						```

						Example Output

						Same dict shape, but `response` (and the `TEXT` part) is the schema-constrained JSON encoded as a string (parse it with `json.loads`).

						```python
						{
						    'schemaVersion': 2,
						    'messageType': 'CHAT',
						    'io': 'OUTPUT',
						    'response': '{"sample_property": [{"sample_property_1": "value_1", "sample_property_2": "value_2"}]}',
						    'parts': [
						        {'type': 'TEXT', 'text': '{"sample_property": [{"sample_property_1": "value_1", "sample_property_2": "value_2"}]}'}
						    ],
						    'messageId': '1b91d3df-87fa-5577-c3b3-9566f5dbc45b',
						    'roomId': '28261853-0e41-49b0-8a50-df34e8c62a19',
						    'numberOfTokensInPrompt': 40,
						    'numberOfTokensInResponse': 22
						}
						```

						Generation with ChatML

						Pass `full_prompt` to explicitly define message history for a single call.

						```python
						model.ask(command='ignore', param_dict=
						    {"full_prompt":[
						        {"role":"system", "content": "You are a helpful assistant."},
						        {"role": "user", "content": "Who won the world series in 2020?"},
						        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
						        {"role": "user", "content": "Where was it played?"}
						    ],
						    'max_completion_tokens':2000,
						    'temperature':0.3
						});
						```

						Example Output

						Same `CHAT` dict as Text Generation.

						Embeddings

						```python
						text_arr = ['Sample String 1', 'Sample String 2']
						model.embeddings(strings_to_embed = text_arr)
						```

						Example Output

						Returns a dict with one vector per input string under `response`.

						```python
						{
						    'response': [
						        [0.007663827, -0.030877046, -0.035327386],
						        [0.012112318, -0.041237816, -0.006112934]
						    ],
						    'numberOfTokensInPrompt': 8,
						    'numberOfTokensInResponse': 0
						}
						```

						Additional parameters: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)
						""",
				engineId);

		addUsage(usage, LANGCHAIN, LANGCHAIN_LABEL, """
				Getting Started
				```python
				# ModelEngine connects to a Semoss model and can expose LangChain-compatible adapters.
				from ai_server import ModelEngine
				model = ModelEngine(engine_id = "<engineid>")
				```

				Chat Model
				```python
				langchain_llm = model.to_langchain_chat_model()
				question = 'Sample Question'
				output = langchain_llm.invoke(input = question)
				```

				Embedding Model
				```python
				langchain_llm = model.to_langchain_embedder()
				text_arr = ['Sample String 1', 'Sample String 2']
				langchain_llm.embed_query(text = text_arr[0])
				langchain_llm.embed_documents(texts = text_arr)
				```
				""", engineId);

		addUsage(usage, OPENAI, OPENAI_LABEL, """
				Direct Client Setup (without sdk)
				```python
				from openai import OpenAI

				# access key + secret key format
				client = OpenAI(
				    api_key="<accesskey>:<secretkey>",
				    base_url="<openaiendpoint>"
				)
				```

				Chat Completions (without sdk)
				```python
				response = client.chat.completions.create(
				    model="<engineid>",
				    messages=[
				        {"role": "system", "content": "You are a helpful assistant."},
				        {"role": "user", "content": "Who won the world series in 2020?"}
				    ]
				)
				print(response.choices[0].message.content)
				```

				Responses API (without sdk)
				```python
				response = client.responses.create(
				    model="<engineid>",
				    instructions="You are a helpful assistant.",
				    input="Who won the world series in 2020?"
				)
				print(response.output[0].text)
				```

				Legacy Completions (Deprecated by OpenAI, without sdk)
				```python
				response = client.completions.create(
				    model="<engineid>",
				    prompt="Write a tagline for an ice cream shop.",
				    extra_body={"insight_id":"<optional insight id>"}
				)
				```

				Embeddings (without sdk)
				```python
				embeddings = client.embeddings.create(
				    model="<engineid>",
				    input=["Your text string goes here"]
				)
				```

				Client Setup (with sdk)

				SDK package: [ai-server-sdk on PyPI](https://pypi.org/project/ai-server-sdk/)
				```python
				# Requires user access/secret, service account, or bearer token
				import ai_server
				server_connection=ai_server.ServerClient(
				    base="<apiendpoint>",
				    access_key="<your access key>",
				    secret_key="<your secret key>"
				)

				# Configure the OpenAI client to route through this Semoss instance
				from openai import OpenAI
				import httpx as httpx
				http_client = httpx.Client()
				http_client.cookies=server_connection.cookies

				client = OpenAI(
				    api_key="EMPTY",
				    base_url=server_connection.get_openai_endpoint(),
				    default_headers=server_connection.get_auth_headers(),
				    http_client=http_client
				)
				```

				Chat Completions (with sdk)
				```python
				response = client.chat.completions.create(
				    model="<engineid>",
				    messages=[
				        {"role": "system", "content": "You are a helpful assistant."},
				        {"role": "user", "content": "Who won the world series in 2020?"},
				        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
				        {"role": "user", "content": "Where was it played?"}
				    ],
				    # Only difference vs a standard OpenAI call: pass the current insight id in extra_body.
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				```

				Responses API (with sdk)
				```python
				response = client.responses.create(
				    model="<engineid>",
				    instructions="You are a helpful assistant.",
				    input="Who won the world series in 2020?",
					# Only difference vs a standard OpenAI call: pass the current insight id in extra_body.
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				print(response.output[0].text)
				```

				Legacy Completions (Deprecated by OpenAI, with sdk)
				```python
				response = client.completions.create(
				    model="<engineid>",
				    prompt="Write a tagline for an ice cream shop.",
				    # Only difference vs a standard OpenAI call: pass the current insight id in extra_body.
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				```

				Embeddings (with sdk)
				```python
				embeddings = client.embeddings.create(
				    model="<engineid>",
				    input=["Your text string goes here"],
				    # Only difference vs a standard OpenAI call: pass the current insight id in extra_body.
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				```
				""", engineId);

		addUsage(usage, ANTHROPIC, ANTHROPIC_LABEL, """
				Direct Client Setup
				```python
				from anthropic import Anthropic

				# access key + secret key format
				client = Anthropic(
				    auth_token="<accesskey>:<secretkey>",
				    base_url="<anthropicendpoint>"
				)
				```

				Chat Generation
				```python
				response = client.messages.create(
				    model="<engineid>",
				    max_tokens=1024,
				    messages=[
				        {"role": "user", "content": "Who won the world series in 2020?"}
				    ]
				)
				print(response.content[0].text)
				```
				""", engineId);

		addUsage(usage, OLLAMA, OLLAMA_LABEL, """
				Direct Client Setup
				```python
				from ollama import Client

				# access key + secret key format
				client = Client(
				    host="<ollamaendpoint>",
				    headers={"Authorization": "Bearer <accesskey>:<secretkey>"}
				)
				```

				Chat Generation
				```python
				response = client.chat(
				    model="<engineid>",
				    messages=[
				        {"role": "user", "content": "Who won the world series in 2020?"}
				    ]
				)
				print(response["message"]["content"])
				```
				""", engineId);

		addUsage(usage, JAVA, JAVA_LABEL, """
				```java
				import prerna.util.Utility;
				import prerna.engine.api.IModelEngine;
				IModelEngine modelEngine = Utility.getModel("<engineid>");
				```
				""", engineId);
		return usage;
	}

	private List<Map<String, Object>> getStorageUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						A **Storage** engine is a file/object store (for example a cloud bucket or a mounted filesystem) exposed behind one consistent interface. You reference files by a `storagePath` inside the engine and move them to and from a local `filePath`.
						<br/>
						<br/>

						Common operations are: list paths, list path details (name, size, mime type, modified time, metadata), upload/download single files, sync a folder in either direction, and delete. Uploads can attach arbitrary key-value `metadata`.
						<br/>
						<br/>

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						List Paths
						```
						Storage(storage = "<engineid>") | ListStoragePath(storagePath='/your/storage/path');
						```

						Example Output

						`output` is a flat array of the paths found under `storagePath`.

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "Storage ( storage = [ \\"<engineid>\\" ] ) | ListStoragePath ( storagePath = [ \\"/your/storage/path\\" ] ) ;",
						            "isMeta": false,
						            "timeToRun": 62,
						            "output": [
						                "report.pdf",
						                "images/",
						                "notes.txt"
						            ],
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						List Path Details<br/>
						Returns one object per file/folder with common keys:<br/>
						`Path`, `Name`, `Size`, `MimeType`, `ModTime`, `IsDir`, `Metadata`.<br/>
						`Metadata` is a key-value map (empty map when none exists).
						```
						Storage(storage = "<engineid>") | ListStoragePathDetails(storagePath='/your/storage/path');
						```

						Example Output

						`output` is an array with one object per file/folder. `Size` is in bytes, `ModTime` is null for folders, and `Metadata` is an empty map when none exists.

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "Storage ( storage = [ \\"<engineid>\\" ] ) | ListStoragePathDetails ( storagePath = [ \\"/your/storage/path\\" ] ) ;",
						            "isMeta": false,
						            "timeToRun": 74,
						            "output": [
						                {
						                    "Path": "/your/storage/path/report.pdf",
						                    "Name": "report.pdf",
						                    "Size": 20841,
						                    "MimeType": "application/pdf",
						                    "ModTime": "2026-06-14T18:03:11Z",
						                    "IsDir": false,
						                    "Metadata": {"author": "jsmith"}
						                },
						                {
						                    "Path": "/your/storage/path/images",
						                    "Name": "images",
						                    "Size": 0,
						                    "MimeType": "inode/directory",
						                    "ModTime": null,
						                    "IsDir": true,
						                    "Metadata": {}
						                }
						            ],
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						Download from Storage
						```
						Storage(storage = "<engineid>") | PullFromStorage(storagePath='/your/storage/path', filePath='/your/local/path');
						```

						Upload to Storage
						```
						Storage(storage = "<engineid>") | PushToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);
						```

						Sync Storage to Local
						```
						Storage(storage = "<engineid>") | SyncStorageToLocal(storagePath='/your/storage/path', filePath='/your/local/path');
						```

						Sync Local to Storage
						```
						Storage(storage = "<engineid>") | SyncLocalToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);
						```

						Delete from Storage
						```
						Storage(storage = "<engineid>") | DeleteFromStorage(storagePath='/your/storage/path', leaveFolderStructure=false);
						```

						Example Output (transfer + delete operations)

						`PullFromStorage`, `PushToStorage`, `SyncStorageToLocal`, `SyncLocalToStorage`, and `DeleteFromStorage` all return a boolean `true` in `output` on success (they throw an error, surfaced as `operationType` `["ERROR"]`, on failure).

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "Storage ( storage = [ \\"<engineid>\\" ] ) | PushToStorage ( storagePath = [ \\"/your/storage/path\\" ] , filePath = [ \\"/your/local/path\\" ] ) ;",
						            "isMeta": false,
						            "timeToRun": 318,
						            "output": true,
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```
						""",
				engineId);

		addUsage(usage, PYTHON, PYTHON_LABEL,
				"""
						Getting Started
						```python
						# StorageEngine is the Semoss SDK wrapper for file/object storage engines.
						# Use it to list, upload, download, sync, and delete storage content.
						from ai_server import StorageEngine
						storageEngine = StorageEngine(engine_id = "<engineid>")
						```

						List Paths
						```python
						storageEngine.list(storagePath = '/your/path/')
						```

						Example Output

						`StorageEngine` unwraps the envelope and returns the `output` directly - a list of paths.

						```python
						['report.pdf', 'images/', 'notes.txt']
						```

						List Path Details<br/>
						Returns one object per file/folder with common keys:<br/>
						`Path`, `Name`, `Size`, `MimeType`, `ModTime`, `IsDir`, `Metadata`.
						```python
						storageEngine.listDetails(storagePath = '/your/path/')
						```

						Example Output

						A list of dicts, one per file/folder (`Size` in bytes, `ModTime` null for folders, `Metadata` an empty dict when none exists).

						```python
						[
						    {'Path': '/your/path/report.pdf', 'Name': 'report.pdf', 'Size': 20841, 'MimeType': 'application/pdf', 'ModTime': '2026-06-14T18:03:11Z', 'IsDir': False, 'Metadata': {'author': 'jsmith'}},
						    {'Path': '/your/path/images', 'Name': 'images', 'Size': 0, 'MimeType': 'inode/directory', 'ModTime': None, 'IsDir': True, 'Metadata': {}}
						]
						```

						Sync Local to Storage
						```python
						storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path', metadata={'metaKey':'metaValue'})
						```

						Sync Storage to Local
						```python
						storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')
						```

						Copy File to Local
						```python
						storageEngine.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
						```

						Copy File to Storage
						```python
						storageEngine.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path', metadata={'metaKey':'metaValue'})
						```

						Delete from Storage
						```python
						storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path', leaveFolderStructure=False)
						```

						Example Output (sync, copy, and delete operations)

						`syncLocalToStorage`, `syncStorageToLocal`, `copyToLocal`, `copyToStorage`, and `deleteFromStorage` all return `True` on success (they raise a `RuntimeError` on failure).

						```python
						True
						```
						""",
				engineId);

		addUsage(usage, LANGCHAIN, LANGCHAIN_LABEL,
				"""
						Getting Started
						```python
						# StorageEngine can expose a LangChain-compatible storage adapter.
						from ai_server import StorageEngine
						storage = StorageEngine(engine_id = "<engineid>")
						langchain_storage = storage.to_langchain_storage()
						```

						List Paths
						```python
						langchain_storage.list(storagePath = '/your/path/')
						```

						List Path Details
						Returns one object per file/folder with common keys:
						`Path`, `Name`, `Size`, `MimeType`, `ModTime`, `IsDir`, `Metadata`.
						```python
						langchain_storage.listDetails(storagePath = '/your/path/')
						```

						Sync Local to Storage
						```python
						langchain_storage.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')
						```

						Sync Storage to Local
						```python
						langchain_storage.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')
						```

						Copy File to Local
						```python
						langchain_storage.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
						```

						Copy File to Storage
						```python
						langchain_storage.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
						```

						Delete from Storage
						```python
						langchain_storage.deleteFromStorage(storagePath = 'your/storage/file/path')
						```
						""",
				engineId);

		addUsage(usage, JAVA, JAVA_LABEL, """
				```java
				import prerna.util.Utility;
				import prerna.engine.api.IStorageEngine;
				IStorageEngine storage = Utility.getStorage("<engineid>");
				```
				""", engineId);
		return usage;
	}

	private List<Map<String, Object>> getDatabaseUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						A **Database** engine is a connected data source (RDBMS, RDF, Graph, NoSQL, etc.) you query behind one consistent interface. For ease of documentation, we will assume a SQL database, but the query can be replaced with SPARQL, Gremlin, etc. query based on the database type.
						<br/>
						<br/>

						Run `SELECT` queries to read rows and `INSERT`/`UPDATE`/`DELETE` to modify data.
						<br/>
						<br/>

						You can also fetch the database structure (logical and physical table/column or vertex/property names plus data types), which is useful for building queries or grounding an LLM. Select-style calls take a row `limit`; modification calls take a `commit` flag.
						<br/>
						<br/>

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						Select Queries
						```
						Database(database = "<engineid>")|Query("<encode> your select query </encode>")|Collect(500);
						```

						Insert/Update/Delete Queries
						```
						Database(database = "<engineid>")|Query("<encode> your insert/update/delete query </encode>")|ExecQuery();
						```

						Direct SQL Query<br/>
						`SqlQuery` auto-detects the SQL type and routes to the appropriate execution path.<br/>
						Select queries use select-style handling (`limit`).<br/>
						Insert/update/delete queries use modification-style handling (`commit`).
						```
						SqlQuery(database = "<engineid>", query = "<encode> SELECT * FROM table_name </encode>", limit = 500);
						SqlQuery(database = "<engineid>", query = "<encode> UPDATE table_name SET column1 = value1 WHERE condition </encode>", commit = true);
						```

						Example Output (select query)

						A select query returns a tabular result inside `pixelReturn[0].output`. `data.values` is an array of row arrays aligned to `data.headers` (display names) and `data.rawHeaders` (physical column names). `headerInfo` describes each column (`dataType`/`type`, and `derived = true` for computed columns), `sources` lists the engine(s) queried, `numCollected` is the number of rows returned (bounded by `limit`), and `taskId` references the server-side task iterator. Rows and columns are trimmed below for brevity.

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "SqlQuery ( database = [ \\"<engineid>\\" ] , query = [ \\"<encode>SELECT ID, AGE, GENDER FROM DIABETES</encode>\\" ] , limit = [ 500 ] ) ;",
						            "isMeta": false,
						            "timeToRun": 114,
						            "output": {
						                "data": {
						                    "values": [
						                        ["1000", 59, "female"],
						                        ["1001", 68, "female"]
						                    ],
						                    "headers": ["ID", "AGE", "GENDER"],
						                    "rawHeaders": ["ID", "AGE", "GENDER"]
						                },
						                "headerInfo": [
						                    {"dataType": "STRING", "alias": "ID", "header": "ID", "type": "STRING", "derived": false},
						                    {"dataType": "INT", "alias": "AGE", "header": "AGE", "type": "NUMBER", "derived": false},
						                    {"dataType": "STRING", "alias": "GENDER", "header": "GENDER", "type": "STRING", "derived": false}
						                ],
						                "sources": [
						                    {"name": "<engineid>", "type": "RAW_ENGINE_QUERY"}
						                ],
						                "numCollected": 2,
						                "taskId": "null"
						            },
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						An insert/update/delete query (or a `SqlQuery` with `commit = true`) instead returns the number of rows affected in `output`.

						Get Database Structure (logical + physical metadata)<br/>
						Each result row contains:<br/>
						1\\. Logical table name (RDBMS) or vertex name (Graph)<br/>
						2\\. Logical column name (RDBMS) or property name (Graph)<br/>
						3\\. Data type of the column or property<br/>
						4\\. Whether this row represents a graph vertex itself, rather than a property on it (only relevant for rdf/graph dbs)<br/>
						5\\. Physical column/property name as stored in the database<br/>
						6\\. Physical table/vertex name as stored in the database<br/>
						```
						GetDatabaseTableStructure(database = "<engineid>");
						```


						Direct Base64 SQL Query<br/>
						`SqlQueryBase64` uses the same wrapper behavior as `SqlQuery`; only the query input format changes (base64-encoded UTF-8 SQL string).<br/>
						`U0VMRUNUICogRlJPTSB0YWJsZV9uYW1lOw==` decodes to `SELECT * FROM table_name;`
						```
						SqlQueryBase64(database = "<engineid>", query = "U0VMRUNUICogRlJPTSB0YWJsZV9uYW1lOw==", limit = 500);
						```
						""",
				engineId);

		addUsage(usage, PYTHON, PYTHON_LABEL,
				"""
						Getting Started
						```python
						# DatabaseEngine is the Semoss SDK wrapper for database query execution.
						# Use it for read/write SQL operations against the selected engine.
						from ai_server import DatabaseEngine
						databaseEngine = DatabaseEngine(engine_id = "<engineid>")
						```

						Get Database Structure
						Each result row contains:<br/>
						1\\. Logical table name (RDBMS) or vertex name (Graph)<br/>
						2\\. Logical column name (RDBMS) or property name (Graph)<br/>
						3\\. Data type of the column or property<br/>
						4\\. Whether this row represents a graph vertex itself, rather than a property on it (only relevant for rdf/graph dbs)<br/>
						5\\. Physical column/property name as stored in the database<br/>
						6\\. Physical table/vertex name as stored in the database<br/>
						```python
						database_structure = databaseEngine.get_database_structure()
						```

						Run Select Query
						```python
						databaseEngine.execQuery(query = 'SELECT ID, AGE, GENDER FROM DIABETES')
						```

						Example Output

						By default `execQuery` returns a pandas DataFrame (columns are the query's display headers):

						```
						     ID  AGE  GENDER
						0  1000   59  female
						1  1001   68  female
						```

						Pass `return_pandas=False` to get the raw dict instead - the same `output` payload shown in the Pixel tab (`data.values`, `data.headers`, `data.rawHeaders`, `headerInfo`, ...).

						Insert Data
						```python
						databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')
						```

						Update Data
						```python
						databaseEngine.updateData(query = 'UPDATE table_name set column1=value1 WHERE condition')
						```

						Delete Data
						```python
						databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')
						```
						""",
				engineId);

		addUsage(usage, LANGCHAIN, LANGCHAIN_LABEL,
				"""
						Getting Started
						```python
						# DatabaseEngine can be adapted to LangChain database interfaces.
						from ai_server import DatabaseEngine
						database = DatabaseEngine(engine_id = "<engineid>")
						langchain_db = database.to_langchain_database()
						```

						Run Select Query
						```python
						langchain_db.executeQuery(query = 'SELECT * FROM table_name')
						```

						Insert Data
						```python
						langchain_db.insertQuery(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')
						```

						Update Data
						```python
						langchain_db.updateQuery(query = 'UPDATE table_name set column1=value1 WHERE condition')
						```

						Delete Data
						```python
						langchain_db.removeQuery(query = 'DELETE FROM table_name WHERE condition')
						```
						""",
				engineId);

		addUsage(usage, JAVA, JAVA_LABEL, """
				```java
				import prerna.util.Utility;
				import prerna.engine.api.IDatabaseEngine;
				IDatabaseEngine database = Utility.getDatabase("<engineid>");
				```
				""", engineId);
		return usage;
	}

	private List<Map<String, Object>> getVectorUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						A **Vector** engine indexes documents as embeddings so you can run semantic (nearest-neighbor) search - the backbone of retrieval-augmented generation (RAG). It handles chunking and embedding for you; you just add documents and query in natural language.
						<br/>
						<br/>

						Common operations are: list indexed documents (unique `Source` values), add documents (files or pre-chunked VectorCSV files, from the insight/room space or an app/user space), run a nearest-neighbor search with optional `filters`/`metaFilters`, and remove documents. Pair a Vector engine with a Model engine to answer questions grounded in your own content.
						<br/>
						<br/>

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						List current vector documents (unique `Source` values)
						```
						ListDocumentsInVectorDatabase (engine = "<engineid>");
						```

						Example Output

						`output` is an array with one object per indexed document (`fileName` is the `Source` identifier, `fileSize` is in MB, `lastModified` is a formatted timestamp).

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "ListDocumentsInVectorDatabase ( engine = [ \\"<engineid>\\" ] ) ;",
						            "isMeta": false,
						            "timeToRun": 41,
						            "output": [
						                {"fileName": "handbook.pdf", "fileSize": 482.11, "lastModified": "2026-06-14 18:03:11"},
						                {"fileName": "faq.pdf", "fileSize": 88.4, "lastModified": "2026-06-14 18:05:52"}
						            ],
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						Add uploaded documents from the current insight/room space

						```
						CreateEmbeddingsFromDocuments (engine = "<engineid>", filePaths = ["fileName1.pdf", "fileName2.pdf", ..., "fileNameX.pdf"]);
						```

						Add uploaded documents from app/project/user space

						Use `space` when files are not in the current insight folder.<br/>
						`space = "app_id"` (project/app UUID), `space = "user"` (user space).
						```
						CreateEmbeddingsFromDocuments (engine = "<engineid>", filePaths = ["docs/file1.pdf"], space = "app_id");
						```

						Add VectorCSVFile-formatted CSV files from current insight/room space

						Supported file types: csv or zip archives containing csv files.<br/>
						Expected csv headers: `Source`, `Modality`, `Divider`, `Part`, `Tokens`, `Content`<br/>
						Headers are case-sensitive and should match exactly as shown.<br/>
						CSV quotes are optional unless a value contains commas/newlines/quotes (`doc1.pdf` does not need quotes).
						```csv
						Source,Modality,Divider,Part,Tokens,Content
						doc1.pdf,text,1,0,120,"First chunk of text"
						```
						```
						CreateEmbeddingsFromVectorCSVFile (engine = "<engineid>", filePaths = ["fileName1.csv", "fileName2.csv", ..., "fileNameX.csv"]);
						```

						Add VectorCSVFile-formatted CSV files from app/project/user space
						```
						CreateEmbeddingsFromVectorCSVFile (engine = "<engineid>", filePaths = ["vector_data/chunks.csv"], space = "app_id");
						```

						Run nearest-neighbor search
						```
						## filters format: Filter(Source == ["your document name 1", "your document name 2"]) ##
						## metaFilters format: Filter(MetadataKey == "Metadata Value") ##
						VectorDatabaseQuery (engine = "<engineid>", command = "Sample Search Statement", limit = 5, filters=[], metaFilters=[]);
						```

						Example Output

						`output` is an array of the closest matching chunks, ordered best-first (up to `limit`). Each match carries the chunk `Content` plus its provenance (`Source`, `Modality`, `Divider`, `Part`, `Tokens`) and a `Score` (relevance/distance - interpretation depends on the underlying vector store). Feed the `Content` values to a Model engine as grounding context for RAG.

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "VectorDatabaseQuery ( engine = [ \\"<engineid>\\" ] , command = [ \\"Sample Search Statement\\" ] , limit = [ 5 ] ) ;",
						            "isMeta": false,
						            "timeToRun": 210,
						            "output": [
						                {
						                    "Score": 0.8123,
						                    "Content": "Employees accrue 15 days of paid time off per year.",
						                    "Source": "handbook.pdf",
						                    "Modality": "text",
						                    "Divider": 12,
						                    "Part": 0,
						                    "Tokens": 11
						                },
						                {
						                    "Score": 0.7460,
						                    "Content": "Unused PTO rolls over up to a maximum of 5 days.",
						                    "Source": "handbook.pdf",
						                    "Modality": "text",
						                    "Divider": 12,
						                    "Part": 1,
						                    "Tokens": 12
						                }
						            ],
						            "operationType": ["OPERATION"]
						        }
						    ]
						}
						```

						Remove files from the vector index

						Use `fileNames` (source identifiers), not file paths.
						```
						RemoveDocumentFromVectorDatabase (engine = "<engineid>", fileNames = ["fileName1.pdf", "fileName2.pdf", ..., "fileNameX.pdf"]);
						```

						Example Output

						`CreateEmbeddingsFromDocuments` / `CreateEmbeddingsFromVectorCSVFile` return a success message, with a per-file status array (`fileName`, `status`, `insertedRecords`, `failedRecords`, `totalRecords`) under `additionalOutput`. `RemoveDocumentFromVectorDatabase` returns a boolean `true` in `output`.

						```json
						{
						    "insightID": "019f2a23-f376-7586-b6e6-3992356a5117",
						    "pixelReturn": [
						        {
						            "pixelId": "0",
						            "pixelExpression": "CreateEmbeddingsFromDocuments ( engine = [ \\"<engineid>\\" ] , filePaths = [ \\"handbook.pdf\\" ] ) ;",
						            "isMeta": false,
						            "timeToRun": 5231,
						            "output": "Successfully embedded all files",
						            "operationType": ["OPERATION"],
						            "additionalOutput": [
						                {
						                    "output": [
						                        {"fileName": "handbook.pdf", "status": "SUCCESS", "insertedRecords": 42, "failedRecords": 0, "totalRecords": 42}
						                    ],
						                    "operationType": ["OPERATION"]
						                }
						            ]
						        }
						    ]
						}
						```
						""",
				engineId);

		addUsage(usage, PYTHON, PYTHON_LABEL,
				"""
						Getting Started
						```python
						# VectorEngine manages document indexing and semantic search in vector stores.
						from ai_server import VectorEngine
						vectorEngine = VectorEngine(engine_id = "<engineid>")
						```

						List Indexed Documents

						Returns unique source identifiers currently stored in the vector database.
						```python
						vectorEngine.listDocuments()
						```

						Example Output

						A list of dicts, one per indexed document (`fileSize` in MB).

						```python
						[
						    {'fileName': 'handbook.pdf', 'fileSize': 482.11, 'lastModified': '2026-06-14 18:03:11'},
						    {'fileName': 'faq.pdf', 'fileSize': 88.4, 'lastModified': '2026-06-14 18:05:52'}
						]
						```

						Add Uploaded Documents (insight/room space)
						```python
						vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])
						```

						Add Uploaded Documents (app/project/user space)

						`space='app_id'` (project/app UUID), `space='user'` (user space).
						```python
						vectorEngine.addDocument(file_paths = ['docs/fileName1.pdf'], space='app_id')
						```

						Add VectorCSVFile-formatted CSVs (insight/room space)

						Expected csv headers: `Source`, `Modality`, `Divider`, `Part`, `Tokens`, `Content`<br/>
						Headers are case-sensitive and should match exactly as shown.<br/>
						CSV quotes are optional unless a value contains commas/newlines/quotes (`doc1.pdf` does not need quotes).
						```csv
						Source,Modality,Divider,Part,Tokens,Content
						doc1.pdf,text,1,0,120,"First chunk of text"
						```
						```python
						vectorEngine.addVectorCSVFile(file_paths = ['fileName1.csv', 'fileName2.csv', ..., 'fileNameX.csv'])
						```

						Add VectorCSVFile-formatted CSVs (app/project/user space)
						```python
						vectorEngine.addVectorCSVFile(file_paths = ['vector_data/chunks.csv'], space='app_id')
						```

						Nearest-neighbor Search

						`filters` can be a dict or string expression.<br/>
						`metafilters` can be a dict or string expression.
						```python
						# str filter example: 'Filter(Source == ["your document name 1", "your document name 2"])'
						# str metafilter example: 'Filter(MetadataKey == "Metadata Value")'
						vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5, param_dict={}, filters='', metafilters='')
						```

						Example Output

						A list of the closest matching chunks, ordered best-first (up to `limit`). Use the `Content` values as grounding context for a Model engine (RAG); `Score` is relevance/distance (interpretation depends on the underlying vector store).

						```python
						[
						    {'Score': 0.8123, 'Content': 'Employees accrue 15 days of paid time off per year.', 'Source': 'handbook.pdf', 'Modality': 'text', 'Divider': 12, 'Part': 0, 'Tokens': 11},
						    {'Score': 0.7460, 'Content': 'Unused PTO rolls over up to a maximum of 5 days.', 'Source': 'handbook.pdf', 'Modality': 'text', 'Divider': 12, 'Part': 1, 'Tokens': 12}
						]
						```

						Remove Indexed Documents

						Use `file_names` as source identifiers (for example names returned by `listDocuments()`).
						```python
						vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])
						```

						Example Output

						`addDocument` / `addVectorCSVFile` return the success message string; `removeDocument` returns `True`.
						""",
				engineId);

		addUsage(usage, LANGCHAIN, LANGCHAIN_LABEL, """
				Getting Started
				```python
				# VectorEngine can be adapted to a LangChain vector store.
				from ai_server import VectorEngine
				vector = VectorEngine(engine_id = "<engineid>")
				langchain_vector = vector.to_langchain_vector_store()
				```

				List Indexed Documents
				```python
				langchain_vector.listDocs()
				```

				Add Documents
				```python
				langchain_vector.addDocs(file_paths = ['file1.pdf','file2.pdf',...])
				```

				Add Documents from app/project/user space

				`to_langchain_vector_store().addDocs(...)` uses insight space.<br/>
				Use the base vector engine call when you need `space`.
				```python
				vector.addDocument(file_paths=['docs/file1.pdf'], space='app_id')
				```

				Remove Documents
				```python
				langchain_vector.removeDocs(file_names = ['file1.pdf','file2.pdf',...])
				```

				Similarity Search
				```python
				langchain_vector.similaritySearch(query = 'Sample Search Statement', k=5)
				```
				""", engineId);

		addUsage(usage, JAVA, JAVA_LABEL, """
				Getting Started
				```java
				// imports
				import java.util.HashMap;
				import java.util.List;
				import java.util.Map;
				import prerna.util.Utility;
				import prerna.engine.api.IVectorDatabaseEngine;
				import prerna.om.Insight;

				// get the vector engine
				IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase("<engineid>");
				Map<String, Object> parameters = new HashMap<>();
				```

				List Indexed Documents
				```java
				vectorEngine.listDocuments(parameters);
				```

				Add Uploaded Documents
				```java
				vectorEngine.addDocument(List.of("fileName1.pdf", "fileName2.pdf"), parameters);
				```

				Add VectorCSVFile-formatted CSV Files
				```java
				Insight insight = ...; // use the current insight context
				vectorEngine.addEmbeddings(List.of("fileName1.csv", "fileName2.csv"), insight, parameters);
				```

				Run Nearest-neighbor Search
				```java
				vectorEngine.nearestNeighbor("Sample Search Statement", 5, parameters);
				```

				Remove Indexed Documents
				```java
				vectorEngine.removeDocument(List.of("fileName1.pdf"), parameters);
				```
				""", engineId);
		return usage;
	}

	private List<Map<String, Object>> getFunctionUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		List<Map<String, Object>> paramInfo = null;
		String mapParams = null;
		if (SAMPLE_ENGINE_ID.equals(engineId)) {
			mapParams = "{\"function_parameter_1\":\"function_value_1\"}";
		} else {
			IFunctionEngine functionEngine = Utility.getFunctionEngine(engineId);
			List<FunctionParameter> parameters = getFunctionParameters(functionEngine);
			List<String> requiredParameters = getRequiredFunctionParameters(functionEngine);
			paramInfo = buildFunctionParamInfo(parameters, requiredParameters);
			mapParams = buildFunctionMapParams(parameters);
		}
		String pixelMapArg = mapParams.isEmpty() ? "" : " , map=[" + mapParams + "] ";

		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						A **Function** engine exposes a callable tool or API behind one consistent interface. You execute it with a map of named parameter values and it returns the tool's result.
						<br/>
						<br/>

						Its parameter contract (each parameter's `name`, `type`, `description`, and whether it is `required`) is published alongside these snippets in the `parameters` field, and the example call below is pre-filled with this engine's actual parameters. Function engines are also what a Model engine calls when doing tool use.
						<br/>
						<br/>

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);

		addUsage(usage, PIXEL, PIXEL_LABEL, """
				Execute Function
				```
				ExecuteFunctionEngine(engine = "<engineid>"<javastring>);
				```
				""".replace("<javastring>", pixelMapArg), engineId, paramInfo);

		addUsage(usage, PYTHON, PYTHON_LABEL, """
				Getting Started
				```python
				# FunctionEngine is the Semoss SDK wrapper for calling function engines.
				# Use it to execute the engine with a parameter dictionary payload.
				from ai_server import FunctionEngine
				function = FunctionEngine(engine_id = "<engineid>")
				```

				Execute Function
				```python
				output = function.execute(<mapparams>)
				```
				""".replace("<mapparams>", mapParams), engineId, paramInfo);

		addUsage(usage, JAVA, JAVA_LABEL, """
				```java
				import prerna.util.Utility;
				import prerna.engine.api.IFunctionEngine;
				IFunctionEngine function = Utility.getFunction("<engineid>");
				```
				""", engineId, paramInfo);
		return usage;
	}

	private List<Map<String, Object>> getPendingUsage() {
		List<Map<String, Object>> usage = new ArrayList<>();
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL, """
				Detailed usage examples for this engine type are not available yet.
				<br/>
				<br/>

				""" + PLATFORM_INTRODUCTION, null);
		addUsage(usage, PIXEL, PIXEL_LABEL, "Documentation pending", null);
		addUsage(usage, PYTHON, PYTHON_LABEL, "Documentation pending", null);
		addUsage(usage, JAVA, JAVA_LABEL, "Documentation pending", null);
		return usage;
	}

	private List<FunctionParameter> getFunctionParameters(IFunctionEngine functionEngine) {
		List<FunctionParameter> parameters = functionEngine.getParameters();
		return parameters == null ? new ArrayList<>() : parameters;
	}

	private List<String> getRequiredFunctionParameters(IFunctionEngine functionEngine) {
		List<String> requiredParameters = functionEngine.getRequiredParameters();
		return requiredParameters == null ? new ArrayList<>() : requiredParameters;
	}

	private List<Map<String, Object>> buildFunctionParamInfo(List<FunctionParameter> parameters,
			List<String> requiredParameters) {
		List<Map<String, Object>> paramInfo = new ArrayList<>();
		for (FunctionParameter fp : parameters) {
			Map<String, Object> pinfo = new HashMap<>();
			pinfo.put("required", requiredParameters.contains(fp.getParameterName()));
			pinfo.put("name", fp.getParameterName());
			pinfo.put("type", fp.getParameterType());
			pinfo.put("description", fp.getParameterDescription());
			paramInfo.add(pinfo);
		}
		return paramInfo;
	}

	private String buildFunctionMapParams(List<FunctionParameter> parameters) {
		if (parameters.isEmpty()) {
			return "";
		}
		StringBuilder mapParams = new StringBuilder("{");
		for (int i = 0; i < parameters.size(); i++) {
			FunctionParameter fp = parameters.get(i);
			if (i > 0) {
				mapParams.append(", ");
			}
			mapParams.append("\"").append(fp.getParameterName()).append("\":")
					.append(getDefaultParamValue(fp.getParameterType()));
		}
		mapParams.append("}");
		return mapParams.toString();
	}

	private String getDefaultParamValue(String type) {
		if ("string".equalsIgnoreCase(type)) {
			return "\"string\"";
		}
		return type;
	}

	/**
	 * Adds a usage entry and performs common formatting for code templates.
	 */
	private void addUsage(List<Map<String, Object>> usage, String type, String label, String codeTemplate,
			String engineId) {
		usage.add(fillMap(type, label, formatUsageCode(codeTemplate, engineId)));
	}

	private void addUsage(List<Map<String, Object>> usage, String type, String label, String codeTemplate,
			String engineId, List<Map<String, Object>> paramInfo) {
		usage.add(fillMap(type, label, formatUsageCode(codeTemplate, engineId), paramInfo));
	}

	private String formatUsageCode(String codeTemplate, String engineId) {
		String formattedCode = codeTemplate.trim();
		if (engineId != null && !engineId.isEmpty()) {
			formattedCode = formattedCode.replace(ENGINE_ID_PLACEHOLDER, engineId);
		}
		String applicationUrl = Utility.getApplicationUrl();
		if (applicationUrl != null && !applicationUrl.isEmpty()) {
			String apiEndpoint = appendPath(applicationUrl, "api");
			String openAiEndpoint = appendPath(apiEndpoint, "model/openai");
			String anthropicEndpoint = appendPath(apiEndpoint, "model/anthropic");
			String ollamaEndpoint = appendPath(apiEndpoint, "model/ollama");
			formattedCode = formattedCode.replace(API_ENDPOINT_PLACEHOLDER, apiEndpoint)
					.replace(OPENAI_ENDPOINT_PLACEHOLDER, openAiEndpoint)
					.replace(ANTHROPIC_ENDPOINT_PLACEHOLDER, anthropicEndpoint)
					.replace(OLLAMA_ENDPOINT_PLACEHOLDER, ollamaEndpoint);
		}
		return formattedCode;
	}

	private String appendPath(String baseUrl, String pathSegment) {
		String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
		String normalizedPath = pathSegment.startsWith("/") ? pathSegment.substring(1) : pathSegment;
		return normalizedBase + "/" + normalizedPath;
	}

	private Map<String, Object> fillMap(String type, String label, String code) {
		Map<String, Object> usageMap = new HashMap<>();
		usageMap.put(TYPE, type);
		usageMap.put(LABEL, label);
		usageMap.put(CODE, code);
		return usageMap;
	}

	private Map<String, Object> fillMap(String type, String label, String code, List<Map<String, Object>> paramInfo) {
		Map<String, Object> usageMap = new HashMap<>();
		usageMap.put(TYPE, type);
		usageMap.put(LABEL, label);
		usageMap.put(CODE, code);
		usageMap.put(PARAM_INFO, paramInfo);
		return usageMap;
	}

	@Override
	public String getReactorDescription() {
		return """
				Builds tutorial-style usage snippets for a selected engine across Pixel, Python, Java, and optional integrations (for example LangChain or OpenAI-compatible usage when supported).

				Platform context (useful for both human readers and machine consumers): on the Semoss AI Server platform every capability is an *engine* registered in a shared catalog - Model (LLMs), Vector (semantic search), Database (SQL/graph), Storage (files), and Function (callable tools) - addressed by a stable `engineId`. Pixel is the server-side scripting language executed through the `runPixel` REST endpoint; the `ai_server` Python SDK and the Java `Utility` helpers call the same engines. Model/chat calls are stateful via a *room* (insight) that holds conversation history.

				- Input resolution order is: `engine` first, then `type` if `engine` is not provided.
				- When only `type` is supplied, snippets are generated with `SAMPLE_ENGINE_ID` as the placeholder engine identifier.
				- The returned vector contains one object per usage channel with `type`, `label`, and `code`.
				- The first channel is always `type = "introduction"`: a markdown primer explaining what this engine type does plus a shared "how to reach this engine" platform section. The remaining channels (`pixel`, `python`, `java`, ...) are per-integration.
				- Each integration channel's `code` is markdown with per-operation example calls, each followed by an "Example Output" block showing the JSON/dict payload it returns. Pixel outputs show the full `runPixel` envelope (`pixelReturn[i].output`); Python SDK outputs show the unwrapped payload.
				- Model responses are schemaVersion 2: `response` is the convenience concatenated text while `parts` is the full ordered content (text, tool_call, media, etc.) that can mix modalities in one turn.
				- Function-engine responses also include `parameters`, where each item contains `name`, `type`, `description`, and `required`.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return """
					Engine ID used to derive both the catalog type and engine-specific usage examples.

					- If both `engine` and `type` are provided, this value takes precedence.
					""";
		} else if (key.equals(ReactorKeysEnum.TYPE.getKey())) {
			String validValues = String.join(", ", IEngine.CATALOG_TYPE.DATABASE.toString(),
					IEngine.CATALOG_TYPE.STORAGE.toString(), IEngine.CATALOG_TYPE.MODEL.toString(),
					IEngine.CATALOG_TYPE.VECTOR.toString(), IEngine.CATALOG_TYPE.FUNCTION.toString());
			return """
					Fallback engine catalog type used only when `engine` is not provided.

					- Values are case-insensitive. If valid, examples are generated with `SAMPLE_ENGINE_ID`.
					- Valid values: %s.
					- If neither a valid `engine` nor a valid `type` is provided, the reactor throws an `IllegalArgumentException`.
					"""
					.formatted(validValues);
		}
		return super.getDescriptionForKey(key);
	}

}
