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
	private static final String JAVASCRIPT = "javascript";
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
	private static final String JAVASCRIPT_LABEL = "How to use in JavaScript/TypeScript with the @semoss/sdk";
	private static final String LANGCHAIN_LABEL = "How to use with LangChain API";
	private static final String OPENAI_LABEL = "How to use externally with OpenAI API";
	private static final String ANTHROPIC_LABEL = "How to use externally with Anthropic API";
	private static final String OLLAMA_LABEL = "How to use externally with Ollama API";

	private static final String SAMPLE_ENGINE_ID = "SAMPLE_ENGINE_ID";

	// Shared platform primer appended to every engine's Introduction section so the
	// orientation text lives in one place instead of being repeated per channel.
	private static final String PLATFORM_INTRODUCTION = """
			## How to reach this engine

			Every engine is addressed by its `engineId` and does the same work no matter which channel calls it. Pick the tab that matches where your code runs:

			- **Pixel** - the platform's server-side scripting language, submitted to the `runPixel` REST endpoint. Every other channel is a wrapper around it, so this tab is the reference for what each operation actually does.
			- **JavaScript/TypeScript** - `runPixel` from the `@semoss/sdk` package, for app front ends.
			- **Python** - engine classes from the `ai_server` package, which build the Pixel and hand back a plain dict.
			- **Java** - `Utility` helpers, for reactors and other server-side code.

			Some engine types add further tabs, for example LangChain adapters or an OpenAI-compatible endpoint for a Model engine.

			## Reading a response

			`runPixel` returns a JSON envelope rather than a bare result:

			- `pixelReturn[i].output` holds the result of the i-th expression you submitted. This is the payload each tab documents.
			- `pixelReturn[i].operationType` of `["ERROR"]` means that expression failed and its `output` is the error message.
			- Entries with `isMeta` set to `true` are bookkeeping; skip them.
			- `insightID` identifies the session the Pixel ran under. Reuse it to keep server-side state across calls.

			The SDKs unwrap most of that for you. The Python engine classes return `pixelReturn[0].output` directly, and `runPixel` from `@semoss/sdk` returns the parsed envelope alongside an `errors` array it pre-collects from every `ERROR` entry, so one check covers both transport and application failures.

			## Escaping text inside Pixel

			Wrapping a string argument in `<encode>...</encode>` URL-encodes that text before it is parsed, so inner quotes and special characters do not need escaping:

			```
			command = "<encode>She said "hi" to O'Brien</encode>"
			```

			It is entirely optional. When you generate Pixel programmatically it is usually simpler to escape inner double quotes with `\\"` and save `<encode>` for text that would be tedious to escape by hand.

			The remaining tabs list the common operations for this engine, each with an example call and the shape it returns.
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
						A **Model** engine wraps one specific LLM behind a single, consistent interface, so provider differences (OpenAI, Anthropic, Ollama, and others) are abstracted away and every model is called the same way.

						## What you can do

						- **Generate text** - send a prompt and get the model's answer back.
						- **Hold a conversation** - chat calls are stateful by default; a *room* holds the history so follow-up turns build on earlier ones.
						- **Send images** - pass a public URL or an uploaded file to a vision-capable model.
						- **Constrain the output** - pass a JSON schema and get schema-valid JSON back.
						- **Create embeddings** - turn text into vectors, usually to store in a Vector engine.

						## Key concepts

						- **Rooms.** Pass a `roomId` (Pixel, JavaScript) or `room_id` (Python) to keep a thread going. Omit it and the current insight id is used as the room identifier.
						- **schemaVersion 2 responses.** `response` is the concatenated text kept for convenience; `parts` is the full ordered content and can mix modalities in one turn (text, thinking, tool_call, tool_result, media). `messageType` (`CHAT`, `TOOL`, `IMAGE`) summarizes the turn.
						- **Token accounting.** Every response carries `numberOfTokensInPrompt` and `numberOfTokensInResponse`, plus cache and thinking counts when the provider reports them.

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

		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL,
				"""
						Getting Started

						`runPixel` from `@semoss/sdk` submits Pixel from an app front end and returns the parsed envelope, so the model answer is at `pixelReturn[0].output`.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const MODEL_ID = "<engineid>";
						const prompt = "Sample Question";

						const { errors, pixelReturn } = await runPixel(
						  `LLM(engine="${MODEL_ID}", command=["${prompt}"], paramValues=[{"temperature":0.3, "max_completion_tokens":2000}]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const response = pixelReturn[0].output.response;
						```

						`errors` already contains any expression the server flagged with `operationType` `["ERROR"]`, so this one check is enough. `output` is the same schemaVersion 2 map documented in the Pixel section (`response`, `parts`, `messageType`, `messageId`, `roomId`, token counts).

						The variations below show only the Pixel string, the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call, the `errors` check, and the response parsing are the same as above.

						Insight Sessions

						`runPixel(pixel, insightId)` takes an optional second argument. Pass `"new"` to start a fresh insight and reuse the returned `insightId` on every later call in that session; omit it to run on the app's current insight.

						```typescript
						const { insightId, pixelReturn } = await runPixel(
						  `LLM(engine="${MODEL_ID}", command=["Sample Question"]);`,
						  "new",
						);
						```

						Generation with ChatML

						Pass a `full_prompt` array inside `paramValues` to control the conversation history for that call. `command` is ignored when `full_prompt` is present, so pass `"ignore"` as a placeholder.

						```
						LLM(engine="${MODEL_ID}", command=["ignore"], paramValues=[{
						    "full_prompt": [
						        {"role": "system", "content": "You are a helpful assistant."},
						        {"role": "user", "content": "Who won the world series in 2020?"},
						        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
						        {"role": "user", "content": "Where was it played?"}
						    ],
						    "max_completion_tokens": 2000,
						    "temperature": 0.3
						}]);
						```

						Structured Outputs

						Pass `schema` in `paramValues` for models that support schema-constrained generation. `response` comes back as a JSON string, so parse it with `JSON.parse`.

						```
						LLM(engine="${MODEL_ID}", command=["Sample Question"], paramValues=[{"schema": {"type":"object","properties":{"sample_property":{"type":"string"}},"required":["sample_property"]}}]);
						```

						Generation with Image

						Pass either a public `url` or a server-accessible `image` filename as a top-level argument. Use `roomId` to thread several image turns into one conversation.

						```
						LLM(engine="${MODEL_ID}", roomId="my_room_id", command=["What is in this image?"], url="https://your_image_url.com");
						```

						Attaching Uploaded Files

						Sending a file with a prompt is always two calls, in order: upload the bytes, then reference the returned `fileLocation`. The bytes are never re-sent.

						```typescript
						import { uploadInsight } from "@semoss/sdk";

						// data is one { fileName, fileLocation } per input file, in input order
						const { data: uploaded } = await uploadInsight(insightId, "", files);
						```

						The helper posts to `{MODULE}/api/uploadFile/baseUpload?insightId=...&path=...&userSpace=false` with every file under the same form field name, `file`. If you call that endpoint directly, do not set `Content-Type` by hand - the browser has to set it so the multipart boundary is added - and send the request with the session cookie.

						Each uploaded file then travels as a `MEDIA` part alongside the user's `TEXT` part:

						```typescript
						const parts = [{ type: "TEXT", text: prompt, uiText: prompt }];
						for (const f of uploaded) {
						  parts.push({
						    type: "MEDIA",
						    mediaInfo: {
						      fileName: f.fileName,
						      fileLocation: f.fileLocation,
						      mediaInputType: "FILE",
						      base64Data: "",
						      fileFormat: "",
						      mimeType: "",
						    },
						  });
						}
						```

						Threaded Chat with a Room

						A room is a durable conversation that owns the history `roomId` threads into. Create it on a new insight, bind the insight to it once, then pass `roomId` on every turn.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const { errors, pixelReturn, insightId } = await runPixel<[{ roomId: string }]>(
						  `CreatePlaygroundRoom();`,
						  "new",
						);
						if (errors.length) throw new Error(errors[0]);

						const roomId = pixelReturn[0].output.roomId;

						// bind this insight to the room so later turns are recorded against it
						await runPixel(`SetRoomForInsight(roomId="${roomId}");`, insightId);

						await runPixel(
						  `LLM(engine="${MODEL_ID}", roomId="${roomId}", command=["Hello"]);`,
						  insightId,
						);
						```

						Read the history back with `GetPlaygroundMessages(roomId=["${roomId}"]);`. It returns one object per turn carrying `io` (`INPUT` or `OUTPUT`), `messageId`, `parentMessageId`, and the same `parts` array documented in the Pixel section.

						Streaming a Response

						`runPixel` buffers the whole answer and returns only the final value. To render tokens as they arrive, submit the job with `runPixelAsync`, poll `getPixelJobStreaming` with the returned `jobId` until a chunk carries `finish_reason`, then collect the final envelope with `getPixelAsyncResult`.

						```typescript
						import {
						  runPixelAsync,
						  getPixelJobStreaming,
						  getPixelAsyncResult,
						} from "@semoss/sdk";

						const { jobId } = await runPixelAsync(
						  `LLM(engine="${MODEL_ID}", command=["Sample Question"]);`,
						);

						let done = false;
						while (!done) {
						  const { message, status } = await getPixelJobStreaming(jobId);
						  for (const chunk of message) {
						    if (chunk.stream_type === "content" && chunk.data.content) {
						      appendToUi(chunk.data.content);
						    }
						    if (chunk.data.finish_reason) done = true;
						  }
						  if (status === "Complete" || status === "Error") done = true;
						  if (!done) await new Promise((resolve) => setTimeout(resolve, 500));
						}

						const { errors, results } = await getPixelAsyncResult(jobId);
						```

						Chunks arrive with a `stream_type` of `content`, `thinking`, or `tool`, matching the `TEXT`, `THINKING`, and `TOOL_CALL` parts of the final message.

						Embeddings

						```
						Embeddings(engine="${MODEL_ID}", values=["Sample String 1", "Sample String 2"], paramValues=[{}]);
						```

						`pixelReturn[0].output.response` holds one vector per input string.

						Listing Available Models

						Use `MyEngines` with `engineTypes=["MODEL"]` to show only the models the current user can reach, then feed the chosen `engine_id` into the calls above.

						```typescript
						const { errors, pixelReturn } = await runPixel(
						  `MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const models = pixelReturn[0].output as Array<{
						  engine_id: string;
						  engine_name: string;
						  engine_display_name: string;
						  engine_subtype: string; // for example "CLAUDE", "OPEN_AI", "VERTEX"
						  engine_favorite: 0 | 1;
						}>;
						```

						`MyEngines` also accepts `filterWord=["claude"]` (substring match on the name), `onlyFavorites=[true]`, and `sort={"ENGINENAME": "ASC"}` (or `DATECREATED`, with `ASC`/`DESC`). Omit `limit` and `offset` to return everything. Read the `engine_*` fields; the `app_*` and `database_*` fields carry the same values but are legacy aliases.
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
						A **Storage** engine is a file or object store, for example a cloud bucket or a mounted filesystem, exposed behind one consistent interface. You address content by a `storagePath` inside the engine and move it to and from a local `filePath`.

						## What you can do

						- **List** - the names under a path, or full details for each entry (name, size, mime type, modified time, metadata).
						- **Download** - pull a single file, or sync a whole folder down to local.
						- **Upload** - push a single file, or sync a local folder up. Uploads can attach arbitrary key-value `metadata`.
						- **Delete** - remove a path, optionally leaving the folder structure in place.

						## Key concepts

						- **Paths, not handles.** Every operation takes a `storagePath` relative to the engine root. There is nothing to open or close.
						- **Metadata travels with the file.** Key-value pairs attached at upload come back on the detail listing.
						- **Transfers report a boolean.** Upload, download, sync, and delete return `true` on success and surface failures as an error rather than a partial result.

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

		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL,
				"""
						Getting Started

						`runPixel` from `@semoss/sdk` submits Pixel from an app front end and returns the parsed envelope. Storage pixels put their payload directly at `pixelReturn[0].output` rather than wrapping it in a tabular `data` object.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const STORAGE_ID = "<engineid>";

						const { errors, pixelReturn } = await runPixel(
						  `Storage(storage="${STORAGE_ID}") | ListStoragePath(storagePath="/your/storage/path");`,
						);

						if (errors.length) throw new Error(errors[0]);

						const paths = pixelReturn[0].output as string[];
						```

						`errors` already contains any expression the server flagged with `operationType` `["ERROR"]`, so this one check is enough. `runPixel(pixel, insightId)` takes an optional second argument: pass `"new"` to start a fresh insight and reuse the returned `insightId` for the rest of the session, or omit it to run on the app's current insight.

						The variations below show only the Pixel string, the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call and the `errors` check are the same as above.

						List Path Details

						Returns one object per file/folder with `Path`, `Name`, `Size` (bytes), `MimeType`, `ModTime` (null for folders), `IsDir`, and `Metadata` (an empty map when none exists).

						```
						Storage(storage="${STORAGE_ID}") | ListStoragePathDetails(storagePath="/your/storage/path");
						```

						Download from Storage

						```
						Storage(storage="${STORAGE_ID}") | PullFromStorage(storagePath="/your/storage/path", filePath="/your/local/path");
						```

						Upload to Storage

						```
						Storage(storage="${STORAGE_ID}") | PushToStorage(storagePath="/your/storage/path", filePath="/your/local/path", metadata=[{"metaKey":"metaValue"}]);
						```

						Sync a Folder

						```
						Storage(storage="${STORAGE_ID}") | SyncStorageToLocal(storagePath="/your/storage/path", filePath="/your/local/path");
						Storage(storage="${STORAGE_ID}") | SyncLocalToStorage(storagePath="/your/storage/path", filePath="/your/local/path", metadata=[{"metaKey":"metaValue"}]);
						```

						Delete from Storage

						```
						Storage(storage="${STORAGE_ID}") | DeleteFromStorage(storagePath="/your/storage/path", leaveFolderStructure=false);
						```

						The transfer and delete operations return a boolean `true` in `output` on success and surface failures through `errors`.

						Listing Available Storage Engines

						Use `MyEngines` with `engineTypes=["STORAGE"]` to show only the storage engines the current user can reach, then feed the chosen `engine_id` into the calls above.

						```typescript
						const { errors, pixelReturn } = await runPixel(
						  `MyEngines(engineTypes=["STORAGE"], limit=[50], offset=[0]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const storageEngines = pixelReturn[0].output as Array<{
						  engine_id: string;
						  engine_name: string;
						  engine_display_name: string;
						  engine_subtype: string;
						  engine_favorite: 0 | 1;
						}>;
						```

						`MyEngines` also accepts `filterWord=["reports"]` (substring match on the name), `onlyFavorites=[true]`, and `sort={"ENGINENAME": "ASC"}` (or `DATECREATED`, with `ASC`/`DESC`). Omit `limit` and `offset` to return everything. Read the `engine_*` fields; the `app_*` and `database_*` fields carry the same values but are legacy aliases.
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
						A **Database** engine is a connected data source (RDBMS, RDF, graph, NoSQL, and others) queried behind one consistent interface. The examples below assume SQL; substitute SPARQL, Gremlin, or whichever dialect the underlying database speaks.

						## What you can do

						- **Read** - run a `SELECT` and get back rows plus per-column metadata. Select-style calls take a row `limit`.
						- **Write** - run `INSERT`, `UPDATE`, or `DELETE`. Modification calls take a `commit` flag instead.
						- **Inspect the schema** - fetch logical and physical table/column (or vertex/property) names with their data types. Useful both for building queries and for grounding an LLM.

						## Key concepts

						- **Results are tabular and positional.** `data.values` is an array of row arrays aligned to `data.headers`, and `headerInfo` describes each column's type.
						- **Logical and physical names can differ.** The catalog can alias a table or column, so the structure call returns both and lets you map between what users see and what the database stores.
						- **The statement type picks the path.** `SqlQuery` detects whether it was handed a read or a write and routes it accordingly, so the same command covers both.

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

		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL,
				"""
						Getting Started

						`runPixel` from `@semoss/sdk` submits Pixel from an app front end and returns the parsed envelope, so the tabular result is at `pixelReturn[0].output.data`.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const DATABASE_ID = "<engineid>";
						const sql = "SELECT ID, AGE, GENDER FROM DIABETES";

						const { errors, pixelReturn } = await runPixel(
						  `SqlQuery(database="${DATABASE_ID}", query="${sql}", limit=500);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const { headers, values } = pixelReturn[0].output.data;
						// headers: ["ID", "AGE", "GENDER"]
						// values:  [["1000", 59, "female"], ["1001", 68, "female"]]
						```

						`errors` already contains any expression the server flagged with `operationType` `["ERROR"]`, so this one check is enough. `output` is the same map documented in the Pixel section (`data.values`, `data.headers`, `data.rawHeaders`, `headerInfo`, `sources`, `numCollected`).

						`runPixel(pixel, insightId)` takes an optional second argument: pass `"new"` to start a fresh insight and reuse the returned `insightId` for the rest of the session, or omit it to run on the app's current insight.

						The variations below show only the Pixel string, the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call and the `errors` check are the same as above.

						Insert/Update/Delete Queries

						Pass `commit=true` instead of `limit`. `SqlQuery` auto-detects the statement type, so the same pixel handles any modification.

						```
						SqlQuery(database="${DATABASE_ID}", query="UPDATE table_name SET column1 = value1 WHERE condition", commit=true);
						```

						Dynamic SQL

						`SqlQueryBase64` takes the same arguments as `SqlQuery`; only the query format changes to a base64-encoded UTF-8 SQL string. Use it when the SQL carries quotes, newlines, or non-ASCII characters that are awkward to embed in a Pixel literal. Escape SQL string literals yourself - base64 protects the Pixel transport, not against SQL injection.

						```typescript
						function encodeUtf8Base64(value: string): string {
						  const bytes = new TextEncoder().encode(value);
						  let binary = "";
						  for (const byte of bytes) binary += String.fromCharCode(byte);
						  return btoa(binary);
						}

						function sqlString(value: string): string {
						  return `'${value.replace(/'/g, "''")}'`;
						}

						const mutationSql = `INSERT INTO CUSTOMER_NOTES (CUSTOMER_NAME, NOTES) VALUES (${sqlString(customerName)}, ${sqlString(notes)});`;

						const { errors } = await runPixel(
						  `SqlQueryBase64(database="${DATABASE_ID}", query="${encodeUtf8Base64(mutationSql)}", commit=true);`,
						);

						if (errors.length) throw new Error(errors[0]);
						```

						When a write fails after it may have started, read the database state back before retrying - a blind retry can duplicate a partially completed mutation.

						Get Database Structure

						```
						GetDatabaseTableStructure(database="${DATABASE_ID}");
						```

						Each row in `output.data.values` is a 6-tuple: logical table (RDBMS) or vertex (graph) name, logical column or property name, data type, whether the row is a graph vertex itself rather than a property on it, physical column/property name, and physical table/vertex name.

						Mapping Rows to Objects

						`data.values` is positional, so zip it against `data.headers` when you want records.

						```typescript
						const { headers, values } = pixelReturn[0].output.data;
						const records = values.map((row) =>
						  Object.fromEntries(headers.map((header, i) => [header, row[i]])),
						);
						```

						Listing Available Databases

						Use `MyEngines` with `engineTypes=["DATABASE"]` to show only the databases the current user can reach, then feed the chosen `engine_id` into the calls above.

						```typescript
						const { errors, pixelReturn } = await runPixel(
						  `MyEngines(engineTypes=["DATABASE"], limit=[50], offset=[0]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const databases = pixelReturn[0].output as Array<{
						  engine_id: string;
						  engine_name: string;
						  engine_display_name: string;
						  engine_subtype: string; // for example "H2_DB", "POSTGRES", "MYSQL", "TINKER"
						  engine_favorite: 0 | 1;
						}>;
						```

						`MyEngines` also accepts `filterWord=["sales"]` (substring match on the name), `onlyFavorites=[true]`, and `sort={"ENGINENAME": "ASC"}` (or `DATECREATED`, with `ASC`/`DESC`). Omit `limit` and `offset` to return everything. Read the `engine_*` fields; the `app_*` and `database_*` fields carry the same values but are legacy aliases.
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
						A **Vector** engine indexes documents as embeddings so you can search them by meaning rather than by keyword. It is the retrieval half of retrieval-augmented generation (RAG), and it handles the chunking and embedding for you.

						## What you can do

						- **Add documents** - point at uploaded files and the engine chunks, embeds, and indexes them. Pre-chunked VectorCSV files skip the default splitter.
						- **Search** - run a nearest-neighbor query in natural language, optionally scoped with `filters` (by source document) or `metaFilters` (by metadata attached at embed time).
						- **List and remove** - see which documents are currently indexed, and drop the ones you no longer want.

						## Key concepts

						- **You get chunks, not documents.** A search returns the closest chunks, each carrying its `Content` plus provenance (`Source`, `Divider`, `Part`) so an answer can cite where it came from.
						- **Files can live in several places.** Documents in the current insight or room are addressed by name; pass `space` to reach a project/app folder or the user space instead.
						- **Pair it with a Model engine.** The usual flow is retrieve, then hand the chunk text to an LLM as grounding context. The Pixel and JavaScript tabs show the full chain.

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

		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL,
				"""
						Getting Started

						`runPixel` from `@semoss/sdk` submits Pixel from an app front end and returns the parsed envelope. Vector pixels put their payload directly at `pixelReturn[0].output` rather than wrapping it in a tabular `data` object the way `SqlQuery` does.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const VECTOR_ID = "<engineid>";
						const query = "Sample Search Statement";

						const { errors, pixelReturn } = await runPixel(
						  `VectorDatabaseQuery(engine="${VECTOR_ID}", command="${query}", limit=5, filters=[], metaFilters=[]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const hits = pixelReturn[0].output as Array<{
						  Score: number;
						  Content: string;
						  Source: string;
						  Modality: string;
						  Divider: string;
						  Part: string;
						  Tokens: number;
						}>;
						```

						`errors` already contains any expression the server flagged with `operationType` `["ERROR"]`, so this one check is enough. `runPixel(pixel, insightId)` takes an optional second argument: pass `"new"` to start a fresh insight and reuse the returned `insightId` for the rest of the session, or omit it to run on the app's current insight.

						The variations below show only the Pixel string, the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call and the `errors` check are the same as above.

						Filtering

						`filters` matches the chunk's `Source`; `metaFilters` matches metadata keys attached at embed time. Both can be combined in the same call.

						```
						VectorDatabaseQuery(engine="${VECTOR_ID}", command="${query}", limit=5, filters=[Filter(Source == ["doc1.pdf", "doc2.pdf"])], metaFilters=[Filter(department == "finance")]);
						```

						List Indexed Documents

						Returns one entry per unique `Source` currently indexed, each with `fileName`, `fileSize`, and `lastModified`.

						```
						ListDocumentsInVectorDatabase(engine="${VECTOR_ID}");
						```

						Add Documents

						`space` is only needed when the files live outside the current insight: pass the project/app UUID, or `"user"` for the user space. `CreateEmbeddingsFromVectorCSVFile` takes csv (or zip-of-csv) files already chunked with the headers `Source`, `Modality`, `Divider`, `Part`, `Tokens`, `Content`.

						```
						CreateEmbeddingsFromDocuments(engine="${VECTOR_ID}", filePaths=["fileName1.pdf", "fileName2.pdf"]);
						CreateEmbeddingsFromDocuments(engine="${VECTOR_ID}", filePaths=["docs/file1.pdf"], space="app_id");
						CreateEmbeddingsFromVectorCSVFile(engine="${VECTOR_ID}", filePaths=["chunks.csv"]);
						```

						Embedding calls return a status string at `output` and a per-file breakdown at `pixelReturn[0].additionalOutput[0].output`, each entry carrying `fileName`, `status`, `insertedRecords`, `failedRecords`, and `totalRecords`.

						```typescript
						const perFile = pixelReturn[0].additionalOutput?.[0]?.output ?? [];
						const failed = perFile.filter((f) => f.status !== "SUCCESS");
						```

						Remove Indexed Documents

						Pass `fileNames`, the source identifiers listed by `ListDocumentsInVectorDatabase`, not file paths.

						```
						RemoveDocumentFromVectorDatabase(engine="${VECTOR_ID}", fileNames=["fileName1.pdf", "fileName2.pdf"]);
						```

						Retrieval-augmented Generation

						The usual flow is two calls: retrieve the closest chunks, then hand them to a Model engine as grounding context.

						```typescript
						const MODEL_ID = "your_model_engine_id";

						const { pixelReturn: retrieval } = await runPixel(
						  `VectorDatabaseQuery(engine="${VECTOR_ID}", command="${question}", limit=3);`,
						);

						const context = retrieval[0].output
						  .map((hit) => `* Document Name: ${hit.Source}, ${hit.Content}`)
						  .join("\\n");

						const prompt = `Answer the question using only the context below.

						Question: ${question}

						Context:
						${context}`;

						const { pixelReturn: answer } = await runPixel(
						  `LLM(engine="${MODEL_ID}", command=["${prompt}"], paramValues=[{"temperature":0.1}]);`,
						);

						const text = answer[0].output.response;
						```

						Ask the model to cite `Source` and `Part`/`Divider` so the answer points back at the retrieved chunks.

						Listing Available Vector Engines

						Use `MyEngines` with `engineTypes=["VECTOR"]` to show only the vector engines the current user can reach, then feed the chosen `engine_id` into the calls above.

						```typescript
						const { errors, pixelReturn } = await runPixel(
						  `MyEngines(engineTypes=["VECTOR"], limit=[50], offset=[0]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const vectorEngines = pixelReturn[0].output as Array<{
						  engine_id: string;
						  engine_name: string;
						  engine_display_name: string;
						  engine_subtype: string; // for example "FAISS", "CHROMA", "WEAVIATE"
						  engine_favorite: 0 | 1;
						}>;
						```

						`MyEngines` also accepts `filterWord=["policy"]` (substring match on the name), `onlyFavorites=[true]`, and `sort={"ENGINENAME": "ASC"}` (or `DATECREATED`, with `ASC`/`DESC`). Omit `limit` and `offset` to return everything. Read the `engine_*` fields; the `app_*` and `database_*` fields carry the same values but are legacy aliases.
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

						## What you can do

						- **Execute the tool** with a parameter map. The example calls in the other tabs come pre-filled with this engine's actual parameters.
						- **Read the contract** - this engine's parameters are published alongside these snippets in the `parameters` field, each with its `name`, `type`, `description`, and whether it is `required`.

						## Key concepts

						- **This is what tool use calls.** When a Model engine performs tool use, the tools it invokes are Function engines. Calling one directly runs exactly what the model would have run, which makes it the fastest way to test a tool in isolation.
						- **Parameters are named, not positional.** Pass a map keyed by parameter name; omit the optional ones.

						"""
						+ PLATFORM_INTRODUCTION,
				engineId);

		addUsage(usage, PIXEL, PIXEL_LABEL, """
				Execute Function
				```
				ExecuteFunctionEngine(engine = "<engineid>"<javastring>);
				```
				""".replace("<javastring>", pixelMapArg), engineId, paramInfo);

		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL,
				"""
						Getting Started

						`runPixel` from `@semoss/sdk` submits Pixel from an app front end and returns the parsed envelope, so the function result is at `pixelReturn[0].output`. The example call below is pre-filled with this engine's actual parameters.

						```typescript
						import { runPixel } from "@semoss/sdk";

						const FUNCTION_ID = "<engineid>";

						const { errors, pixelReturn } = await runPixel(
						  `ExecuteFunctionEngine(engine="${FUNCTION_ID}"<jsstring>);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const output = pixelReturn[0].output;
						```

						`errors` already contains any expression the server flagged with `operationType` `["ERROR"]`, so this one check is enough. `runPixel(pixel, insightId)` takes an optional second argument: pass `"new"` to start a fresh insight and reuse the returned `insightId` for the rest of the session, or omit it to run on the app's current insight.

						The engine's parameter contract is published alongside these snippets in the `parameters` field, where each entry carries `name`, `type`, `description`, and `required`.

						Listing Available Function Engines

						Use `MyEngines` with `engineTypes=["FUNCTION"]` to show only the function engines the current user can reach, then feed the chosen `engine_id` into the call above.

						```typescript
						const { errors, pixelReturn } = await runPixel(
						  `MyEngines(engineTypes=["FUNCTION"], limit=[50], offset=[0]);`,
						);

						if (errors.length) throw new Error(errors[0]);

						const functionEngines = pixelReturn[0].output as Array<{
						  engine_id: string;
						  engine_name: string;
						  engine_display_name: string;
						  engine_subtype: string;
						  engine_favorite: 0 | 1;
						}>;
						```

						`MyEngines` also accepts `filterWord=["weather"]` (substring match on the name), `onlyFavorites=[true]`, and `sort={"ENGINENAME": "ASC"}` (or `DATECREATED`, with `ASC`/`DESC`). Omit `limit` and `offset` to return everything. Read the `engine_*` fields; the `app_*` and `database_*` fields carry the same values but are legacy aliases.
						"""
						.replace("<jsstring>", pixelMapArg),
				engineId, paramInfo);

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
		addUsage(usage, INTRODUCTION, INTRODUCTION_LABEL,
				"""
						Detailed usage examples for this engine type have not been written yet. The platform notes below still apply - every engine is reachable the same way, whichever type it is.

						"""
						+ PLATFORM_INTRODUCTION,
				null);
		addUsage(usage, PIXEL, PIXEL_LABEL, "Documentation pending", null);
		addUsage(usage, JAVASCRIPT, JAVASCRIPT_LABEL, "Documentation pending", null);
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
				Builds tutorial-style usage snippets for a selected engine across Pixel, JavaScript/TypeScript, Python, Java, and optional integrations (for example LangChain or OpenAI-compatible usage when supported).

				Platform context (useful for both human readers and machine consumers): on the Semoss AI Server platform every capability is an *engine* registered in a shared catalog - Model (LLMs), Vector (semantic search), Database (SQL/graph), Storage (files), and Function (callable tools) - addressed by a stable `engineId`. Pixel is the server-side scripting language executed through the `runPixel` REST endpoint; the `@semoss/sdk` JavaScript package, the `ai_server` Python SDK, and the Java `Utility` helpers call the same engines. Model/chat calls are stateful via a *room* (insight) that holds conversation history.

				- Input resolution order is: `engine` first, then `type` if `engine` is not provided.
				- When only `type` is supplied, snippets are generated with `SAMPLE_ENGINE_ID` as the placeholder engine identifier.
				- The returned vector contains one object per usage channel with `type`, `label`, and `code`.
				- The first channel is always `type = "introduction"`: a markdown primer explaining what this engine type does plus a shared "how to reach this engine" platform section. The remaining channels (`pixel`, `javascript`, `python`, `java`, ...) are per-integration.
				- Each integration channel's `code` is markdown with per-operation example calls, each followed by an "Example Output" block showing the JSON/dict payload it returns. Pixel outputs show the full `runPixel` envelope (`pixelReturn[i].output`); Python SDK outputs show the unwrapped payload.
				- The `javascript` channel covers the `@semoss/sdk` front-end package: `runPixel` (which returns the parsed envelope plus a pre-collected `errors` array), insight sessions, engine discovery via `MyEngines`, and, for Model engines, room-threaded chat, file uploads, and streaming through `runPixelAsync`/`getPixelJobStreaming`.
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
