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

	private static final String PIXEL_LABEL = "How to use in Pixel";
	private static final String PYTHON_LABEL = "How to use in Python";
	private static final String JAVA_LABEL = "How to use in Java";
	private static final String LANGCHAIN_LABEL = "How to use with LangChain API";
	private static final String OPENAI_LABEL = "How to use externally with OpenAI API (with or without our Python SDK)";
	private static final String ANTHROPIC_LABEL = "How to use externally with Anthropic API (chat generation)";
	private static final String OLLAMA_LABEL = "How to use externally with Ollama API (chat generation)";

	private static final String SAMPLE_ENGINE_ID = "SAMPLE_ENGINE_ID";

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

						Generation with Image

						```
						LLM(engine = "<engineid>", roomId = "my_room_id", command = "<encode>Sample Question With Image</encode>", url = "https://your_image_url.com");
						LLM(engine = "<engineid>", roomId = "my_room_id", command = "<encode>Sample Question With Image</encode>", image = "myImage.png");
						```

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

						Embeddings

						```
						Embeddings(engine = "<engineid>", values = ["Sample String 1", "Sample String 2"], paramValues=[{}]);
						```

						Additional parameters: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)
						""",
				engineId);

		addUsage(usage, PYTHON, PYTHON_LABEL,
				"""
						Method Parameters<br/>
						`command` (str): prompt sent to the model.<br/>
						`question` (str): deprecated, use `command`.<br/>
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

						Generation with Image / Vision

						Use only for models that support image input.

						```python
						prompt = 'Sample Command With Image'
						output = model.ask(command = prompt, url=['https://your_image_url.com'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
						output = model.ask(command = prompt, image=['base64_of_image'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
						```

						Continue Conversation with Room ID

						```python
						prompt = 'Sample Question'
						room_id = 'my_room_id'
						output = model.ask(command = prompt, room_id = room_id, param_dict={'max_completion_tokens':2000,'temperature':0.3})
						```

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

						Embeddings

						```python
						text_arr = ['Sample String 1', 'Sample String 2']
						model.embeddings(strings_to_embed = text_arr)
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
				Direct Client Setup (Without ai_server SDK)
				```python
				from openai import OpenAI

				# access key + secret key format
				client = OpenAI(
				    api_key="<accesskey>:<secretkey>",
				    base_url="<openaiendpoint>"
				)
				```

				Chat Completions (Without ai_server SDK)
				```python
				response = client.chat.completions.create(
				    model="<engineid>",
				    messages=[
				        {"role": "system", "content": "You are a helpful assistant."},
				        {"role": "user", "content": "Who won the world series in 2020?"}
				    ],
				    extra_body={"insight_id":"<optional insight id>"}
				)
				```

				Responses API (Without ai_server SDK)
				```python
				response = client.responses.create(
				    model="<engineid>",
				    instructions="You are a helpful assistant.",
				    input="Who won the world series in 2020?",
				    extra_body={"insight_id":"<optional insight id>"}
				)
				print(response.output_text)
				```

				Client Setup (With ai_server SDK)

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

				Chat Completions (With ai_server SDK)
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

				Responses API (With ai_server SDK)
				```python
				response = client.responses.create(
				    model="<engineid>",
				    instructions="You are a helpful assistant.",
				    input="Who won the world series in 2020?",
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				print(response.output_text)
				```

				Legacy Completions (Deprecated)
				```python
				response = client.completions.create(
				    model="<engineid>",
				    prompt="Write a tagline for an ice cream shop.",
				    extra_body={"insight_id":server_connection.cur_insight}
				)
				```

				Embeddings
				```python
				embeddings = client.embeddings.create(
				    model="<engineid>",
				    input=["Your text string goes here"],
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
				    ],
				    extra_body={"insight_id":"<optional insight id>"}
				)
				print(response.content[0].text)
				```

				Embeddings are not supported for this Anthropic route yet.
				""", engineId);

		addUsage(usage, OLLAMA, OLLAMA_LABEL, """
				Direct Client Setup
				```python
				from ollama import Client

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

				Embeddings are not supported for this Ollama route yet.
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
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						List Paths
						```
						Storage(storage = "<engineid>") | ListStoragePath(storagePath='/your/storage/path');
						```

						List Path Details<br/>
						Returns one object per file/folder with common keys:<br/>
						`Path`, `Name`, `Size`, `MimeType`, `ModTime`, `IsDir`, `Metadata`.<br/>
						`Metadata` is a key-value map (empty map when none exists).
						```
						Storage(storage = "<engineid>") | ListStoragePathDetails(storagePath='/your/storage/path');
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

						List Path Details<br/>
						Returns one object per file/folder with common keys:<br/>
						`Path`, `Name`, `Size`, `MimeType`, `ModTime`, `IsDir`, `Metadata`.
						```python
						storageEngine.listDetails(storagePath = '/your/path/')
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
						databaseEngine.execQuery(query = 'SELECT * FROM table_name')
						```

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
		addUsage(usage, PIXEL, PIXEL_LABEL,
				"""
						List current vector documents (unique `Source` values)
						```
						ListDocumentsInVectorDatabase (engine = "<engineid>");
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

						Remove files from the vector index

						Use `fileNames` (source identifiers), not file paths.
						```
						RemoveDocumentFromVectorDatabase (engine = "<engineid>", fileNames = ["fileName1.pdf", "fileName2.pdf", ..., "fileNameX.pdf"]);
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

						Remove Indexed Documents

						Use `file_names` as source identifiers (for example names returned by `listDocuments()`).
						```python
						vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])
						```
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
				Builds sample usage snippets for a selected engine across Pixel, Python, Java, and optional integrations (for example LangChain or OpenAI-compatible usage when supported).

				- Input resolution order is: `engine` first, then `type` if `engine` is not provided.
				- When only `type` is supplied, snippets are generated with `SAMPLE_ENGINE_ID` as the placeholder engine identifier.
				- The returned vector contains one object per usage channel with `type`, `label`, and `code`.
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
