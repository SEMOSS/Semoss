package prerna.reactor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;

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

	public GetEngineUsageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		List<Map<String, Object>> output;
		switch (engineType) {
		case DATABASE:
			output = getDatabaseUsage(engineId);
			break;
		case STORAGE:
			output = getStorageUsage(engineId);
			break;
		case MODEL:
			output = getModelUsage(engineId);
			break;
		case VECTOR:
			output = getVectorUsage(engineId);
			break;
		case FUNCTION:
			output = getFunctionUsage(engineId);
			break;
		default:
			output = getPendingUsage();
			break;
		}
		return new NounMetadata(output, PixelDataType.VECTOR);
	}

	private List<Map<String, Object>> getModelUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript",
					"""
							Generation
							
							
							roomId is used to maintain conversational history if that is enabled for a model.
							
							```python
							myRoom=UUID();
							LLM(engine = "<engineid>", roomId = myRoom, command = "<encode>Sample Question</encode>", paramValues=[{'max_completion_tokens':2000,'temperature':0.3}]);

							LLM ( engine = "<engineid>" , roomId = myRoom,  command = "<encode>Sample Question With Image", url = "https://your_image_url.com");
							LLM ( engine = "<engineid>" , roomId = myRoom,  command = "<encode>Sample Question With Image", image = "myImage.png");
							```

							Generation with ChatML

							```python
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

							```python
							Embeddings(engine = "<engineid>", values = ["Sample String 1", "Sample String 2"], paramValues=[{}]);
							```

							Additional parameters found at: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)
							"""
							.trim().replace("<engineid>", engineId)

			);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python",
					"""
							```python
							\"\"\"
						        Args:
						            - command (str): The command to send to the model.
						            - question (str): **Deprecated**. Use `command` instead.
						            - room_id (Optional[str]): Identifier for the room/conversation.
						            - context (Optional[str]): Context for the model (the system prompt).
						            - image (Optional[List]): List of base64 image data to provide to the model.
						            - url (Optional[List]): List of image URLs to provide to the model.
						            - use_history (Optional[bool]): Whether to provide the conversation history to the model on an individual call.
						            - param_dict (Optional[Dict]): Additional parameters.
						            - insight_id (Optional[str]): Identifier for insights.
						    \"\"\"
						    
						    
							from ai_server import ModelEngine
							model = ModelEngine(engine_id = "<engineid>")

							# Text Generation
							command = 'Sample Question'
							output = model.ask(command = command, param_dict={'max_completion_tokens':2000,'temperature':0.3})

							# Text Generation with Vision (if supported by model)
							command = 'Sample Command With Image'
							output = model.ask(command = command, url=['https://your_image_url.com'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
							output = model.ask(command = command, image=['base64_of_image'], param_dict={'max_completion_tokens':2000,'temperature':0.3})
							
							# Continue Conversation with Room ID
							command = 'Sample Question'
							room_id = 'my_room_id'
							output = model.ask(command = command, room_id= room_id, param_dict={'max_completion_tokens':2000,'temperature':0.3})
		
							# Structured Ouputs (if supported by model)
							command = 'Sample Command With Structured Output'
							json_schema = {
										    "type": "object",
										    "properties": {
										        "sample_property": {
										            "type": "array",
										            "items": {
										                "type": "object",
										                "properties": {
										                    "sample_property_1": {"type": "string"},
										                    "sample_property_2": {"type": "string"},
										                },
										                "required": ["sample_property_1", "sample_property_2"],
										            },
										        }
										    },
										    "required": ["sample_property"],
										}
							output = model.ask(command = command, param_dict={"schema": json_schema}) 

							# Geneartion with ChatML
							model.ask(question='ignore', param_dict=
							    {"full_prompt":[
							        {"role":"system", "content": "You are a helpful assistant."},
							        {"role": "user", "content": "Who won the world series in 2020?"},
							        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
							        {"role": "user", "content": "Where was it played?"}
							    ],
							    'max_completion_tokens':2000,
							    'temperature':0.3
							});

							# Embeddings
							text_arr = ['Sample String 1', 'Sample String 2']
							model.embeddings(strings_to_embed = text_arr)
							```

							Additional chat parameters found at: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap("LANGCHAIN", "How to use with Langchain API", """
					```python
					from ai_server import ModelEngine
					model = ModelEngine(engine_id = "<engineid>")

					# Generation
					langchain_llm = model.to_langchain_chat_model()
					question = 'Sample Question'
					output = langchain_llm.invoke(input = question)

					# Embeddings
					langchain_llm = model.to_langchain_embedder()
					text_arr = ['Sample String 1', 'Sample String 2']
					langchain_llm.embed_query(text = text_arr[0])
					langchain_llm.embed_documents(texts = text_arr)
					```
					""".trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap("OPENAI", "How to use externally with OpenAI API and our Python SDK",
					"""
							```python
							# import the ai platform package - requires user access/secret, service account, or bearer_token
							import ai_server
							server_connection=ai_server.ServerClient(
							    base="<the api endpoint>",         # example: https://{domain}/{direcotry/path segment}/Monolith/api
							    access_key="<your access key>",    # example: 'd0033d40-ea83-4083-96ce-17a01451f831'
							    secret_key="<your secret key>"     # example: 'c2b3fae8-20d1-458c-8565-30ae935c4dfb'
							)

							# import the openai package and httpx
							from openai import OpenAI
							import httpx as httpx
							http_client = httpx.Client()
							http_client.cookies=server_connection.cookies

							# setup openai to point to this running instance
							client = OpenAI(
							    api_key="EMPTY",
							    base_url=server_connection.get_openai_endpoint(),
							    default_headers=server_connection.get_auth_headers(),
							    http_client=http_client
							)

							# chat completitions using openai
							response = client.chat.completions.create(
							    model="<engineid>",
							    messages=[
							        {"role": "system", "content": "You are a helpful assistant."},
							        {"role": "user", "content": "Who won the world series in 2020?"},
							        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
							        {"role": "user", "content": "Where was it played?"}
							    ],
							    extra_body={"insight_id":server_connection.cur_insight}
							)

							# completitions using openai - note this is marked deprecated by openai
							response = client.completions.create(
							    model="<engineid>",
							    prompt="Write a tagline for an ice cream shop.",
							    extra_body={"insight_id":server_connection.cur_insight}
							)

							# embeddings using openai
							embeddings = client.embeddings.create(
							    model="<engineid>",
							    input=["Your text string goes here"],
							    extra_body={"insight_id":server_connection.cur_insight}
							)
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", """
					```java
					import prerna.util.Utility;
					import prerna.engine.api.IModelEngine;
					IModelEngine modelEngine = Utility.getModel("<engineid>");
					```
					""".trim().replace("<engineid>", engineId));

			usage.add(usageMap);
		}
		return usage;
	}

	private List<Map<String, Object>> getStorageUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript",
					"""
							```python
							Storage(storage = "<engineid>") | ListStoragePath(storagePath='/your/storage/path');
							Storage(storage = "<engineid>") | ListStoragePathDetails(storagePath='/your/storage/path');
							Storage(storage = "<engineid>") | PullFromStorage(storagePath='/your/storage/path', filePath='/your/local/path');
							Storage(storage = "<engineid>") | PushToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);
							Storage(storage = "<engineid>") | SyncStorageToLocal(storagePath='/your/storage/path', filePath='/your/local/path');
							Storage(storage = "<engineid>") | SyncLocalToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);
							Storage(storage = "<engineid>") | DeleteFromStorage(storagePath='/your/storage/path', leaveFolderStructure=false);
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python",
					"""
							```python
							from ai_server import StorageEngine
							storageEngine = StorageEngine(engine_id = "<engineid>")
							storageEngine.list(storagePath = '/your/path/')
							storageEngine.listDetails(storagePath = '/your/path/')
							storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path', metadata={'metaKey':'metaValue'})
							storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')
							storageEngine.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
							storageEngine.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path', metadata={'metaKey':'metaValue'})
							storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path', leaveFolderStructure=False)
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap("LANGCHAIN", "How to use with Langchain API",
					"""
							```python
							from ai_server import StorageEngine
							storage = StorageEngine(engine_id = "<engineid>")
							langhchain_storage = storage.to_langchain_storage()
							langhchain_storage.list(storagePath = '/your/path/')
							langhchain_storage.listDetails(storagePath = '/your/path/')
							langhchain_storage.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')
							langhchain_storage.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')
							langhchain_storage.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
							langhchain_storage.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')
							langhchain_storage.deleteFromStorage(storagePath = 'your/storage/file/path')
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", """
					```java
					import prerna.util.Utility;
					import prerna.engine.api.IStorageEngine;
					IStorageEngine storage = Utility.getStorage("<engineid>");
					```
					""".trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		return usage;
	}

	private List<Map<String, Object>> getDatabaseUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript",
					"""
							```python
							Database(database = "<engineid>")|Query("<encode> your select query </encode>")|Collect(500);
							Database(database = "<engineid>")|Query("<encode> your insert/update/delete query </encode>")|ExecQuery();
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python",
					"""
							```python
							from ai_server import DatabaseEngine
							databaseEngine = DatabaseEngine(engine_id = "<engineid>")
							databaseEngine.execQuery(query = 'SELECT * FROM table_name')
							databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')
							databaseEngine.updateData(query = 'UPDATE table_name set column1=value1 WHERE condition')
							databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap("LANGCHAIN", "How to use with Langchain API",
					"""
							```python
							from ai_server import DatabaseEngine
							database = DatabaseEngine(engine_id = "<engineid>")
							langhchain_db = database.to_langchain_database()
							langhchain_db.executeQuery(query = 'SELECT * FROM table_name')
							langhchain_db.insertQuery(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')
							langhchain_db.updateQuery(query = 'UPDATE table_name set column1=value1 WHERE condition')
							langhchain_db.removeQuery(query = 'DELETE FROM table_name WHERE condition')
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", """
					```java
					import prerna.util.Utility;
					import prerna.engine.api.IDatabaseEngine;
					IDatabaseEngine database = Utility.getDatabase("<engineid>");
					```
					""".trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		return usage;
	}

	private List<Map<String, Object>> getVectorUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript",
					"""
							List all the documents the vector database currently comprises of
							```python
							ListDocumentsInVectorDatabase (engine = "<engineid>");
							```
							Add document(s) that have been uploaded to the insight
							```python
							CreateEmbeddingsFromDocuments (engine = "<engineid>", filePaths = ["fileName1.pdf", "fileName2.pdf", ..., "fileNameX.pdf"]);
							```
							Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight
							```python
							CreateEmbeddingsFromVectorCSVFile (engine = "<engineid>", filePaths = ["fileName1.csv", "fileName2.csv", ..., "fileNameX.csv"]);
							```
							Perform a nearest neighbor search on the embedded documents
							```python
							##filters of the form Filter(Source == ["your document name 1", "your document name 2"])##
							##metaFilters of the form Filter( MetadataKey == "Metadata Value" )##
							VectorDatabaseQuery (engine = "<engineid>", command = "Sample Search Statement", limit = 5, filters=[], metaFilters=[]);
							```
							Remove document(s) from the vector database
							```python
							RemoveDocumentFromVectorDatabase (engine = "<engineid>", filePaths = ["fileName1.pdf", "fileName2.pdf", ..., "fileNameX.pdf"]);
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python",
					"""
							```python
							# import vector engine class and initialize
							from ai_server import VectorEngine
							vectorEngine = VectorEngine(engine_id = "<engineid>")

							# List all the documents the vector database currently comprises of
							vectorEngine.listDocuments()

							# Add document(s) that have been uploaded to the insight
							vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])

							# Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight
							vectorEngine.addVectorCSVFile(file_paths = ['fileName1.csv', 'fileName2.csv', ..., 'fileNameX.csv'])

							# Perform a nearest neighbor search on the embedded documents
							# filters is Optional[Dict] | Optional[str]
							# 	str of the form 'Filter(Source == ["your document name 1", "your document name 2"])'
							# 	dict of the form {"Source": ["constitution.pdf", "scientific_journal.pdf"] and comparator is assumed to be '=' for all values
							# metafilters is Optional[Dict] | Optional[str].
							# 	str of the form 'Filter( MetadataKey == "Metadata Value" )'
							# 	dict of the form {"age": [5,6,7]} and comparator is assumed to be '=' for all values
							vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5, param_dict={}, filters='', metafilters='')

							# Remove document(s) from the vector database
							vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])
							```
							"""
							.trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap("LANGCHAIN", "How to use with Langchain API", """
					```python
					from ai_server import VectorEngine
					vector = VectorEngine(engine_id = "<engineid>")
					langhchain_vector = vector.to_langchain_vector_store()
					langhchain_vector.listDocs()
					langhchain_vector.addDocs(file_paths = ['file1.pdf','file2.pdf',...])
					langhchain_vector.removeDocs(file_names = ['file1.pdf','file2.pdf',...])
					langhchain_vector.similaritySearch(query = 'Sample Search Statement', k=5)
					```
					""".trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", """
					```java
					// imports
					import prerna.util.Utility;
					import prerna.engine.api.IVectorDatabaseEngine;

					// get the vector engine
					IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase("<engineid>");

					// List all the documents the vector database currently comprises of
					vectorEngine.listDocuments(Map<String, Object> parameters)

					// Add document(s) that have been uploaded to the insight
					vectorEngine.addDocument(List<String> filePaths, Map<String, Object> parameters);

					// Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight
					vectorEngine.addEmbeddings(List<String> filePaths, Insight insight, Map<String, Object> parameters);

					// Perform a nearest neighbor search on the embedded documents
					vectorEngine.nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters);

					// Remove document(s) from the vector database
					vectorEngine.removeDocument(List<String> fileNames, Map <String, Object> parameters);
					```
					""".trim().replace("<engineid>", engineId));
			usage.add(usageMap);
		}
		return usage;
	}

	private List<Map<String, Object>> getFunctionUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		IFunctionEngine ife = Utility.getFunctionEngine(engineId);
		List<FunctionParameter> fps = ife.getParameters();
		if (fps == null) {
			fps = new ArrayList<>();
		}
		List<Map<String, Object>> paramInfo = new ArrayList<>();
		List<String> requiredParams = ife.getRequiredParameters();
		if (requiredParams == null) {
			requiredParams = new ArrayList<>();
		}

		boolean first = true;
		String mapParams = "";
		for (FunctionParameter fp : fps) {
			Map<String, Object> pinfo = new HashMap<>();
			String name = fp.getParameterName();
			String type = fp.getParameterType();
			String description = fp.getParameterDescription();

			if (requiredParams.contains(name)) {
				pinfo.put("required", true);
			} else {
				pinfo.put("required", false);
			}
			pinfo.put("name", name);
			pinfo.put("type", type);
			pinfo.put("description", description);

			paramInfo.add(pinfo);
			if (first) {
				mapParams = mapParams + "{";
				first = false;
			} else {
				mapParams = mapParams + ", ";
			}

			mapParams = mapParams + "\"" + name + "\":";

			if (type.equalsIgnoreCase("string")) {
				mapParams = mapParams + "\"string\"";
			} else {
				mapParams = mapParams + type;
			}

		}

		if (fps.size() == 0) {
			mapParams = "";
		} else {
			mapParams = mapParams + "}";
		}

		{
			String javaString = "";
			if (!mapParams.isEmpty()) {
				javaString = javaString + " , map=[" + mapParams + "] ";
			}
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript", """
					```python
					ExecuteFunctionEngine(engine = "<engineid>"<javastring>);
					```
					""".trim().replace("<engineid>", engineId).replace("<javastring>", javaString), paramInfo);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python", """
					```python
					from ai_server import FunctionEngine
					function = FunctionEngine(engine_id = "<engineid>")
					output = function.execute(<mapparams>)
					```
					""".trim().replace("<engineid>", engineId).replace("<mapparams>", mapParams), paramInfo);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", """
					```java
					import prerna.util.Utility;
					import prerna.engine.api.IFunctionEngine;
					IFunctionEngine function = Utility.getFunction("<engineid>");
					```
					""".trim().replace("<engineid>", engineId), paramInfo);
			usage.add(usageMap);
		}
		return usage;
	}

	private List<Map<String, Object>> getPendingUsage() {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(PIXEL, "How to use in Javascript", "Documentation pending");
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(PYTHON, "How to use in Python", "Documentation pending");
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(JAVA, "How to use in Java", "Documentation pending");
			usage.add(usageMap);
		}
		return usage;
	}

	/**
	 * 
	 * @param type
	 * @param label
	 * @param code
	 * @return
	 */
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
}
