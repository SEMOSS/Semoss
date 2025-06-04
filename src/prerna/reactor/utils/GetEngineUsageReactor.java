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
	
	public GetEngineUsageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		List<Map<String, Object>> output;
		switch(engineType) {
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
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"Generation\r\n" + 
						"```\r\n"+
						"LLM(engine = \""+engineId+"\", command = \"<encode>Sample Question</encode>\", paramValues=[{'max_completion_tokens':2000,'temperature':0.3}]);\r\n" + 
						"```\r\n\r\n"+

					"Geneartion with ChatML\r\n" +
						"```\r\n"+
						"LLM(engine = \""+engineId+"\", command = \"<encode>ignore</encode>\", paramValues=[\r\n"+
						"    {\"full_prompt\":[\r\n"+
						"        {\"role\":\"system\", \"content\": \"You are a helpful assistant.\"},\r\n"+
						"        {\"role\": \"user\", \"content\": \"Who won the world series in 2020?\"},\r\n"+
						"        {\"role\": \"assistant\", \"content\": \"The Los Angeles Dodgers won the World Series in 2020.\"},\r\n"+
						"        {\"role\": \"user\", \"content\": \"Where was it played?\"}\r\n"+ 
						"    ],\r\n"+
						"    'max_completion_tokens':2000,\r\n"+
						"    'temperature':0.3\r\n"+
						"    }]);\r\n"+
						"```\r\n\r\n"+
							
					"Embeddings\r\n" +
						"```\r\n"+
						"Embeddings(engine = \""+engineId+"\", values = [\"Sample String 1\", \"Sample String 2\"], paramValues=[{}]);\r\n" +
						"```\r\n\r\n" +
						
					"Additional parameters found at: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)\r\n\r\n"

					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"```python\r\n"+
						"from ai_server import ModelEngine\r\n" + 
						"model = ModelEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						
						"\n# Generation\r\n" +
						"question = 'Sample Question'\r\n" +
						"output = model.ask(question = question, param_dict={'max_completion_tokens':2000,'temperature':0.3})\r\n" +

						"\r\n# Geneartion with ChatML\r\n" +
						"model.ask(question='ignore', param_dict=\r\n"+
						"    {\"full_prompt\":[\r\n"+
						"        {\"role\":\"system\", \"content\": \"You are a helpful assistant.\"},\r\n"+
						"        {\"role\": \"user\", \"content\": \"Who won the world series in 2020?\"},\r\n"+
						"        {\"role\": \"assistant\", \"content\": \"The Los Angeles Dodgers won the World Series in 2020.\"},\r\n"+
						"        {\"role\": \"user\", \"content\": \"Where was it played?\"}\r\n"+
						"    ],\r\n"+
						"    'max_completion_tokens':2000,\r\n"+
						"    'temperature':0.3\r\n"+
						"    });\r\n"+
						
						"\n# Embeddings\r\n" + 
						"text_arr = ['Sample String 1', 'Sample String 2']\r\n" +
						"model.embeddings(strings_to_embed = text_arr)\r\n"+
					"```\r\n\r\n" +
					"Additional chat parameters found at: [OpenAI Parameter Spec](https://platform.openai.com/docs/api-reference/chat/create)\r\n\r\n"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					"LANGCHAIN", 
					"How to use with Langchain API",
					"```python\r\n"+
						"from ai_server import ModelEngine\r\n" + 
						"model = ModelEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						
						"\n# Generation\r\n" +
						"langhchain_llm = model.to_langchain_chat_model()\r\n" +
						"question = 'Sample Question'\r\n" +
						"output = langhchain_llm.invoke(input = question)\r\n" +
						
						"\n# Embeddings\r\n" + 
						"langhchain_llm = model.to_langchain_embedder()\r\n" +
						"text_arr = ['Sample String 1', 'Sample String 2']\r\n" +
						"langhchain_llm.embed_query(text = text_arr[0])\r\n"+
						"langhchain_llm.embed_documents(texts = text_arr)\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					"OPENAI", 
					"How to use externally with OpenAI API and our Python SDK",
					"```python\r\n"
					+"# import the ai platform package - requires user access/secret, service account, or bearer_token"
					+ "\r\n"
					+ "import ai_server\r\n"
					+ "server_connection=ai_server.ServerClient(\r\n"
					+ "    base=\"<the api endpoint>\",         # example: https://{domain}/{direcotry/path segment}/Monolith/api\r\n"
					+ "    access_key=\"<your access key>\",    # example: 'd0033d40-ea83-4083-96ce-17a01451f831'\r\n"
					+ "    secret_key=\"<your secret key>\"     # example: 'c2b3fae8-20d1-458c-8565-30ae935c4dfb'\r\n"
					+ ")"
					+ "\r\n"
					+ "\r\n"
					+ "# import the openai package and httpx\r\n"
					+ "from openai import OpenAI\r\n"
					+ "import httpx as httpx\r\n"
					+ "http_client = httpx.Client()\r\n"
					+ "http_client.cookies=server_connection.cookies\r\n"
					+ "\r\n"
					+ "# setup openai to point to this running instance\r\n"
					+ "client = OpenAI(\r\n"
					+ "    api_key=\"EMPTY\",\r\n"
					+ "    base_url=server_connection.get_openai_endpoint(),\r\n"
					+ "    default_headers=server_connection.get_auth_headers(),\r\n"
					+ "    http_client=http_client\r\n"
					+ ")"
					+ "\r\n"
					+ "\r\n"
					+ "# chat completitions using openai\r\n"
					+ "response = client.chat.completions.create(\r\n"
					+ "    model=\""+engineId+"\",\r\n"
					+ "    messages=[\r\n"
					+ "        {\"role\": \"system\", \"content\": \"You are a helpful assistant.\"},\r\n"
					+ "        {\"role\": \"user\", \"content\": \"Who won the world series in 2020?\"},\r\n"
					+ "        {\"role\": \"assistant\", \"content\": \"The Los Angeles Dodgers won the World Series in 2020.\"},\r\n"
					+ "        {\"role\": \"user\", \"content\": \"Where was it played?\"}\r\n"
					+ "    ],\r\n"
					+ "    extra_body={\"insight_id\":server_connection.cur_insight}\r\n"
					+ ")"
					+ "\r\n"
					+ "\r\n"
					+ "# completitions using openai - note this is marked deprecated by openai\r\n"
					+ "response = client.completions.create(\r\n"
					+ "    model=\""+engineId+"\",\r\n"
					+ "    prompt=\"Write a tagline for an ice cream shop.\",\r\n"
					+ "    extra_body={\"insight_id\":server_connection.cur_insight}\r\n"
					+ ")"
					+ "\r\n"
					+ "\r\n"
					+ "# embeddings using openai\r\n"
					+ "embeddings = client.embeddings.create(\r\n"
					+ "    model=\""+engineId+"\",\r\n"
					+ "    input=[\"Your text string goes here\"],\r\n"
					+ "    extra_body={\"insight_id\":server_connection.cur_insight}\r\n"
					+ ")\r\n"
					+ "```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"```java\r\n" +
							"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IModelEngine;\r\n" + 
							"IModelEngine modelEngine = Utility.getModel(\""+engineId+"\");\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		return usage;
	}
	
	private List<Map<String, Object>> getStorageUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"```\r\n" 
							+ "Storage(storage = \""+engineId+"\") | ListStoragePath(storagePath='/your/storage/path');\r\n"
							+ "Storage(storage = \""+engineId+"\") | ListStoragePathDetails(storagePath='/your/storage/path');\r\n"
							+ "Storage(storage = \""+engineId+"\") | PullFromStorage(storagePath='/your/storage/path', filePath='/your/local/path');\r\n"
							+ "Storage(storage = \""+engineId+"\") | PushToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);\r\n"
							+ "Storage(storage = \""+engineId+"\") | SyncStorageToLocal(storagePath='/your/storage/path', filePath='/your/local/path');\r\n"
							+ "Storage(storage = \""+engineId+"\") | SyncLocalToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);\r\n"
							+ "Storage(storage = \""+engineId+"\") | DeleteFromStorage(storagePath='/your/storage/path', leaveFolderStructure=false);\r\n"
					+ "```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"```python\r\n"+
						"from ai_server import StorageEngine\r\n" + 
						"storageEngine = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						"storageEngine.list(storagePath = '/your/path/')\r\n" + 
						"storageEngine.listDetails(storagePath = '/your/path/')\r\n" + 
						"storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path', metadata={'metaKey':'metaValue'})\r\n" +
						"storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
						"storageEngine.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')\r\n" + 
						"storageEngine.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path', metadata={'metaKey':'metaValue'})\r\n" + 
						"storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path', leaveFolderStructure=False)\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					"LANGCHAIN", 
					"How to use with Langchain API",
					"```python\r\n"+
						"from ai_server import StorageEngine\r\n" + 
						"storage = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						"langhchain_storage = storage.to_langchain_storage()\r\n" +
						"langhchain_storage.list(storagePath = '/your/path/')\r\n" + 
						"langhchain_storage.listDetails(storagePath = '/your/path/')\r\n" + 
						"langhchain_storage.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" +
						"langhchain_storage.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
						"langhchain_storage.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')\r\n" + 
						"langhchain_storage.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')\r\n" + 
						"langhchain_storage.deleteFromStorage(storagePath = 'your/storage/file/path')\r\n"+
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"```java\r\n" +
							"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IStorageEngine;\r\n" + 
							"IStorageEngine storage = Utility.getStorage(\""+engineId+"\");" + 
					"```"
					);
			usage.add(usageMap);
		}
		return usage;
	}
	
	private List<Map<String, Object>> getDatabaseUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"```\r\n"+
						"Database(database = \""+engineId+"\")|Query(\"<encode> your select query </encode>\")|Collect(500);\r\n" + 
						"Database(database = \""+engineId+"\")|Query(\"<encode> your insert/update/delete query </encode>\")|ExecQuery();\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"```python\r\n"+
							"from ai_server import DatabaseEngine\r\n" + 
							"databaseEngine = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
							"databaseEngine.execQuery(query = 'SELECT * FROM table_name')\r\n" + 
							"databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n" + 
							"databaseEngine.updateData(query = 'UPDATE table_name set column1=value1 WHERE condition')\r\n" + 
							"databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					"LANGCHAIN", 
					"How to use with Langchain API",
					"```python\r\n"+
						"from ai_server import DatabaseEngine\r\n" + 
						"database = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						"langhchain_db = database.to_langchain_database()\r\n" +
						"langhchain_db.executeQuery(query = 'SELECT * FROM table_name')\r\n"+
						"langhchain_db.insertQuery(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n"+
						"langhchain_db.updateQuery(query = 'UPDATE table_name set column1=value1 WHERE condition')\r\n"+
						"langhchain_db.removeQuery(query = 'DELETE FROM table_name WHERE condition')\r\n"+
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"```\r\n"+
							"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IDatabaseEngine;\r\n" + 
							"IDatabaseEngine database = Utility.getDatabase(\""+engineId+"\");\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		return usage;
	}
	
	private List<Map<String, Object>> getVectorUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"#### List all the documents the vector database currently comprises of ##\r\n" +
					"```\r\n" +
						"ListDocumentsInVectorDatabase (engine = \""+engineId+"\");\r\n" + 
					"```\r\n" +

					"\r\n#### Add document(s) that have been uploaded to the insight ##\r\n" + 
					"```\r\n" +
						"CreateEmbeddingsFromDocuments (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);\r\n" +
					"```\r\n" +

					"#### Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight ##\r\n" + 
					"```\r\n" +
						"CreateEmbeddingsFromVectorCSVFile (engine = \""+engineId+"\", filePaths = [\"fileName1.csv\", \"fileName2.csv\", ..., \"fileNameX.csv\"]);\r\n" +
					"```\r\n" +

					"\r\n#### Perform a nearest neighbor search on the embedded documents ##\r\n" +
					"```\r\n" +
						"##filters of the form Filter(Source == [\"your document name 1\", \"your document name 2\"])##\r\n" +
						"##metaFilters of the form Filter( MetadataKey == \"Metadata Value\" )##\r\n" +
						"VectorDatabaseQuery (engine = \""+engineId+"\", command = \"Sample Search Statement\", limit = 5, filters=[], metaFilters=[]);\r\n" +
					"```\r\n" +

					"\r\n#### Remove document(s) from the vector database ##\r\n" +
					"```\r\n" +
						"RemoveDocumentFromVectorDatabase (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);\r\n" +
					"```\r\n"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"```python\r\n"+
							"# import vector engine class and initialize\r\n" + 
							"from ai_server import VectorEngine\r\n" + 
							"vectorEngine = VectorEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
							
							"\n# List all the documents the vector database currently comprises of\r\n" +
							"vectorEngine.listDocuments()\r\n" + 
							
							"\n# Add document(s) that have been uploaded to the insight\r\n" +
							"vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])\r\n" + 
							
							"\n# Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight\r\n" +
							"vectorEngine.addVectorCSVFile(file_paths = ['fileName1.csv', 'fileName2.csv', ..., 'fileNameX.csv'])\r\n" + 
							
							"\n# Perform a nearest neighbor search on the embedded documents\r\n" +
							"# filters is Optional[Dict] | Optional[str]\r\n" + 
							"# \tstr of the form 'Filter(Source == [\"your document name 1\", \"your document name 2\"])'\r\n" +
							"# \tdict of the form {\"Source\": [\"constitution.pdf\", \"scientific_journal.pdf\"] and comparator is assumed to be '=' for all values\r\n" +
							"# metafilters is Optional[Dict] | Optional[str].\r\n" +
							"# \tstr of the form 'Filter( MetadataKey == \"Metadata Value\" )'\r\n" +
							"# \tdict of the form {\"age\": [5,6,7]} and comparator is assumed to be '=' for all values\r\n" +
							"vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5, param_dict={}, filters='', metafilters='')\r\n" + 
							
							"\n# Remove document(s) from the vector database\r\n" +
							"vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])\r\n"+
						"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					"LANGCHAIN", 
					"How to use with Langchain API",
					"```python\r\n"+
						"from ai_server import VectorEngine\r\n" + 
						"vector = VectorEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
						"langhchain_vector = vector.to_langchain_vector_store()\r\n" +
						"langhchain_vector.listDocs()\r\n" +
						"langhchain_vector.addDocs(file_paths = ['file1.pdf','file2.pdf',...])\r\n" +
						"langhchain_vector.removeDocs(file_names = ['file1.pdf','file2.pdf',...])\r\n" +
						"langhchain_vector.similaritySearch(query = 'Sample Search Statement', k=5)\r\n" +
					"```"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"```java\r\n" +
							"// imports\r\n" + 
							"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IVectorDatabaseEngine;\r\n\n" + 
							"// get the vector engine\r\n" + 
							"IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(\""+engineId+"\");\r\n" + 
							
							"\n// List all the documents the vector database currently comprises of\r\n" +
							"vectorEngine.listDocuments(Map<String, Object> parameters)\r\n" + 
							
							"\n// Add document(s) that have been uploaded to the insight\r\n" +
							"vectorEngine.addDocument(List<String> filePaths, Map<String, Object> parameters);\r\n" + 
							
							"\n// Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight\r\n" +
							"vectorEngine.addEmbeddings(List<String> filePaths, Insight insight, Map<String, Object> parameters);\r\n" + 
							
							"\n// Perform a nearest neighbor search on the embedded documents\r\n" +
							"vectorEngine.nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters);\r\n" + 
							
							"\n// Remove document(s) from the vector database\r\n" +
							"vectorEngine.removeDocument(List<String> fileNames, Map <String, Object> parameters);\r\n" + 
					"```"
					);
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
				javaString = javaString + ", map=[" + mapParams + "] ";
			}
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"```\r\n"+
					"ExecuteFunctionEngine(engine = \""+engineId+"\"" + javaString + ");\r\n" +
					"```",
          paramInfo
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"```python\r\n" +
							"from ai_server import FunctionEngine \r\n" + 
							"function = FunctionEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" + 
							"output = function.execute(" + mapParams + ")\r\n" +
					"```",
          paramInfo
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"```java\r\n" +
							"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IFunctionEngine;\r\n" + 
							"IFunctionEngine function = Utility.getFunction(\""+engineId+"\");\r\n" +
					"```",
          paramInfo
					);
			usage.add(usageMap);
		}
		return usage;
	}
	
	private List<Map<String, Object>> getPendingUsage() {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"Documentation pending"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"Documentation pending"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"Documentation pending"
					);
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
	
	/*
	 * Legacy structure
	 */
	
//	@Override
//	public NounMetadata execute() {
//		// get the selectors
//		this.organizeKeys();
//		String engineId = this.keyValue.get(this.keysToGet[0]);
//		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
//		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
//		Map<String, String> outputMap;
//		switch(engineType) {
//			case DATABASE:
//				outputMap = getDatabaseUsage(engineId);
//				break;
//			case STORAGE:
//				outputMap = getStorageUsage(engineId);
//				break;
//			case MODEL:
//				outputMap = getModelUsage(engineId);
//				break;
//			case VECTOR:
//				outputMap = getVectorUsage(engineId);
//				break;
//			case FUNCTION:
//				outputMap = getFunctionUsage(engineId);
//				break;
//			default:
//				outputMap = new HashMap<>();
//				outputMap.put(PYTHON, "Documentation pending");
//				outputMap.put(JAVA, "Documentation pending");
//				outputMap.put(PIXEL, "Documentation pending");
//				break;
//		}
//		return new NounMetadata(outputMap, PixelDataType.MAP);
//	}
//	
//	private Map<String, String> getModelUsage(String engineId) {
//		Map<String, String> usageMap = new HashMap<>();
//		usageMap.put(PYTHON,"from gaas_gpt_model import ModelEngine\r\n" + 
//				"question = 'Sample Question'\r\n" +
//				"model = ModelEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
//				"output = model.ask(question = question)");
//		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
//				"import prerna.engine.api.IModelEngine;\r\n" + 
//				"IModelEngine eng = Utility.getModel(\""+engineId+"\");");
//		usageMap.put(PIXEL,"LLM(engine = \""+engineId+"\", command = \"<encode>Sample Question</encode>\", paramValues = [ {} ] );");
//		
//		return usageMap;
//	}
//	
//	private Map<String, String> getStorageUsage(String engineId) {
//		Map<String, String> usageMap = new HashMap<>();
//		usageMap.put(PYTHON,"from gaas_gpt_storage import StorageEngine\r\n" + 
//				"storageEngine = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
//				"storageEngine.list(path = '/your/path/')\r\n" + 
//				"storageEngine.listDetails(path = '/your/path/')\r\n" + 
//				"storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" +
//				"storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
//				"storageEngine.copyToLocal(localFolderPath= 'your/local/file/path', storageFilePath = 'your/storage/file/path')\r\n" + 
//				"storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path')");
//		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
//				"import prerna.engine.api.IStorageEngine;\r\n" + 
//				"IStorageEngine storage = Utility.getStorage(\""+engineId+"\");");
//		usageMap.put(PIXEL,"Storage(storage = \""+engineId+"\")");
//		return usageMap;
//	}
//	
//	private Map<String, String> getDatabaseUsage(String engineId) {
//		Map<String, String> usageMap = new HashMap<>();
//		usageMap.put(PYTHON,"from gaas_gpt_database import DatabaseEngine\r\n" + 
//				"databaseEngine = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
//				"databaseEngine.execQuery(query = 'SELECT * FROM table_name')\r\n" + 
//				"databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n" + 
//				"databaseEngine.updateData(query = 'UPDATE table_name set column1=value1 WHERE condition')\r\n" + 
//				"databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')");
//		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
//				"import prerna.engine.api.IDatabaseEngine;\r\n" + 
//				"IDatabaseEngine database = Utility.getDatabase(\""+engineId+"\");");
//		usageMap.put(PIXEL,"Database(database = \""+engineId+"\")");
//		return usageMap;
//	}
//	
//	private Map<String, String> getVectorUsage(String engineId) {
//		Map<String, String> usageMap = new HashMap<>();
//		usageMap.put(PYTHON,"# import vector engine class and initialize\r\nfrom gaas_gpt_vector import VectorEngine\r\n" + 
//				"vectorEngine = VectorEngine(engine_id = \""+engineId+"\", insight_id = '${i}', insight_folder = '${if}')\r\n" +
//				"\n# Add document(s) that have been uploaded to the insight\r\n" +
//				"vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])\r\n" + 
//				"\n# Perform a nearest neighbor search on the embedded documents\r\n" +
//				"vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5)\r\n" + 
//				"\n# List all the documents the vector database currently comprises of\r\n" +
//				"vectorEngine.listDocuments()\r\n" + 
//				"\n# Remove document(s) from the vector database\r\n" +
//				"vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])");
//		usageMap.put(JAVA,"// imports\r\nimport prerna.util.Utility;\r\n" + 
//				"import prerna.engine.api.IVectorDatabaseEngine;\r\n\n" + 
//				"// get the vector engine\r\nIVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(\""+engineId+"\");\r\n" + 
//				"\n// Add document(s) that have been uploaded to the insight\r\n" +
//				"vectorEngine.addDocument(List<String> filePaths, Map <String, Object> parameters);\r\n" + 
//				"\n// Perform a nearest neighbor search on the embedded documents\r\n" +
//				"vectorEngine.nearestNeighbor(String searchStatement, Number limit, Map <String, Object> parameters);\r\n" + 
//				"\n// List all the documents the vector database currently comprises of\r\n" +
//				"vectorEngine.listDocuments(Map<String, Object> parameters)\r\n" + 
//				"\n// Remove document(s) from the vector database\r\n" +
//				"vectorEngine.removeDocument(List<String> fileNames, Map <String, Object> parameters);"
//				);
//		usageMap.put(PIXEL,"## Add document(s) that have been uploaded to the insight ##\r\n" + 
//				"CreateEmbeddingsFromDocuments (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);\r\n" +
//				"\n## Perform a nearest neighbor search on the embedded documents ##\r\n" +
//				"VectorDatabaseQuery (engine = \""+engineId+"\", command = \"Sample Search Statement\", limit = 5);\r\n" +
//				"\n## List all the documents the vector database currently comprises of ##\r\n" +
//				"ListDocumentsInVectorDatabase (engine = \""+engineId+"\");\r\n" + 
//				"\n## Remove document(s) from the vector database ##\r\n" +
//				"RemoveDocumentFromVectorDatabase (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);"
//				);
//		return usageMap;
//	}
//	
//	private Map<String, String> getFunctionUsage(String engineId) {
//		Map<String, String> usageMap = new HashMap<>();
//		usageMap.put(PYTHON,"from gaas_gpt_function import FunctionEngine \r\n" + 
//				"function = FunctionEngine(engine_id = \"f3a4c8b2-7f3e-4d04-8c1f-2b0e3dabf5e9\", insight_id = '${i}')\r\n" + 
//				"output = function.execute({'param1':'value1', ... , 'paramN':'valueN'})");
//		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
//				"import prerna.engine.api.IFunctionEngine;\r\n" + 
//				"IFunctionEngine function = Utility.getFunction(\""+engineId+"\");");
//		usageMap.put(PIXEL,"ExecuteFunctionEngine(engine = \""+engineId+"\", map=[{'param1':'value1', ... , 'paramN':'valueN'}] )");
//		return usageMap;
//	}
}
