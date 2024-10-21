package prerna.reactor.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineUsageReactor extends AbstractReactor {

	private static final String TYPE = "type";
	private static final String LABEL = "label";
	private static final String CODE = "code";
	
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
					"## Generation\r\n" + 
							"LLM(engine = \""+engineId+"\", command = \"<encode>Sample Question</encode>\", paramValues=[{}]);\r\n" + 
							
							"\n## Embeddings\r\n" +
							"Embeddings(engine = \""+engineId+"\", values = [\"Sample String 1\", \"Sample String 2\"], paramValues=[{}]);"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"from gaas_gpt_model import ModelEngine\r\n" + 
							"model = ModelEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
							
							"\n# Generation\r\n" +
							"question = 'Sample Question'\r\n" +
							"output = model.ask(question = question)\r\n" +
							
							"\n# Embeddings\r\n" + 
							"text_arr = ['Sample String 1', 'Sample String 2']\r\n" +
							"model.embeddings(strings_to_embed = text_arr)"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IModelEngine;\r\n" + 
							"IModelEngine modelEngine = Utility.getModel(\""+engineId+"\");"
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
					"Storage(storage = \""+engineId+"\")|ListStoragePath(storagePath='/your/storage/path');\r\n"
							+ "Storage(storage = \""+engineId+"\")|ListStoragePathDetails(storagePath='/your/storage/path');\r\n"
							+ "Storage(storage = \""+engineId+"\")|PullFromStorage(storagePath='/your/storage/path', filePath='/your/local/path');\r\n"
							+ "Storage(storage = \""+engineId+"\")|PushToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);\r\n"
							+ "Storage(storage = \""+engineId+"\")|SyncStorageToLocal(storagePath='/your/storage/path', filePath='/your/local/path');\r\n"
							+ "Storage(storage = \""+engineId+"\")|SyncLocalToStorage(storagePath='/your/storage/path', filePath='/your/local/path', metadata=[{'metaKey':'metaValue'}]);\r\n"
							+ "Storage(storage = \""+engineId+"\")|DeleteFromStorage(storagePath='/your/storage/path', leaveFolderStructure=false);"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"from gaas_gpt_storage import StorageEngine\r\n" + 
							"storageEngine = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
							"storageEngine.list(storagePath = '/your/path/')\r\n" + 
							"storageEngine.listDetails(storagePath = '/your/path/')\r\n" + 
							"storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path', metadata={'metaKey':'metaValue'})\r\n" +
							"storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
							"storageEngine.copyToLocal(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path')\r\n" + 
							"storageEngine.copyToStorage(localPath= 'your/local/file/path', storagePath = 'your/storage/file/path', metadata={'metaKey':'metaValue'})\r\n" + 
							"storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path', leaveFolderStructure=False)"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IStorageEngine;\r\n" + 
							"IStorageEngine storage = Utility.getStorage(\""+engineId+"\");"
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
					"Database(database = \""+engineId+"\")|Query(\"<encode> your select query </encode>\")|Collect(500);\r\n"
							+ "Database(database = \""+engineId+"\")|Query(\"<encode> your insert/update/delete query </encode>\")|ExecQuery();"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"from gaas_gpt_database import DatabaseEngine\r\n" + 
							"databaseEngine = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
							"databaseEngine.execQuery(query = 'SELECT * FROM table_name')\r\n" + 
							"databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n" + 
							"databaseEngine.updateData(query = 'UPDATE table_name set column1=value1 WHERE condition')\r\n" + 
							"databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IDatabaseEngine;\r\n" + 
							"IDatabaseEngine database = Utility.getDatabase(\""+engineId+"\");"
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
					"## List all the documents the vector database currently comprises of ##\r\n" +
							"ListDocumentsInVectorDatabase (engine = \""+engineId+"\");\r\n" + 
							
							"\n## Add document(s) that have been uploaded to the insight ##\r\n" + 
							"CreateEmbeddingsFromDocuments (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);\r\n" +

							"\n## Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight ##\r\n" + 
							"CreateEmbeddingsFromVectorCSVFile (engine = \""+engineId+"\", filePaths = [\"fileName1.csv\", \"fileName2.csv\", ..., \"fileNameX.csv\"]);\r\n" +

							"\n## Perform a nearest neighbor search on the embedded documents ##\r\n" +
							"VectorDatabaseQuery (engine = \""+engineId+"\", command = \"Sample Search Statement\", limit = 5);\r\n" +
							
							"\n## Remove document(s) from the vector database ##\r\n" +
							"RemoveDocumentFromVectorDatabase (engine = \""+engineId+"\", filePaths = [\"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"# import vector engine class and initialize\r\nfrom gaas_gpt_vector import VectorEngine\r\n" + 
							"vectorEngine = VectorEngine(engine_id = \""+engineId+"\", insight_id = '${i}', insight_folder = '${if}')\r\n" +
							
							"\n# List all the documents the vector database currently comprises of\r\n" +
							"vectorEngine.listDocuments()\r\n" + 
							
							"\n# Add document(s) that have been uploaded to the insight\r\n" +
							"vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])\r\n" + 
							
							"\n# Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight\r\n" +
							"vectorEngine.addVectorCSVFile(file_paths = ['fileName1.csv', 'fileName2.csv', ..., 'fileNameX.csv'])\r\n" + 
							
							"\n# Perform a nearest neighbor search on the embedded documents\r\n" +
							"vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5)\r\n" + 
							
							"\n# Remove document(s) from the vector database\r\n" +
							"vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"// imports\r\nimport prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IVectorDatabaseEngine;\r\n\n" + 
							"// get the vector engine\r\nIVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(\""+engineId+"\");\r\n" + 
							
							"\n// List all the documents the vector database currently comprises of\r\n" +
							"vectorEngine.listDocuments(Map<String, Object> parameters)\r\n" + 
							
							"\n// Add document(s) that have been uploaded to the insight\r\n" +
							"vectorEngine.addDocument(List<String> filePaths, Map<String, Object> parameters);\r\n" + 
							
							"\n// Add the VectorCSVFile Formatted CSVs that have been uploaded to the insight\r\n" +
							"vectorEngine.addEmbeddings(List<String> filePaths, Insight insight, Map<String, Object> parameters);\r\n" + 
							
							"\n// Perform a nearest neighbor search on the embedded documents\r\n" +
							"vectorEngine.nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters);\r\n" + 
							
							"\n// Remove document(s) from the vector database\r\n" +
							"vectorEngine.removeDocument(List<String> fileNames, Map <String, Object> parameters);"
					);
			usage.add(usageMap);
		}
		return usage;
	}
	
	private List<Map<String, Object>> getFunctionUsage(String engineId) {
		List<Map<String, Object>> usage = new ArrayList<>();
		{
			Map<String, Object> usageMap = fillMap(
					PIXEL, 
					"How to use in Javascript",
					"ExecuteFunctionEngine(engine = \""+engineId+"\", map=[{'param1':'value1', ... , 'paramN':'valueN'}] )"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					PYTHON, 
					"How to use in Python",
					"from gaas_gpt_function import FunctionEngine \r\n" + 
							"function = FunctionEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" + 
							"output = function.execute({'param1':'value1', ... , 'paramN':'valueN'})"
					);
			usage.add(usageMap);
		}
		{
			Map<String, Object> usageMap = fillMap(
					JAVA, 
					"How to use in Java",
					"import prerna.util.Utility;\r\n" + 
							"import prerna.engine.api.IFunctionEngine;\r\n" + 
							"IFunctionEngine function = Utility.getFunction(\""+engineId+"\");"
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
