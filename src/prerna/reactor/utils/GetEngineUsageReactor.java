package prerna.reactor.utils;

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
		Map<String, String> outputMap;
		switch(engineType) {
			case DATABASE:
				outputMap = getDatabaseUsage(engineId);
				break;
			case STORAGE:
				outputMap = getStorageUsage(engineId);
				break;
			case MODEL:
				outputMap = getModelUsage(engineId);
				break;
			case VECTOR:
				outputMap = getVectorUsage(engineId);
				break;
			default:
				outputMap = new HashMap<>();
				outputMap.put(PYTHON, "Documentation pending");
				outputMap.put(JAVA, "Documentation pending");
				outputMap.put(PIXEL, "Documentation pending");
				break;
		}
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}
	
	private Map<String, String> getModelUsage(String engineId) {
		Map<String, String> usageMap = new HashMap<>();
		usageMap.put(PYTHON,"from gaas_gpt_model import ModelEngine\r\n" + 
				"question = 'Sample Question'\r\n" +
				"model = ModelEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
				"output = model.ask(question = question)");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IModelEngine;\r\n" + 
				"IModelEngine eng = Utility.getModel(\""+engineId+"\");");
		usageMap.put(PIXEL,"LLM(engine = \""+engineId+"\", command = \"Sample Question\", paramValues = [ {} ] );");
		
		return usageMap;
	}
	
	private Map<String, String> getStorageUsage(String engineId) {
		Map<String, String> usageMap = new HashMap<>();
		usageMap.put(PYTHON,"from gaas_gpt_storage import StorageEngine\r\n" + 
				"storageEngine = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
				"storageEngine.list(path = '/your/path/')\r\n" + 
				"storageEngine.listDetails(path = '/your/path/')\r\n" + 
				"storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" +
				"storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
				"storageEngine.copyToLocal(localFolderPath= 'your/local/file/path', storageFilePath = 'your/storage/file/path')\r\n" + 
				"storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path')");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IStorageEngine;\r\n" + 
				"IStorage storage = Utility.getStorage(\""+engineId+"\");");
		usageMap.put(PIXEL,"Storage(storage = \""+engineId+"\")");
		return usageMap;
	}
	
	private Map<String, String> getDatabaseUsage(String engineId) {
		Map<String, String> usageMap = new HashMap<>();
		usageMap.put(PYTHON,"from gaas_gpt_database import DatabaseEngine\r\n" + 
				"databaseEngine = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
				"databaseEngine.execQuery(query = 'SELECT * FROM table_name')\r\n" + 
				"databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n" + 
				"databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IDatabaseEngine;\r\n" + 
				"IDatabase database = Utility.getDatabase(\""+engineId+"\");");
		usageMap.put(PIXEL,"Database(database = \""+engineId+"\")");
		return usageMap;
	}
	
	private Map<String, String> getVectorUsage(String engineId) {
		Map<String, String> usageMap = new HashMap<>();
		usageMap.put(PYTHON,"# initialize\r\nfrom gaas_gpt_vector import VectorEngine\r\n" + 
				"vectorEngine = VectorEngine(engine_id = \""+engineId+"\", insight_id = '${i}', insight_folder = '${if}')\r\n" +
				"\n# Add document(s) that have been uploaded to the insight\r\n" +
				"vectorEngine.addDocument(file_paths = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])\r\n" + 
				"\n# Perform a nearest neighbor search on the embedded documents\r\n" +
				"vectorEngine.nearestNeighbor(search_statement = 'Sample Search Statement', limit = 5)\r\n" + 
				"\n# List all the documents the vector database currently comprises of\r\n" +
				"vectorEngine.listDocuments()\r\n" + 
				"\n# Remove document(s) from the vector database\r\n" +
				"vectorEngine.removeDocument(file_names = ['fileName1.pdf', 'fileName2.pdf', ..., 'fileNameX.pdf'])");
		usageMap.put(JAVA,"// imports\r\nimport prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IVectorDatabaseEngine;\r\n\n" + 
				"// initialize\r\nIVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(\""+engineId+"\");\r\n" + 
				"\n// Add document(s) that have been uploaded to the insight\r\n" +
				"vectorEngine.addDocument(List<String> filePaths, Map <String, Object> parameters);\r\n" + 
				"\n// Perform a nearest neighbor search on the embedded documents\r\n" +
				"vectorEngine.nearestNeighbor(String searchStatement, Number limit, Map <String, Object> parameters);\r\n" + 
				"\n// List all the documents the vector database currently comprises of\r\n" +
				"vectorEngine.listDocuments(Map<String, Object> parameters)\r\n" + 
				"\n// Remove document(s) from the vector database\r\n" +
				"vectorEngine.removeDocument(List<String> fileNames, Map <String, Object> parameters);"
				);
		usageMap.put(PIXEL,"## Add document(s) that have been uploaded to the insight ##\r\n" + 
				"CreateEmbeddingsFromDocuments (engine = \""+engineId+"\", filePaths = \"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);\r\n" +
				"\n## Perform a nearest neighbor search on the embedded documents ##\r\n" +
				"VectorDatabaseQuery (engine = \""+engineId+"\", command = \"Sample Search Statement\", limit = 5);\r\n" +
				"\n## List all the documents the vector database currently comprises of ##\r\n" +
				"ListDocumentsInVectorDatabase (engine = \""+engineId+"\");\r\n" + 
				"\n## Remove document(s) from the vector database##\r\n" +
				"RemoveDocumentFromVectorDatabase (engine = \""+engineId+"\", filePaths = \"fileName1.pdf\", \"fileName2.pdf\", ..., \"fileNameX.pdf\"]);"
				);
		return usageMap;
	}
}
