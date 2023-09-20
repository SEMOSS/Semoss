package prerna.engine.impl.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.LLMReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.storage.StorageReactor;

public class GetEngineUsageReactor extends AbstractReactor {

	private static final String PYTHON = "python";
	private static final String JAVA = "java";
	private static final String PIXEL = "pixel";
	
	AbstractReactor engineAbstractReactor;
	
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
			default:
				throw new SemossPixelException("Engine Type is undefined");
		}
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}
	
	private Map<String, String> getModelUsage(String engineId) {
		this.engineAbstractReactor = new LLMReactor();
		
		Map<String, String> usageMap = new HashMap<String, String>();
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
		this.engineAbstractReactor = new StorageReactor();
		Map<String, String> usageMap = new HashMap<String, String>();
		usageMap.put(PYTHON,"from gaas_gpt_storage import StorageEngine\r\n" + 
				"storageEngine = StorageEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
				"storageEngine.list(path = '/your/path/')\r\n" + 
				"storageEngine.listDetails(path = '/your/path/')\r\n" + 
				"storageEngine.syncLocalToStorage(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" +
				"storageEngine.syncStorageToLocal(localPath= 'your/local/path', storagePath = 'your/storage/path')\r\n" + 
				"storageEngine.copyToLocal(localFolderPath= 'your/local/file/path', storageFilePath = 'your/storage/file/path')\r\n" + 
				"storageEngine.deleteFromStorage(storagePath = 'your/storage/file/path')");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IStorage;\r\n" + 
				"IStorage storage = Utility.getStorage(\""+engineId+"\");");
		usageMap.put(PIXEL,"Storage(storage = \""+engineId+"\")");
		return usageMap;
	}
	
	private Map<String, String> getDatabaseUsage(String engineId) {
		this.engineAbstractReactor = new DatabaseReactor();
		
		Map<String, String> usageMap = new HashMap<String, String>();
		usageMap.put(PYTHON,"from gaas_gpt_database import DatabaseEngine\r\n" + 
				"databaseEngine = DatabaseEngine(engine_id = \""+engineId+"\", insight_id = '${i}')\r\n" +
				"databaseEngine.execQuery(query = 'SELECT * FROM table_name')\r\n" + 
				"databaseEngine.insertData(query = 'INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...)')\r\n" + 
				"databaseEngine.removeData(query = 'DELETE FROM table_name WHERE condition')");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IDatabase;\r\n" + 
				"IDatabase database = Utility.getDatabase(\""+engineId+"\");");
		usageMap.put(PIXEL,"Database(database = \""+engineId+"\")");
		return usageMap;
	}
}
