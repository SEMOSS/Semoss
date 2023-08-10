package prerna.engine.impl.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.storage.StorageReactor;
import prerna.util.DIHelper;
import prerna.engine.impl.model.LLMReactor;

public class GetEngineUsageReactor extends AbstractReactor {

	private static final String TYPE = "TYPE";
	private static final String PYTHON = "python";
	private static final String JAVA = "java";
	private static final String PIXEL = "pixel";
	AbstractReactor engineAbstractReactor;
	
	public GetEngineUsageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the selectors
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		String engineType = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + TYPE);
	
		Map<String, String> outputMap;
		switch(engineType) {
			case "DATABASE":
				outputMap = getDatabaseUsage(engineId);
				break;
			case "STORAGE":
				outputMap = getStorageUsage(engineId);
				break;
			case "MODEL":
				outputMap = getModelUsage(engineId);
				break;
			default:
				throw new SemossPixelException("Engine Type is undefined");
		}
		
		String[] reactorInputs = engineAbstractReactor.keysToGet;
		StringBuilder reactorKeyInfo = new StringBuilder(outputMap.get(PIXEL)).append("(");
		for (int i = 0; i < reactorInputs.length; i++) {
			String inputKey = reactorInputs[i];
			reactorKeyInfo.append(inputKey).append(" = ");
			if (inputKey.equals("engine") || inputKey.equals("database") || inputKey.equals("storage")) {
				reactorKeyInfo.append("\"").append(engineId).append("\"");
			} else {
				reactorKeyInfo.append("<").append("").append(">");
			}
			if (i == reactorInputs.length - 1) {
				reactorKeyInfo.append(");");
			} else {
				reactorKeyInfo.append(", ");
			}
		}
		outputMap.put(PIXEL, reactorKeyInfo.toString());
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}
	
	private Map<String, String> getModelUsage(String engineId) {
		this.engineAbstractReactor = new LLMReactor();
		
		Map<String, String> usageMap = new HashMap<String, String>();
		usageMap.put(PYTHON,"from gaas_gpt_model import ModelEngine\r\n" + 
				"model = ModelEngine(engine_id=\""+engineId+"\")");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IModelEngine;\r\n" + 
				"IModelEngine eng = Utility.getModel(\""+engineId+"\");");
		usageMap.put(PIXEL,"LLM");
		
		return usageMap;
	}
	
	private Map<String, String> getStorageUsage(String engineId) {
		this.engineAbstractReactor = new StorageReactor();
		Map<String, String> usageMap = new HashMap<String, String>();
		usageMap.put(PYTHON,"");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IStorage;\r\n" + 
				"IStorage storage = Utility.getStorage(\""+engineId+"\");");
		usageMap.put(PIXEL,"Storage");
		return usageMap;
	}
	
	private Map<String, String> getDatabaseUsage(String engineId) {
		this.engineAbstractReactor = new DatabaseReactor();
		
		Map<String, String> usageMap = new HashMap<String, String>();
		usageMap.put(PYTHON,"");
		usageMap.put(JAVA,"import prerna.util.Utility;\r\n" + 
				"import prerna.engine.api.IDatabase;\r\n" + 
				"IDatabase database = Utility.getDatabase(\""+engineId+"\");");
		usageMap.put(PIXEL,"Database");
		return usageMap;
	}
}
