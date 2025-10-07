package prerna.playground.reactors;

import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.FileNotFoundException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * This is a temporary reactor. It's functionality can be
 * integrated elsewhere in a more cohesive manner.
 */
public class AddJsonToVectorDatabaseReactor extends AbstractReactor{

    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

    public AddJsonToVectorDatabaseReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.VECTORDB.getKey(),
            ReactorKeysEnum.MODEL.getKey(),
            ReactorKeysEnum.FILE_PATH.getKey(),
            ReactorKeysEnum.SPACE.getKey(), 
            "jsonFields",
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 3, not sure what this is for
        };

        this.keyRequired = new int[]{1, 1, 1, 0};
    }
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String vectorDatabaseId = this.keyValue.get(ReactorKeysEnum.VECTORDB.getKey());
		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		String space = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
		String rootFolder =  AssetUtility.getRootFolderPath(this.insight, space, false);
		
		String filePath = rootFolder + "/" + this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		filePath = Utility.normalizePath(filePath);

		File jsonFile = new File(rootFolder, this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		
		Reader reader = null;
		try {
		reader = new FileReader(jsonFile);
		}
		catch (FileNotFoundException e) {
			throw new IllegalArgumentException("json file " + ReactorKeysEnum.FILE_PATH.getKey() + " does not exist");
		}
		
		List<Map<String, Object>> tools = GSON.fromJson(reader, new TypeToken<List<Map<String, Object>>>(){}.getType());
		
		List<String> strippedTools = new ArrayList<String>();
		for(Map<String, Object> tool : tools) {
			
			StringBuilder sb = new StringBuilder();
			
			for(String field : this.store.getNoun("jsonFields").getAllStrValues()) {
				sb.append(field + ": " + tool.get(field) + "\n");
			}
			strippedTools.add(sb.toString());
		}

		
		IModelEngine model = Utility.getModel(modelId);
		EmbeddingsModelEngineResponse response = model.embeddings(strippedTools, insight, null);
		List<List<Double>> embeddingsList = response.getResponse();
		

		IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(vectorDatabaseId);
		
		
		File parentDir = new File(rootFolder);
		try {
			File embeddingFile = File.createTempFile("embeddings" + "ADD_UUID", ".csv", parentDir);
			
			FileWriter writer = null;
			writer = new FileWriter(embeddingFile);
			
			for(List<Double> vector : embeddingsList) {
				StringBuilder sb = new StringBuilder();
				
				for(int i = 0; i < vector.size() - 1; i++) {
					sb.append(vector.get(i));
					if (i < vector.size()  - 1) sb.append(",");
				}
				
			}
			
			try {
				vectorDatabase.addEmbeddingFile(embeddingFile, insight, null);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(e.getMessage());
			}
			
			
			embeddingFile.delete();			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
