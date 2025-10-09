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

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
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
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };

        this.keyRequired = new int[]{1, 1, 1, 1, 1, 0};
    }
	
	
	@Override
	public NounMetadata execute() {		
		organizeKeys();
		
		String vectorDatabaseId = this.keyValue.get(ReactorKeysEnum.VECTORDB.getKey());
		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		String space = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
		String rootFolder =  AssetUtility.getRootFolderPath(this.insight, space, false);
		
        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }
        
        if (!SecurityEngineUtils.userCanViewEngine(user, vectorDatabaseId)) {
            throw new IllegalArgumentException("Model " + vectorDatabaseId + " does not exist or user does not have access to this model");
        }
		
		
		
		
		//grab path to json file
		String filePath = rootFolder + "/" + this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		filePath = Utility.normalizePath(filePath);
		File jsonFile = new File(rootFolder, filePath);
		
		//pull list of json tools from file
		Reader reader = null;
		try {
		reader = new FileReader(jsonFile);
		}
		catch (FileNotFoundException e) {
			throw new IllegalArgumentException("json file " + ReactorKeysEnum.FILE_PATH.getKey() + " does not exist");
		}
		List<Map<String, Object>> tools = GSON.fromJson(reader, new TypeToken<List<Map<String, Object>>>(){}.getType());
		
		//extract relevant fields from jsonFile into string list
		List<String> strippedTools = new ArrayList<String>();
		for(Map<String, Object> tool : tools) {
			
			StringBuilder sb = new StringBuilder();
			
			for(String field : this.store.getNoun("jsonFields").getAllStrValues()) {
				sb.append(field + ": " + tool.get(field) + "\n");
			}
			strippedTools.add(sb.toString());
		}

		//convert strings into vectors using embed engine
		IModelEngine model = Utility.getModel(modelId);
		EmbeddingsModelEngineResponse response = model.embeddings(strippedTools, insight, null);
		List<List<Double>> embeddingsList = response.getResponse();
		

		
		
		//Write vectors to a temp file.
		//TODO: implement vectorDB engine method which does not require file as input, then remove this section.
		File parentDir = new File(rootFolder);
		File embeddingFile = null;
		try {
			embeddingFile = File.createTempFile("embeddings" + "ADD_UUID", ".csv", parentDir);
			
			FileWriter writer = null;
			writer = new FileWriter(embeddingFile);
			
			for(List<Double> vector : embeddingsList) {
				StringBuilder sb = new StringBuilder();
				
				for(int i = 0; i < vector.size() - 1; i++) {
					sb.append(vector.get(i));
					if (i < vector.size()  - 1) sb.append(",");
				}
				
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}

		//add embeddings to vector database/
		try {
			IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(vectorDatabaseId);
			vectorDatabase.addEmbeddingFile(embeddingFile, insight, null);
			embeddingFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(e.getMessage());
		}
		
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
