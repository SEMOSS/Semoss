package prerna.reactor.insights.recipemanagement;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RetrieveInsightPipelineReactor extends AbstractInsightReactor {

	public RetrieveInsightPipelineReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProject();
		String projectName = null;
		String rdbmsId = getRdbmsId();
		
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
			throw new IllegalArgumentException("User does not have access to this insight");
		}
		
		// get the app name
		projectName = Utility.getProject(projectId).getProjectName();

		// get the pipeline file
		File f = getPipelineFileLocation(projectId, projectName, rdbmsId);
		
		// no file exists
		if(!f.exists()) {
			return new NounMetadata(new HashMap<String, Object>(), PixelDataType.MAP, PixelOperationType.PIPELINE);
		}
		
		Map<String, Object> pipeline = null;
		
		FileReader reader = null;
		try {
			reader = new FileReader(f);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			pipeline = gson.fromJson(reader, new TypeToken<Map<String, Object>>(){}.getType());
		} catch(Exception e) {
			throw new IllegalArgumentException("An error occurred with reading the saved pipeline", e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return new NounMetadata(pipeline, PixelDataType.MAP, PixelOperationType.PIPELINE);
	}

}
