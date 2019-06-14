package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrieveInsightPipelineReactor extends AbstractInsightReactor {

	public RetrieveInsightPipelineReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey()};
	}	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = getApp();
		String appName = null;
		String rdbmsId = getRdbmsId();
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), appId, rdbmsId)) {
				throw new IllegalArgumentException("User does not have access to this insight");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		// get the app name
		appName = MasterDatabaseUtility.getEngineAliasForId(appId);

		// get the pipeline file
		File f = getPipelineFileLocation(appId, appName, rdbmsId);
		
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
			throw new IllegalArgumentException("An error occured with reading the saved pipeline", e);
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
