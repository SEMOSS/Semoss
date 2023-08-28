package prerna.solr.reactor;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class LoadEngineMetadataReactor extends AbstractSetMetadataReactor {
	
	private static final Logger classLogger = LogManager.getLogger(LoadEngineMetadataReactor.class);

	public LoadEngineMetadataReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if(!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		
		Map<String, Object> metadata = null;
		JsonReader jReader = null;
		FileReader fReader = null;
		try {
			Gson gson = new Gson();
			fReader = new FileReader(fileLocation);
			jReader = new JsonReader(fReader);
			metadata = gson.fromJson(jReader, Map.class);
	    } catch(IOException e) {
	    	classLogger.error(Constants.STACKTRACE, e);
	    } finally {
	    	if(fReader != null) {
	    		try {
					fReader.close();
				} catch (IOException e) {
			    	classLogger.error(Constants.STACKTRACE, e);
				}
	    	}
	    	if(jReader != null) {
	    		try {
					jReader.close();
				} catch (IOException e) {
			    	classLogger.error(Constants.STACKTRACE, e);
				}
	    	}
	    }
		
		String engineId = (String) metadata.remove("engineId");
		if(engineId == null) {
			// assume its in the filename
			String engineAliasAndId = FilenameUtils.getBaseName(fileLocation);
			if(engineAliasAndId.contains("__")) {
				engineId = engineAliasAndId.split("__")[1];
			} else {
				engineId = engineAliasAndId;
			}
		}
		
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
		}
		
		// check for invalid metakeys
		List<String> validMetakeys = SecurityEngineUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityEngineUtils.updateEngineMetadata(engineId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the engine"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Define metadata on an engine through a JSON file";
	}
	
}
