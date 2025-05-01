package prerna.reactor.vector;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class VectorAttachFileToSourceReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(VectorAttachFileToSourceReactor.class);

	public VectorAttachFileToSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.SOURCE.getKey()};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
		}
		
		IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(engineId);
		if (vectorDatabase == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		String vectorDbDocumentFilePath = vectorDatabase.getDocumentsFilesPath(null);
		String fileLocation = UploadInputUtility.getFilePath(this.store, this.insight);
		// if the user is attaching this 
		// they can determine the source name
		// or it will default to the name of the file uploaded
		String fileName = this.keyValue.get(ReactorKeysEnum.SOURCE.getKey());
		if(fileName == null || (fileName=fileName.trim()).isEmpty()) {
			fileName = FilenameUtils.getName(fileLocation);
		}
		
		String finalVectorDbFile = vectorDbDocumentFilePath + "/" + fileName;

		File source = new File(fileLocation);
		File dest = new File(finalVectorDbFile);
		try {
			FileUtils.copyFile(source, dest);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred while copying file. Detailed message = " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReactorDescription() {
		return "Attach a file as being the source for a vector database entry";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.ENGINE.getKey())) {
	        return "This is a required value for the vector database";
	    } else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
	        return "This is a required value containing the relative file path of a file";
	    } else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	        return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
	    } else if(key.equals(ReactorKeysEnum.SOURCE.getKey())) {
	    	return "This is an optional field to make sure the file is attached to the right source. The default will be to assume the uploaded file has the exact name as the source file name.";
	    }
	    return super.getDescriptionForKey(key);
	}
}