package prerna.reactor.uservenv;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.PythonUtils;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.UserVenv;

public class CreateRequirementsFileReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(CreateRequirementsFileReactor.class);
	
    public CreateRequirementsFileReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    }

    @Override
    public NounMetadata execute() {
    	organizeKeys();
    	String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
    	
        try {
            PythonUtils.verifyPyCapabilities();
        } catch (IllegalArgumentException e) {
            String errorMsg = "Python capabilities verification failed: " + e.getMessage();
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
        }
        
        // Make sure an instance of Python is being served
        this.insight.getPyTranslator();
        
        UserVenv userVenv = this.insight.getUser().getUserVenv();
        
        String creationResults;
        
		try {
			creationResults = userVenv.createRequirementsFile(fileLocation);
		} catch (InterruptedException ie) {
			creationResults = ie.getMessage();
		    classLogger.error(Constants.STACKTRACE, ie);
		} catch (IOException ioe) {
			creationResults = ioe.getLocalizedMessage();
		    classLogger.error(Constants.STACKTRACE, ioe);
		}
		return new NounMetadata(creationResults, PixelDataType.CONST_STRING);
    }
}
