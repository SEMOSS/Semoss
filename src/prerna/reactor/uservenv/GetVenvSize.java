package prerna.reactor.uservenv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.om.UserVenv;
import prerna.util.Constants;
import prerna.util.PythonUtils;

public class GetVenvSize extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(ListLibrariesReactor.class);
	
	@Override
	public NounMetadata execute() {
        try {
            PythonUtils.verifyPyCapabilities();
        } catch (IllegalArgumentException e) {
            String errorMsg = "Python capabilities verification failed: " + e.getMessage();
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
        }
        
        // Make sure an instance of Python is being served
        this.insight.getPyTranslator();
        
        Integer venvSize;
        UserVenv userVenv = this.insight.getUser().getUserVenv();
        
        try {
        	venvSize = userVenv.getSitePackagesSize();
        	String sizeMsg = venvSize.toString() + " mbs";
        	return new NounMetadata(sizeMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
        } catch(Exception e) {
			String errorMsg = "Error getting site package directory size: " + e.getMessage();
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
	}

}
