package prerna.reactor.uservenv;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.om.UserVenv;
import prerna.util.Constants;
import prerna.util.PythonUtils;

public class ListLibrariesReactor extends AbstractReactor {
	
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
		
		List<UserVenv.LibraryInfo> pipListResult;
		UserVenv userVenv = this.insight.getUser().getUserVenv();
		
		try {
			pipListResult = userVenv.getLibraryList();
			return new NounMetadata(pipListResult, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		} catch(Exception e) {
			String errorMsg = "Error getting library list: " + e.getMessage();
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
	}
}
