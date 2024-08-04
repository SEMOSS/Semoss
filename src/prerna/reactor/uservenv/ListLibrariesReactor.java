package prerna.reactor.uservenv;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.om.UserVenv;
import prerna.util.Constants;

public class ListLibrariesReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ListLibrariesReactor.class);
	
	@Override
	public NounMetadata execute() {
		List<UserVenv.LibraryInfo> pipListResult;
		UserVenv userVenv = this.insight.getUser().getUserVenv();
		
		// TODO: Check the socket status
		
		try {
			pipListResult = userVenv.pipList();
		} catch(InterruptedException ie) {
			String errorMsg = "There was a problem retrieving the list.";
			classLogger.error(Constants.STACKTRACE, ie);
			return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		} catch (IOException ioe) {
			String errorMsg = "There was a problem retrieving the list.";
			classLogger.error(Constants.STACKTRACE, ioe);
			return new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
		
		return new NounMetadata(pipListResult, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
