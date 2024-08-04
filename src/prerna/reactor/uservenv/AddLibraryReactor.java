package prerna.reactor.uservenv;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.om.UserVenv;
import prerna.util.Constants;

public class AddLibraryReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(AddLibraryReactor.class);
	
	public AddLibraryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.LIBRARY.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
		};
		this.keyRequired = new int[] {1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String library = this.keyValue.get(this.keysToGet[0]);
		String libInstallResult = "";
		UserVenv userVenv = this.insight.getUser().getUserVenv();
		
		// TODO: Check the socket status
		
		try {
		    libInstallResult = userVenv.installLibrary(this.insight, library);
		} catch (InterruptedException ie) {
		    libInstallResult = "There was a problem installing " + library;
		    classLogger.error(Constants.STACKTRACE, ie);
		} catch (IOException ioe) {
		    libInstallResult = "There was a problem installing " + library;
		    classLogger.error(Constants.STACKTRACE, ioe);
		}
		
		return new NounMetadata(libInstallResult , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
