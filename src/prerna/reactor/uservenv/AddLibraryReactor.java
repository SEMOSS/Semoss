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
import prerna.util.PythonUtils;

public class AddLibraryReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(AddLibraryReactor.class);
	
	public AddLibraryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.VERSION.getKey()
		};
		this.keyRequired = new int[] {1, 0};
	}
	
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
		
		organizeKeys();

		
		String library = this.keyValue.get(this.keysToGet[0]);
		String version = "";
		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			version = this.keyValue.get(this.keysToGet[1]);
		}
		
		String libInstallResult = "";
		UserVenv userVenv = this.insight.getUser().getUserVenv();
		
		try {
		    libInstallResult = userVenv.installLibrary(library, version);
		} catch (InterruptedException ie) {
		    libInstallResult = "There was a problem installing " + library + ie.getMessage();
		    classLogger.error(Constants.STACKTRACE, ie);
		} catch (IOException ioe) {
		    libInstallResult = "There was a problem installing " + library + ioe.getMessage();
		    classLogger.error(Constants.STACKTRACE, ioe);
		}
		
		return new NounMetadata(libInstallResult , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
	
}
