package api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.configure.Me;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SocialPropertiesUtil;

public class ApiSemossTestPropsUtils {

protected static final Logger classLogger = LogManager.getLogger(ApiSemossTestPropsUtils.class);

	static void loadDIHelper() throws IOException {
		Files.copy(BaseSemossApiTests.BASE_RDF_MAP, BaseSemossApiTests.TEST_RDF_MAP, StandardCopyOption.REPLACE_EXISTING);
		Me configurationManager = new Me();
		configurationManager.changeRDFMap(BaseSemossApiTests.TEST_BASE_DIRECTORY.replace('\\', '/'), "80",
				BaseSemossApiTests.TEST_RDF_MAP.toAbsolutePath().toString());
		DIHelper.getInstance().loadCoreProp(BaseSemossApiTests.TEST_RDF_MAP.toAbsolutePath().toString());

		// Just in case, manually override USE_PYTHON to be true for testing purposes
		// Warn if this was not the case to begin with
		if (!Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.USE_PYTHON))) {
			classLogger.warn("Python must be functional for local testing.");
			Properties coreProps = DIHelper.getInstance().getCoreProp();
			coreProps.setProperty(Constants.USE_PYTHON, "true");
			DIHelper.getInstance().setCoreProp(coreProps);
		}

		//override use r to be true
		// set jri to false
		// use user rserve

		Properties corePropsR = DIHelper.getInstance().getCoreProp();
		corePropsR.setProperty(Constants.USE_R, "true");
		corePropsR.setProperty(Constants.R_CONNECTION_JRI, "true");
		corePropsR.setProperty("IS_USER_RSERVE", "false");
		corePropsR.setProperty("R_USER_CONNECTION_TYPE", "dedicated");
		DIHelper.getInstance().setCoreProp(corePropsR);


		// Turn tracking off while testing
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.T_ON))) {
			classLogger.info("Setting tracking off during unit tests.");
			Properties coreProps = DIHelper.getInstance().getCoreProp();
			coreProps.setProperty(Constants.T_ON, "false");
			DIHelper.getInstance().setCoreProp(coreProps);
		}
	}
	
	
	

	private static void unloadDIHelper() {
		DIHelper.getInstance().loadCoreProp(BaseSemossApiTests.BASE_RDF_MAP.toAbsolutePath().toString());
		try {
			Files.delete(BaseSemossApiTests.TEST_RDF_MAP);
		} catch (IOException e) {
			classLogger.warn("Unable to delete " + BaseSemossApiTests.TEST_RDF_MAP, e);
		}
	}
	
    private static void unloadSocialProps() {
    	SocialPropertiesUtil inst = SocialPropertiesUtil.getInstance();
    	inst = null;
		
	}
}
