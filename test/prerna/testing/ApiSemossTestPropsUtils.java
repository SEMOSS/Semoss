/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.testing;

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
		Files.copy(ApiTestsSemossConstants.BASE_RDF_MAP, ApiTestsSemossConstants.TEST_RDF_MAP,
				StandardCopyOption.REPLACE_EXISTING);
		Me configurationManager = new Me();
		configurationManager.changeRDFMap(ApiTestsSemossConstants.TEST_BASE_DIRECTORY.replace('\\', '/'), "80",
				ApiTestsSemossConstants.TEST_RDF_MAP.toAbsolutePath().toString());
		DIHelper.getInstance().loadCoreProp(ApiTestsSemossConstants.TEST_RDF_MAP.toAbsolutePath().toString());

		Properties coreProps = DIHelper.getInstance().getCoreProp();
		coreProps.setProperty(Constants.PY_BASE_FOLDER, ApiTestsSemossConstants.BASE_DIRECTORY);

		// Just in case, manually override USE_PYTHON to be true for testing purposes
		// Warn if this was not the case to begin with
		if (!Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.USE_PYTHON))) {
			classLogger.warn("Python must be functional for local testing.");
			coreProps.setProperty(Constants.USE_PYTHON, "true");
			DIHelper.getInstance().setCoreProp(coreProps);
		}

		// override use r to be true
		// set jri to false
		// use user rserve

		coreProps.setProperty(Constants.USE_R, "true");
		coreProps.setProperty(Constants.R_CONNECTION_JRI, "true");
		coreProps.setProperty("IS_USER_RSERVE", "false");
		coreProps.setProperty("R_USER_CONNECTION_TYPE", "dedicated");

		DIHelper.getInstance().setCoreProp(coreProps);
	}

	private static void unloadDIHelper() {
		DIHelper.getInstance().loadCoreProp(ApiTestsSemossConstants.BASE_RDF_MAP.toAbsolutePath().toString());
		try {
			Files.delete(ApiTestsSemossConstants.TEST_RDF_MAP);
		} catch (IOException e) {
			classLogger.warn("Unable to delete " + ApiTestsSemossConstants.TEST_RDF_MAP, e);
		}
	}

	private static void unloadSocialProps() {
		SocialPropertiesUtil inst = SocialPropertiesUtil.getInstance();
		inst = null;

	}
}
