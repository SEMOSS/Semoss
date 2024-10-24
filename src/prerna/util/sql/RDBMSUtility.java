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

package prerna.util.sql;

import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.Utility;

public class RDBMSUtility {
	
	private RDBMSUtility() {
		
	}
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static String getH2BaseConnectionURL() {
		return "jdbc:h2:nio:" + "@" + Constants.BASE_FOLDER + "@" + DIR_SEPARATOR + "db" + DIR_SEPARATOR + "@" + Constants.ENGINE + "@"
				+ DIR_SEPARATOR + "database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	}

	public static String getH2BaseConnectionURL2() {
		return "jdbc:h2:nio:" + "@database@;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	}

	public static String fillParameterizedFileConnectionUrl(String baseURL, String engineId, String engineName) {
		if(engineId == null && engineName == null) {
			return baseURL;
		}
		
		if(baseURL == null || baseURL.isEmpty()) {
			baseURL = getH2BaseConnectionURL();
		}
		
		String baseFolder = Utility.getBaseFolder().replace('\\', '/');
		if(baseFolder.endsWith("/")) {
			baseFolder = baseFolder.substring(0, baseFolder.length()-1);
		}
		
		return baseURL.replace("@" + Constants.BASE_FOLDER + "@", baseFolder)
				.replace("@" + Constants.ENGINE + "@", SmssUtilities.getUniqueName(engineName, engineId));
	}
}