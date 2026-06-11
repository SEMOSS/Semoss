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
package prerna.io.connector.antivirus;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class VirusScannerUtils {

	private static Logger classLogger = LogManager.getLogger(VirusScannerUtils.class);

	public static Map<String, Collection<String>> getViruses(String name, InputStream is) {
		if (Utility.isVirusScanningEnabled()) {
			long start = System.currentTimeMillis();
			IVirusScanner vs = VirusScannerFactory.getVirusScannerConnector();
			if (vs == null) {
				throw new IllegalArgumentException("Could not find virus scanner.");
			}

			Map<String, Collection<String>> viruses = vs.getViruses(name, is);
			long end = System.currentTimeMillis();
			classLogger.info("TIME TOOK: {} ms", (end - start));

			return viruses;
		} else {
			classLogger.warn("Virus scanner is disabled.");
			return new HashMap<String, Collection<String>>();
		}
	}

}
