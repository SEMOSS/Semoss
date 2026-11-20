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
package prerna.io.connector.google;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.HttpHelperUtility;

public class GoogleFileRetriever implements IConnectorIOp {

	private static final Logger classLogger = LogManager.getLogger(GoogleFileRetriever.class);

	@Override
	public Object execute(User user, Map<String, Object> params) {
		String fileName = (String) params.remove("target");
		String url = "https://docs.google.com/spreadsheets/export";
		try (BufferedReader br = HttpHelperUtility.getHttpStream(url, null, params, false)) {
			// create a file
			File outputFile = new File(fileName);
			try (BufferedWriter target = new BufferedWriter(new FileWriter(outputFile))) {
				String data = null;
				while ((data = br.readLine()) != null) {
					target.write(data);
					target.write("\n");
					target.flush();
				}
			} catch (IOException e) {
				classLogger.error("Failed to retrieve/write Google spreadsheet content to {}", fileName, e);
			}
		} catch (IOException e) {
			classLogger.error("Failed to retrieve Google spreadsheet content", e);
		}

		return fileName;
	}

}
