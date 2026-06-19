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
package prerna.reactor.qs.source;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleFileRetrieverReactor extends AbstractQueryStructReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleFileRetrieverReactor.class);
	private static final String CLASS_NAME = GoogleFileRetrieverReactor.class.getName();

	public GoogleFileRetrieverReactor() {
		this.keysToGet = new String[] { "id", "type" };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {

		// get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String fileID = this.keyValue.get(this.keysToGet[0]);
		if (fileID == null || fileID.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file id");
		}
		String type = this.keyValue.get(this.keysToGet[1]);
		if (type == null || type.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file type");
		}

		// get access token
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}

		// Initialize variables
		Hashtable params = new Hashtable();
		CsvQueryStruct qs = new CsvQueryStruct();

		// filepath for the download/export
		String filePath = Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ Utility.getDIHelperProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");
		String url_str = null;
		if (type.contains("google-apps.spreadsheet")) {
			url_str = "https://www.googleapis.com/drive/v3/files/" + fileID + "/export";
			params = new Hashtable();
			params.put("mimeType", "text/csv");
		} else if (type.contains("text/csv")) {
			url_str = "https://www.googleapis.com/drive/v3/files/" + fileID;
			params = new Hashtable();
			params.put("alt", "media");
		} else {
			throw new IllegalArgumentException("Illegal file type");
		}
		BufferedWriter target = null;
		try {
			BufferedReader br = HttpHelperUtility.getHttpStream(url_str, accessToken, params, true);

			// create a file

			File outputFile = new File(filePath);

			target = new BufferedWriter(new FileWriter(outputFile));
			String data = null;

			while ((data = br.readLine()) != null) {
				target.write(data);
				target.write("\n");
				target.flush();
			}
			// get datatypes
			CSVFileHelper helper = new CSVFileHelper();
			helper.setDelimiter(',');
			helper.parse(filePath);
			Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(),
					helper.predictTypes());
			Map<String, String> dataTypes = predictionMaps[0];
			Map<String, String> additionalDataTypes = predictionMaps[1];
			for (String key : dataTypes.keySet()) {
				qs.addSelector("DND", key);
			}
			helper.clear();
			qs.merge(this.qs);
			qs.setFilePath(filePath);
			qs.setDelimiter(',');
			qs.setColumnTypes(dataTypes);
			qs.setAdditionalTypes(additionalDataTypes);
			return qs;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (target != null) {
				try {
					target.flush();
					target.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return qs;
	}
}
