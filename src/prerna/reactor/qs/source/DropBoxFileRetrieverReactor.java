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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.Constants;
import prerna.util.Utility;

public class DropBoxFileRetrieverReactor extends AbstractQueryStructReactor {

	// private String[] keysToGet;
	private static final String CLASS_NAME = DropBoxFileRetrieverReactor.class.getName();

	public DropBoxFileRetrieverReactor() {
		this.keysToGet = new String[] { "path" };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {

		// get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String dropboxPath = this.keyValue.get(this.keysToGet[0]);
		if (dropboxPath == null || dropboxPath.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file path");
		}

		// get access token
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "dropbox");
				retMap.put("message", "Please login to your DropBox account");
				throwLoginError(retMap);
			} else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.DROPBOX);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "dropbox");
			retMap.put("message", "Please login to your DropBox account");
			throwLoginError(retMap);
		}

		//

		// lists the various files for this user
		// if the
		// name of the object to return
		String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
		String[] beanProps = { "name", "id", "url" }; // add is done when you have a list
		String jsonPattern = "[metadata.name,metadata.id,link]";

		// you fill what you want to send on the API call
		String url_str = "https://api.dropboxapi.com/2/files/get_temporary_link";
		Hashtable params = new Hashtable();
		params.put("path", dropboxPath);

		String output = HttpHelperUtility.makePostCall(url_str, accessToken, params, true);

		// fill the bean with the return. This return will have a url to download the
		// file from which is done below
		RemoteItem link = (RemoteItem) BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		String filePath = Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ Utility.getDIHelperProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");
		try {
			URL urlDownload = new URL(link.getUrl());
			File destination = new File(filePath);
			FileUtils.copyURLToFile(urlDownload, destination);
		} catch (MalformedURLException e2) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e2);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e1);
		}
		// get datatypes
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(),
				helper.predictTypes());
		Map<String, String> dataTypes = predictionMaps[0];
		Map<String, String> additionalDataTypes = predictionMaps[1];
		CsvQueryStruct qs = new CsvQueryStruct();
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

	}

}