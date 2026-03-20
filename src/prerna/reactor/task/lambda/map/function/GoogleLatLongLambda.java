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
package prerna.reactor.task.lambda.map.function;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleLatLongGetter;
import prerna.om.GeoLocation;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleLatLongLambda extends AbstractMapLambda {

	private static final Logger classLogger = LogManager.getLogger(GoogleLatLongLambda.class);

	// cahing of some results
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static String cache_file_loc = null;
	private static Map<String, List<Double>> localcache = new HashMap<String, List<Double>>();

	static {
		String baseFolder = Utility.getBaseFolder();
		cache_file_loc = baseFolder + DIR_SEPARATOR + "geo" + DIR_SEPARATOR + "latlong.json";
		File f = new File(cache_file_loc);
		if (f.exists()) {
			Map<String, List<Double>> mapData = null;
			try {
				mapData = new ObjectMapper().readValue(f, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				// do noting
			}

			if (mapData != null) {
				localcache.putAll(mapData);
			}
		}
	}

	// col index we care about to get lat/long from
	private int colIndex;
	// total number of columns
	private int totalCols;

	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[] { "lat", "long" };
		Object[] newValues = new Object[2];

		// grab the column index we want to use as the address
		String address = row.getValues()[colIndex].toString().toLowerCase().replace("_", " ");
		if (localcache.containsKey(address)) {
			List<Double> cacheValues = localcache.get(address);
			newValues[0] = cacheValues.get(0);
			newValues[1] = cacheValues.get(1);
		} else {
			Hashtable params = new Hashtable();
			params.put("address", address);

			// add new values
			try {
				GoogleLatLongGetter goog = new GoogleLatLongGetter();
				// geo location object flushes the JSON return into something for getters and
				// setters
				GeoLocation location = (GeoLocation) goog.execute(this.user, params);
				newValues[0] = location.getLatitude();
				newValues[1] = location.getLongitude();

				// cache it
				List<Double> cacheV = new Vector<Double>();
				cacheV.add((double) location.getLatitude());
				cacheV.add((double) location.getLongitude());
				localcache.put(address, cacheV);

//				File f = new File(cache_file_loc);
//				if(!f.getParentFile().exists()) {
//					f.getParentFile().mkdirs();
//				}
//				if(f.exists()) {
//					f.delete();
//				}
//				try {
//					Gson gson = new GsonBuilder().setPrettyPrinting().create();
//					// write json to file
//					FileUtils.writeStringToFile(f, gson.toJson(localcache));
//				} catch (IOException e1) {
//					classLogger.error(Constants.STACKTRACE, e1);
//				}

			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		row.addFields(newHeaders, newValues);
		return row;
	}

	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		this.totalCols = headerInfo.size();

		String headerToConvert = columns.get(0);
		for (int j = 0; j < totalCols; j++) {
			Map<String, Object> headerMap = headerInfo.get(j);
			String alias = headerMap.get("alias").toString();
			if (alias.equals(headerToConvert)) {
				// we found the index
				this.colIndex = j;
			}
		}

		// this modifies the header info map by reference
		Map<String, Object> latHeader = getBaseHeader("lat", "NUMBER");
		this.headerInfo.add(latHeader);
		Map<String, Object> longHeader = getBaseHeader("long", "NUMBER");
		this.headerInfo.add(longHeader);
	}

	/**
	 * Grab a base header object
	 * 
	 * @param name
	 * @param type
	 * @return
	 */
	private Map<String, Object> getBaseHeader(String name, String type) {
		Map<String, Object> header = new HashMap<String, Object>();
		header.put("alias", name);
		header.put("header", name);
		header.put("derived", true);
		header.put("type", type.toUpperCase());
		return header;
	}

}
