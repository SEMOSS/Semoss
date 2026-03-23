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
package prerna.reactor.masterdatabase.util;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class GenerateMetamodelUtility {

	private static final Logger classLogger = LogManager.getLogger(GenerateMetamodelUtility.class);
	private static final Gson GSON = new GsonBuilder().create();

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Object> getMetamodelPositions(String databaseId, String databaseName, String smssFile) {
		Map<String, Object> positions = MasterDatabaseUtility.getMetamodelPositions(databaseId);

		// Could not find in database, read from file
		if (positions.size() == 0) {
			classLogger.info("Pulling database positions for database " + databaseId);
			positions = EngineSyncUtility.getMetamodelPositions(databaseId);
			if (positions == null) {
				positions = getOwlMetamodelPositions(databaseId, databaseName, smssFile);
			}

			// Save positions read from file to database
			if (positions.size() > 0) {
				MasterDatabaseUtility.saveMetamodelPositions(databaseId, positions);
			}
		}
		return positions;
	}

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Object> getOwlMetamodelPositions(String databaseId, String databaseName,
			String smssFile) {

		Map<String, Object> positions = new HashMap<>();
		if (!new File(smssFile).exists()) {
			classLogger.warn("Could not find database smss '" + smssFile + "'");
			classLogger.warn("Could not find database smss '" + smssFile + "'");
			classLogger.warn("Could not find database smss '" + smssFile + "'");
			return positions;
		}
		Properties smssProp = Utility.loadProperties(smssFile);
		if (smssProp == null) {
			classLogger.warn("Could not load smss at '" + smssFile + "'");
			classLogger.warn("Could not load smss at '" + smssFile + "'");
			classLogger.warn("Could not load smss at '" + smssFile + "'");
			return positions;
		}
		// if the file is present, pull it and load
		File owlF = SmssUtilities.getOwlFile(smssFile, smssProp);
		if (owlF != null && owlF.isFile()) {
			// position file is in same folder as OWL
			String baseFolder = owlF.getParent();
			String positionJson = baseFolder + "/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
			File positionFile = new File(positionJson);
			// try to make the file
			if (!positionFile.exists() && !positionFile.isFile()) {
				try {
					classLogger.info("Generating metamodel layout for database " + databaseId);
					classLogger.info("This process may take some time");
					GenerateMetamodelLayout.generateLayout(databaseId, databaseName, owlF);
					classLogger.info("Metamodel layout has been generated");
				} catch (Exception e) {
					classLogger.info("Exception in creating database metamodel layout");
					classLogger.error(Constants.STACKTRACE, e);
				} catch (NoClassDefFoundError e) {
					classLogger.info("Error in creating database metamodel layout");
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			if (positionFile.exists() && positionFile.isFile()) {
				// load the file
				Path path = positionFile.toPath();
				try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
					positions = GSON.fromJson(reader, Map.class);
					EngineSyncUtility.setMetamodelPositions(databaseId, positions);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return positions;
	}

}
