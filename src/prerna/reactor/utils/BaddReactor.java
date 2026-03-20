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
package prerna.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

public class BaddReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(BaddReactor.class);

	public BaddReactor() {
		this.keysToGet = new String[] { "fancy", "embed" };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		IRDBMSEngine engine = SystemEngineRegistry.getLocalMasterDb();
		Connection conn = null;
		try {
			conn = engine.getConnection();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			String engineName = engine.getEngineName() != null ? engine.getEngineName() : "engine";
			throw new IllegalArgumentException("Could not connect to " + engineName);
		}
		String errorMessage = "";
		Statement stmt = null;
		try {
			// check to see if such a fancy name exists
			stmt = conn.createStatement();
			String query = "SELECT embed, fancy from bitly where fancy='" + this.keyValue.get("fancy") + "'";
			ResultSet rs = stmt.executeQuery(query);
			// if there is a has next not sure what

			if (rs.next()) {
				errorMessage = "Name " + this.keyValue.get("fancy") + " already exists. Please enter a new name.";
			} else {
				query = "Insert into bitly(embed, fancy) values ('" + this.keyValue.get("embed") + "' , '"
						+ this.keyValue.get("fancy") + "')";
				stmt.execute(query);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (engine.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if (errorMessage.isEmpty()) {
			return new NounMetadata("Added " + this.keyValue.get("fancy"), PixelDataType.CONST_STRING);
		} else {
			System.out.println(errorMessage);
			return new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}
	}
}
