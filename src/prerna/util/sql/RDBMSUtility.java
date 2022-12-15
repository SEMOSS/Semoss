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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.google.gson.Gson;

import net.snowflake.client.jdbc.internal.com.nimbusds.jose.util.IOUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDBMSUtility {
	
	private RDBMSUtility() {
		
	}
	
	// TODO: move insertion of clob to query utils
	// TODO: move insertion of clob to query utils
	// TODO: move insertion of clob to query utils
	// TODO: move insertion of clob to query utils
	// TODO: move insertion of clob to query utils
	// TODO: move insertion of clob to query utils

	/**
	 * Determine how to handle a clob into a prepared statement based on if the input is already a clob
	 * and if the database allows clobs or not
	 * 
	 * @param engine
	 * @param queryUtil
	 * @param ps
	 * @param parameterIndex
	 * @param value
	 * @param gson
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void handleInsertionOfClobInput(RDBMSNativeEngine engine, AbstractSqlQueryUtil queryUtil, 
			PreparedStatement ps, int parameterIndex, Object value, Gson gson) throws SQLException, IOException {
		if(value == null) {
			ps.setNull(parameterIndex, java.sql.Types.CLOB);
		}
		
		// grab the connection used in the prepared statement
		// in case we are doing pooling
		Connection conn = ps.getConnection();
		
		// DUE TO NOT RETURNING CLOB FROM THE WRAPPER BUT FLUSHING IT OUT
		// THIS IS THE ONLY BLOCK THAT SHOULD GET ENTERED
		if(value instanceof String) {
			if(queryUtil.allowClobJavaObject()) {
				Clob engineClob = engine.createClob(conn);
				engineClob.setString(1, (String) value);
				ps.setClob(parameterIndex, engineClob);
			} else {
				ps.setString(parameterIndex, (String) value);
			}
			return;
		}
		
		// do we allow clob data types?
		if(queryUtil.allowClobJavaObject()) {
			Clob engineClob = engine.createClob(conn);

			boolean canTransfer = false;
			// is our input also a clob?
			if(value instanceof Clob) {
				// yes clob - transfer clob from one to the other
				try {
					RDBMSUtility.transferClob((Clob) value, engineClob);
				} catch(Exception e) {
					//ignore 
					canTransfer = false;
				}
			}
			// if we cant transfer
			// we have to flush and push
			if(!canTransfer) {
				if(value instanceof Clob) {
					// flush the clob to a string
					String stringInput = RDBMSUtility.flushClobToString((Clob) value);
					engineClob.setString(1, stringInput);
				} else {
					// no clob - flush to string
					engineClob.setString(1, gson.toJson(value));
				}
			}
			// set the clob in the prepared statement
			ps.setClob(parameterIndex, engineClob);
		} else {
			// we do not allow clob data types
			// we will just set this as a string value
			// in the prepared statement
			String stringInput = null;
			// is our input a clob?
			if(value instanceof Clob) {
				// flush the clob to a string
				stringInput = RDBMSUtility.flushClobToString((Clob) value);
			} else {
				stringInput = gson.toJson(value);
			}
			// add the string to the prepared statement
			ps.setString(parameterIndex, stringInput);
		}
	}
	
	/**
	 * 
	 * @param sourceClob
	 * @param targetClob
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void transferClob(Clob sourceClob, Clob targetClob) throws SQLException, IOException {
		InputStream source = sourceClob.getAsciiStream();
		OutputStream target = targetClob.setAsciiStream(1);
		
		byte[] buf = new byte[8192];
		int length;
		while ((length = source.read(buf)) > 0) {
			target.write(buf, 0, length);
		}
	}
	
	/**
	 * 
	 * @param clob
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public static String flushClobToString(Clob clob) throws SQLException, IOException {
		if(clob == null) {
			return null;
		}
		// flush the clob to a string
		InputStream inputStream =  clob.getAsciiStream();
		// flush input stream to string
		return IOUtils.readInputStreamToString(inputStream, StandardCharsets.UTF_8);
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
		
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
			baseFolder = baseFolder.substring(0, baseFolder.length()-1);
		}
		
		return baseURL.replace("@" + Constants.BASE_FOLDER + "@", baseFolder)
				.replace("@" + Constants.ENGINE + "@", SmssUtilities.getUniqueName(engineName, engineId));
	}
}