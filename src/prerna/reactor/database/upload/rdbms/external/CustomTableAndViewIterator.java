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
package prerna.reactor.database.upload.rdbms.external;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.RdbmsTypeEnum;

public class CustomTableAndViewIterator implements Iterator<String[]>, Closeable {

	private static final Logger classLogger = LogManager.getLogger(CustomTableAndViewIterator.class);

	private Statement tableStmt;
	private ResultSet tablesRs;
	private String[] tableKeys;
	private Collection<String> tableAndViewFilters;
//	private boolean useDirectValues = false;
//	private Iterator<String> directValuesIterator;

	private String[] currRow = null;

	/**
	 * Generates an iterator to either use the database metadata to get a list of
	 * table or views Or iterators through a passed in list of values
	 * 
	 * @param meta
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param tableAndViewFilters
	 */
	public CustomTableAndViewIterator(Connection con, DatabaseMetaData meta, String catalogFilter, String schemaFilter,
			RdbmsTypeEnum driver, Collection<String> tableAndViewFilters) {
		boolean autoclose = false;
		try {
			this.tableStmt = con.createStatement();
			this.tablesRs = RdbmsConnectionHelper.getTables(con, tableStmt, meta, catalogFilter, schemaFilter, driver);
			this.tableKeys = RdbmsConnectionHelper.getTableKeys(driver);
			this.tableAndViewFilters = tableAndViewFilters;
		} catch (SQLException e) {
			autoclose = true;
			classLogger.error("Failed to fetch tables and views from database metadata (catalog={}, schema={})",
					catalogFilter, schemaFilter, e);
			throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		} finally {
			if (autoclose) {
				close();
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (currRow == null) {
			currRow = getNextRow();
		}

		// if after attempting to get the next row it is
		// still null, then there are no new returns within the rs
		if (currRow != null) {
			return true;
		}

		return false;
	}

	private String[] getNextRow() {
		String[] nextRow = null;
		boolean findValidRow = true;
		try {
			while (findValidRow && this.tablesRs.next()) {
				String tableOrViewName = this.tablesRs.getString(this.tableKeys[0]);
				String tableType = this.tablesRs.getString(this.tableKeys[1]);
				String tableSchema = this.tablesRs.getString(this.tableKeys[2]);

				if (tableAndViewFilters != null && !tableAndViewFilters.isEmpty()) {
					if (tableAndViewFilters.contains(tableOrViewName)) {
						nextRow = new String[] { tableOrViewName, tableType, tableSchema };
						findValidRow = false;
					}
				} else {
					// no filtering, so this must be valid
					nextRow = new String[] { tableOrViewName, tableType, tableSchema };
					findValidRow = false;
				}
			}
		} catch (SQLException e) {
			classLogger.error("Error iterating over tables/views result set from database metadata", e);
			throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

		return nextRow;
	}

	@Override
	public String[] next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		if (currRow == null) {
			hasNext();
		}

		String[] retRow = this.currRow;
		this.currRow = null;
		return retRow;
	}

	@Override
	public void close() {
		if (this.tablesRs != null) {
			try {
				this.tablesRs.close();
			} catch (SQLException e) {
				classLogger.warn("Failed to close tables and views result set", e);
			}
		}
		if (this.tableStmt != null) {
			try {
				this.tableStmt.close();
			} catch (SQLException e) {
				classLogger.warn("Failed to close tables and views statement", e);
			}
		}
	}

}
