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
package prerna.reactor.export;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryRowCountReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QueryRowCountReactor.class);
	private static final String CLASS_NAME = QueryRowCountReactor.class.getName();

	public QueryRowCountReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		SelectQueryStruct qs = getQs();
		AbstractQueryStruct.QUERY_STRUCT_TYPE qsType = qs.getQsType();

		if (qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE
				|| qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY
				|| qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY) {
			return rowsForEngine(qs, logger);
		} else if (qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME
				|| qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			return rowsForFrame(qs, logger);
		}

		throw new IllegalArgumentException("Can not determine row count for Query Struct of type " + qsType);
	}

	/**
	 * 
	 * @param qs
	 * @param logger
	 * @return
	 */
	private NounMetadata rowsForFrame(SelectQueryStruct qs, Logger logger) {
		ITableDataFrame frame = qs.getFrame();
		if (frame == null) {
			throw new IllegalArgumentException("Query Struct is of type " + qs.getQsType() + " but no frame is set");
		}
		IRawSelectWrapper iterator = null;
		try {
			iterator = frame.query(qs);
			long start = System.currentTimeMillis();
			logger.info("Query Row Count : Executing query on frame {}", frame.getName());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Query Row Count : Frame {} execution time = {}ms", frame.getName(), (end - start));
			return new NounMetadata(numRows, PixelDataType.CONST_INT, PixelOperationType.QUERY_ROW_COUNT);
		} catch (Exception e) {
			classLogger.error("Error counting query rows on frame {}", frame.getName(), e);
			if (iterator == null) {
				throw new IllegalArgumentException(
						"Error occurred retrieving the query with message " + e.getMessage());
			} else {
				throw new IllegalArgumentException(
						"Error occurred retrieving the count of the query with message " + e.getMessage());
			}
		} finally {
			if (iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error("Failed to close the iterator after counting query rows on frame {}",
							frame.getName(), e);
				}
			}
		}
	}

	/**
	 * 
	 * @param qs
	 * @param logger
	 * @return
	 */
	private NounMetadata rowsForEngine(SelectQueryStruct qs, Logger logger) {
		IDatabaseEngine engine = qs.retrieveQueryStructEngine();
		if (engine == null) {
			throw new IllegalArgumentException("Query Struct is of type " + qs.getQsType() + " but no engine is set");
		}
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, qs, true);
			long start = System.currentTimeMillis();
			logger.info("Query Row Count : Executing query on engine {}", engine.getEngineId());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Query Row Count : Engine {} execution time = {}ms", engine.getEngineId(), (end - start));
			return new NounMetadata(numRows, PixelDataType.CONST_INT, PixelOperationType.QUERY_ROW_COUNT);
		} catch (Exception e) {
			classLogger.error("Error counting query rows on engine {}", engine.getEngineId(), e);
			if (iterator == null) {
				throw new IllegalArgumentException(
						"Error occurred retrieving the query with message " + e.getMessage());
			} else {
				throw new IllegalArgumentException(
						"Error occurred retrieving the count of the query with message " + e.getMessage());
			}
		} finally {
			if (iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error("Failed to close the iterator after counting query rows on engine {}",
							engine.getEngineId(), e);
				}
			}
		}
	}

	/**
	 * Generate the task from the query struct
	 * 
	 * @return
	 */
	private SelectQueryStruct getQs() {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
		// if we don't have tasks in the curRow, check if it exists in genrow under the
		// qs key
		if (grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			qs = (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				qs = (SelectQueryStruct) noun.getValue();
			}
		}

		if (qs == null) {
			throw new IllegalArgumentException("Must pass in a database query to get the row count");
		}

		return qs;
	}

}
