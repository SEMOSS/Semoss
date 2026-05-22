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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class DirectJdbcConnectionReactor extends AbstractQueryStructReactor {

	private static final Logger classLogger = LogManager.getLogger(DirectJdbcConnectionReactor.class);

	public DirectJdbcConnectionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(),
				ReactorKeysEnum.CONNECTION_DETAILS.getKey() };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> connectionDetails = getConDetails();

		String driver = (String) connectionDetails.get(Constants.RDBMS_TYPE);
		if (driver == null) {
			driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		}
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);

		Connection con = null;
		String connectionUrl = null;
		try {
			connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
		} catch (RuntimeException e) {
			throw new SemossPixelException(
					new NounMetadata("Unable to generation connection url with message " + e.getMessage(),
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

		try {
			con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetails);
			queryUtil.enhanceConnection(con);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			String driverError = e.getMessage();
			String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
			errorMessage += driverError;
			errorMessage += " \"";
			throw new SemossPixelException(
					new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

		IRDBMSEngine temporalEngine = new RDBMSNativeEngine();
		temporalEngine.setEngineId("FAKE_ENGINE");
		temporalEngine.setConnection(con);
		temporalEngine.setBasic(true);
		temporalEngine.setQueryUtil(queryUtil);

		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(connectionDetails);
		qs.setEngine(temporalEngine);
		if (query != null && !query.isEmpty()) {
			qs.setQuery(query);
		}
		this.qs = qs;
		return qs;
	}

	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		init();
		createQueryStructPlan();
		if (parentReactor != null) {
			// this is only called lazy
			// have to init to set the qs
			// to them add to the parent
			NounMetadata data = new NounMetadata(this.qs, PixelDataType.QUERY_STRUCT);
			if (parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
					|| parentReactor instanceof GenericReactor) {
				parentReactor.getCurRow().add(data);
			} else {
				GenRowStruct parentQSInput = parentReactor.getNounStore()
						.makeGenRowStruct(PixelDataType.QUERY_STRUCT.toString());
				parentQSInput.add(data);
			}
		}
	}

	private AbstractQueryStruct createQueryStructPlan() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> connectionDetails = getConDetails();

		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(connectionDetails);
		qs.setEngineId("FAKE_ENGINE");
		if (query != null && !query.isEmpty()) {
			qs.setQuery(query);
		}
		this.qs = qs;
		return this.qs;
	}

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if (mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}

		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}

		return null;
	}

}
