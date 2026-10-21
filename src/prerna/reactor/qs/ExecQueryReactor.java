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
package prerna.reactor.qs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.DatabaseWriteAudit;

public class ExecQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExecQueryReactor.class);

	private static final String REACTOR_NAME = "ExecQuery";

	private NounMetadata qStruct = null;
	private boolean recordedByCaller = false;
	// the user's level on one engine, read by the calling SqlQuery for its request
	private String callerEngineId = null;
	private AccessPermissionEnum callerLevel = null;

	public ExecQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey(), "commit",
				ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (qStruct == null) {
			qStruct = getQueryStruct();
		}

		GenRowStruct commitGrs = this.store.getGenRowStruct("commit");
		Boolean commit = false;
		if (commitGrs != null && !commitGrs.isEmpty()) {
			commit = Boolean.parseBoolean(commitGrs.get(0) + "");
		}

		IDatabaseEngine engine = null;
		ITableDataFrame frame = null;
		AbstractQueryStruct qs = null;
		String userId = "user not defined";

		if (qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE
					|| qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
				engine = qs.retrieveQueryStructEngine();
				if (!(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS
						|| engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME
						|| engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA)) {
					throw new IllegalArgumentException("Query update/deletes only works for rdbms/rdf databases");
				}

				// the user id fills any <USER_ID> value in an update
				User user = this.insight.getUser();
				if (user != null) {
					userId = user.getAccessToken(user.getLogins().get(0)).getId();
				}

				// check that the user can edit the engine
				if (!canEdit(user, engine.getEngineId())) {
					throw new IllegalArgumentException("User does not have permission to exec query for this app");
				}
			} else if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
				frame = qs.getFrame();
				if (!(frame instanceof AbstractRdbmsFrame)) {
					throw new IllegalArgumentException("Query update/deletes only works for sql frames");
				}
				// convert any aliases the FE is using from the frame
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			}
		} else {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}

		String query = null;
		if (qs instanceof HardSelectQueryStruct) {
			query = ((HardSelectQueryStruct) qs).getQuery();
		} else if (qs instanceof UpdateQueryStruct) {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter((UpdateQueryStruct) qs);
			interp.setUserId(userId);
			query = interp.composeQuery();
		} else if (qs instanceof SelectQueryStruct) {
			DeleteSqlInterpreter interp = new DeleteSqlInterpreter((SelectQueryStruct) qs);
			query = interp.composeQuery();
		}

		classLogger.info("Executing Query {} ", query);
		if (qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE
				|| qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			if (engine == null) {
				throw new NullPointerException("No engine passed in to execute the query");
			}
			try {
				engine.insertData(query);
				if (commit) {
					engine.commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to execute the query {} in the database", query, e);
				if (!this.recordedByCaller) {
					DatabaseWriteAudit.record(this.insight, engine,
							DatabaseWriteAudit.Write.perStatement(REACTOR_NAME, query, 1, 0, 0, e));
				}
				String errorMessage = "An error occurred trying to execute the query in the database";
				if (e.getMessage() != null && !e.getMessage().isEmpty()) {
					errorMessage += ": " + e.getMessage();
				}
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(errorMessage));
			}
			if (!this.recordedByCaller) {
				DatabaseWriteAudit.record(this.insight, engine,
						DatabaseWriteAudit.Write.perStatement(REACTOR_NAME, query, 1, 1, 1, null));
				// only an engine that keeps its data in its own folder has files to push
				if (engine.holdsFileLocks()) {
					ClusterUtil.pushEngine(engine.getEngineId());
				}
			}
		} else {
			try {
				if (frame != null) {
					((AbstractRdbmsFrame) frame).getBuilder().runQuery(query);
				}
			} catch (Exception e) {
				classLogger.error("Failed to execute the query {} to update the frame", query, e);
				String errorMessage = "An error occurred trying to update the frame";
				if (e.getMessage() != null && !e.getMessage().isEmpty()) {
					errorMessage += ": " + e.getMessage();
				}
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(errorMessage));
			}
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE,
				PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
		String customSuccessMessage = getCustomSuccessMessage();
		if (customSuccessMessage != null && !customSuccessMessage.isEmpty()) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(customSuccessMessage));
		}
		return noun;
	}

	/**
	 * Hands the database write event and the cluster push to the caller. SqlQuery
	 * sets this so a request with many statements records one event and pushes the
	 * engine once, after its last statement.
	 *
	 * @param recordedByCaller whether the caller records the write and pushes the
	 *                         engine
	 */
	void setRecordedByCaller(boolean recordedByCaller) {
		this.recordedByCaller = recordedByCaller;
	}

	/**
	 * Hands over the user's level on an engine, which SqlQuery reads once at the
	 * start of a request with many statements. The edit check holds the level to
	 * the same rule, so this saves reading it again for each statement and skips
	 * nothing. A query on any other engine reads the level as usual.
	 * Package-private so only the reactors in this package can call it.
	 *
	 * @param engineId the engine the level was read for
	 * @param level    the user's level on it, or null for no access
	 */
	void setCallerLevel(String engineId, AccessPermissionEnum level) {
		this.callerEngineId = engineId;
		this.callerLevel = level;
	}

	/**
	 * Whether the user can edit the engine, from the level the caller read for it
	 * when there is one.
	 */
	private boolean canEdit(User user, String engineId) {
		if (engineId != null && engineId.equals(this.callerEngineId)) {
			return this.callerLevel != null && AccessPermissionEnum.isEditor(this.callerLevel.getId());
		}
		return SecurityEngineUtils.userCanEditEngine(user, engineId);
	}

	/**
	 * 
	 * @return
	 */
	private NounMetadata getQueryStruct() {
		GenRowStruct grs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getNoun(0);
		}
		grs = this.store.getGenRowStruct(PixelDataType.QUERY_STRUCT.name());
		if (grs != null && !grs.isEmpty()) {
			return grs.getNoun(0);
		}

		return null;
	}

	/**
	 * 
	 * @return
	 */
	public String getCustomSuccessMessage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return (String) grs.get(0);
		}

		return null;
	}

	/**
	 * 
	 * @param qs
	 */
	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}

}
