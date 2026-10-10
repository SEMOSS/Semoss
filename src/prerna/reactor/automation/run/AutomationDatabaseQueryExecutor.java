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
package prerna.reactor.automation.run;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.reactor.qs.SqlQueryReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

/**
 * Loads a generated Automation database read into a SEMOSS frame.
 *
 * <p>
 * The generated Python source resolves its ordinary {@code scope} references and
 * returns only an internal query request. This adapter sends that request through
 * {@link SqlQueryReactor}, preserving its SQL routing, authorization, audit, and
 * limit behavior. The returned task is imported once into the run Insight's
 * native frame backend and registered under the node output alias. Native frames
 * retain the authorized query plan and page the source engine on demand. Full
 * rows therefore do not cross the Python-to-Java Automation result boundary or
 * get materialized solely to render a node result.
 */
final class AutomationDatabaseQueryExecutor {

	private static final Logger classLogger = LogManager.getLogger(AutomationDatabaseQueryExecutor.class);

	private AutomationDatabaseQueryExecutor() {
	}

	/** Returns whether this node owns the internal generated-query contract. */
	static boolean supports(Map<String, Object> node) {
		return AutomationConstants.NODE_DATABASE_QUERY.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& !AutomationConstants.NODE_CODE_MODE_CUSTOM
						.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
	}

	/** Returns whether a generated source returned a frame-producing query request. */
	static boolean isRequest(Object value) {
		return value instanceof Map<?, ?> result
				&& result.containsKey(AutomationConstants.INTERNAL_DATABASE_QUERY);
	}

	/**
	 * Executes one internal request and registers the resulting frame.
	 *
	 * @param insight        run Insight that owns database access and frame lifetime
	 * @param rawRequest     internal request returned by generated Python source
	 * @param outputVariable node output alias used by the live SEMOSS frame
	 * @param runId          durable Automation run identifier used for diagnostics
	 * @param nodeId         executing node identifier used for diagnostics
	 * @return registered frame identity and bounded public summary
	 */
	static AutomationFrameOutput.RegisteredFrame execute(Insight insight, Object rawRequest, String outputVariable,
			String runId, String nodeId) {
		QueryRequest request = parseRequest(rawRequest);
		BasicIteratorTask task = createTask(insight, request);
		ITableDataFrame frame = null;
		boolean registered = false;
		try {
			frame = FrameFactory.getFrame(insight, DataFrameTypeEnum.NATIVE.getTypeAsString(), outputVariable);
			SelectQueryStruct queryStruct = task.getQueryStruct();
			IImporter importer = ImportFactory.getImporter(frame, queryStruct, task);
			if (importer == null) {
				throw new IllegalStateException("SEMOSS could not create a native frame importer.");
			}
			importer.setInsight(insight);
			importer.insertData();
			long rowCount = queryRowCount(task, runId, nodeId);

			AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, outputVariable,
					frame, rowCount);
			registered = true;

			classLogger.debug(
					"Loaded database query output '{}' for Automation run '{}', node '{}', execution Insight '{}': "
							+ "{} rows, {} columns",
					outputVariable, runId, nodeId, insight.getInsightId(), output.summary().get("rowCount"),
					output.summary().get("columnCount"));
			return output;
		} catch (Exception e) {
			if (!registered && frame != null) {
				closeFailedFrame(frame, outputVariable, runId, nodeId);
			}
			throw e instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(e);
		} finally {
			closeTask(task, runId, nodeId);
		}
	}

	private static long queryRowCount(BasicIteratorTask task, String runId, String nodeId) {
		try {
			IRawSelectWrapper wrapper = task.getIterator();
			long rowCount = wrapper.getNumRows();
			if (rowCount < 0) {
				throw new IllegalStateException("Database query returned an invalid row count.");
			}
			return rowCount;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to count database frame rows for Automation run '{}', node '{}'", runId, nodeId,
					e);
			throw new IllegalStateException("Unable to count Automation database query rows.", e);
		}
	}

	private static BasicIteratorTask createTask(Insight insight, QueryRequest request) {
		NounStore store = new NounStore("SqlQuery");
		store.makeGenRowStruct(ReactorKeysEnum.DATABASE.getKey())
				.add(new NounMetadata(request.databaseId(), PixelDataType.CONST_STRING));
		store.makeGenRowStruct(ReactorKeysEnum.QUERY_KEY.getKey())
				.add(new NounMetadata(request.query(), PixelDataType.CONST_STRING));
		store.makeGenRowStruct(ReactorKeysEnum.LIMIT.getKey())
				.add(new NounMetadata(request.limit(), PixelDataType.CONST_INT));

		SqlQueryReactor reactor = new SqlQueryReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		Object queryResult = reactor.execute().getValue();
		if (!(queryResult instanceof BasicIteratorTask task)) {
			throw new IllegalStateException("Generated database query did not return a tabular task.");
		}
		return task;
	}

	static QueryRequest parseRequest(Object value) {
		if (!(value instanceof Map<?, ?> result) || result.size() != 1
				|| !(result.get(AutomationConstants.INTERNAL_DATABASE_QUERY) instanceof Map<?, ?> request)) {
			throw new IllegalStateException("Generated database query returned an invalid internal request.");
		}
		String databaseId = requiredString(request, AutomationConstants.CONFIG_ENGINE_ID);
		String query = requiredString(request, "query");
		int limit = requiredLimit(request.get(AutomationConstants.CONFIG_LIMIT));
		return new QueryRequest(databaseId, query, limit);
	}

	private static String requiredString(Map<?, ?> request, String key) {
		Object value = request.get(key);
		if (!(value instanceof String text) || text.isBlank()) {
			throw new IllegalStateException("Generated database query request." + key + " must be a nonblank string.");
		}
		return text;
	}

	private static int requiredLimit(Object value) {
		if (!(value instanceof Number number)) {
			throw new IllegalStateException("Generated database query request.limit must be a number.");
		}
		double configured = number.doubleValue();
		if (!Double.isFinite(configured) || configured != Math.rint(configured)
				|| configured < AutomationConstants.DB_QUERY_MIN_LIMIT
				|| configured > AutomationConstants.DB_QUERY_MAX_LIMIT) {
			throw new IllegalStateException("Generated database query request.limit is outside the supported range.");
		}
		return (int) configured;
	}

	private static void closeFailedFrame(ITableDataFrame frame, String outputVariable, String runId, String nodeId) {
		try {
			frame.close();
		} catch (RuntimeException closeError) {
			classLogger.warn("Unable to close failed database frame '{}' for Automation run '{}', node '{}'",
					outputVariable, runId, nodeId, closeError);
		}
	}

	private static void closeTask(BasicIteratorTask task, String runId, String nodeId) {
		try {
			task.close();
		} catch (IOException closeError) {
			classLogger.warn("Unable to close database query task for Automation run '{}', node '{}'", runId, nodeId,
					closeError);
		}
	}

	record QueryRequest(String databaseId, String query, int limit) {
	}
}
