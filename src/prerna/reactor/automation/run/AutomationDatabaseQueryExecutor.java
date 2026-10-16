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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter.CompiledQuery;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.rdf.engine.wrappers.RawPreparedRDBMSSelectWrapper;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;

/**
 * Loads a typed Automation database read into an Insight-owned SEMOSS frame.
 *
 * <p>
 * Java resolves the node's immutable config against scope and creates a typed
 * query request. The generic Pixel executor sends that request through
 * {@code SqlQuery}, preserving its SQL routing, authorization, audit, and limit
 * behavior. This adapter only imports the authorized task once into the run
 * Insight's configured Python frame backend. Full rows never cross into
 * Automation scope.
 */
final class AutomationDatabaseQueryExecutor {

	private static final Logger classLogger = LogManager.getLogger(AutomationDatabaseQueryExecutor.class);

	private AutomationDatabaseQueryExecutor() {
	}

	static QueryRequest request(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		Object engine = AutomationScopeResolver.resolveExactOrLiteral(config.get(AutomationConstants.CONFIG_ENGINE_ID),
				scope, "Database query node '" + nodeId + "' engineId");
		Object query = AutomationScopeResolver.resolveExactOrLiteral(config.get("query"), scope,
				"Database query node '" + nodeId + "' query");
		Object limit = AutomationScopeResolver.resolveExactOrLiteral(config.get(AutomationConstants.CONFIG_LIMIT),
				scope,
				"Database query node '" + nodeId + "' limit");
		Object parameters = AutomationScopeResolver.resolveConfig(
				config.getOrDefault(AutomationConstants.CONFIG_QUERY_PARAMETERS, List.of()), scope,
				"Database query node '" + nodeId + "' parameters");
		List<Object> queryParameters = parameters == null ? List.of()
				: parameters instanceof List<?> list ? new ArrayList<>(list)
						: throwInvalidParameters();
		return new QueryRequest(requiredString(engine, AutomationConstants.CONFIG_ENGINE_ID),
				requiredString(query, "query"), requiredLimit(limit), queryParameters);
	}

	/**
	 * Executes one typed request and registers the resulting frame.
	 *
	 * @param insight        run Insight that owns database access and frame
	 *                       lifetime
	 * @param pixelExecutor  authenticated Pixel boundary for the run Insight
	 * @param request        typed request resolved from immutable node config
	 * @param outputVariable node output alias and Python variable name
	 * @param runId          durable Automation run identifier used for diagnostics
	 * @param nodeId         executing node identifier used for diagnostics
	 * @return internal reference to the registered frame
	 */
	static AutomationFrameOutput.RegisteredFrame execute(Insight insight, AutomationPixelExecutor pixelExecutor,
			QueryRequest request, String outputVariable, String runId, String nodeId) {
		BasicIteratorTask task = createTask(pixelExecutor, request);
		ITableDataFrame frame = null;
		boolean registered = false;
		try {
			frame = FrameFactory.getFrame(insight, DataFrameTypeEnum.PYTHON.getTypeAsString(), outputVariable);
			IImporter importer = ImportFactory.getImporter(frame, task.getQueryStruct(), task);
			if (importer == null) {
				throw new IllegalStateException("SEMOSS could not create a Python frame importer.");
			}
			importer.setInsight(insight);
			importer.insertData();
			if (!(frame instanceof PandasFrame pandasFrame)) {
				throw new IllegalStateException("Automation database query did not create a Pandas frame.");
			}
			normalizeDateColumns(pandasFrame);
			AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, outputVariable,
					frame);
			registered = true;
			classLogger.debug(
					"Loaded database frame '{}' for Automation run '{}', node '{}', execution Insight '{}': "
							+ "{} rows, {} columns",
					outputVariable, runId, nodeId, insight.getInsightId(), output.rowCount(),
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

	static List<String> dateNormalizationScripts(PandasFrame frame) {
		String tableName = frame.getName();
		String data = frame.getWrapperName() + ".cache['data']";
		List<String> scripts = new ArrayList<>();
		for (String column : frame.getColumnHeaders()) {
			String metadataType = frame.getMetaData().getHeaderTypeAsString(tableName + "__" + column);
			SemossDataType dataType = SemossDataType.convertStringToDataType(metadataType);
			if (dataType == SemossDataType.DATE || dataType == SemossDataType.TIMESTAMP) {
				String dataColumn = data + "[" + AutomationRuntimeUtils.GSON.toJson(column) + "]";
				scripts.add(dataColumn + " = pd.to_datetime(" + dataColumn + ")");
			}
		}
		return scripts;
	}

	static void normalizeDateColumns(PandasFrame frame) {
		for (String script : dateNormalizationScripts(frame)) {
			frame.runScript(script);
		}
	}

	private static void closeFailedFrame(ITableDataFrame frame, String outputVariable, String runId, String nodeId) {
		try {
			frame.close();
		} catch (RuntimeException closeError) {
			classLogger.warn("Unable to close failed database frame '{}' for Automation run '{}', node '{}'",
					outputVariable, runId, nodeId, closeError);
		}
	}

	private static BasicIteratorTask createTask(AutomationPixelExecutor pixelExecutor, QueryRequest request) {
		BasicIteratorTask task = pixelExecutor.executeTask(AutomationPixelExpressionRenderer.databaseQuery(
				request.databaseId(), request.query(), request.limit()));
		if (!request.parameters().isEmpty()) {
			if (!(task.getQueryStruct() instanceof HardSelectQueryStruct queryStruct)
					|| !(queryStruct.getEngine() instanceof IRDBMSEngine database)) {
				closeTask(task, "parameterized", "authorization");
				throw new IllegalStateException("Parameterized database query requires a relational database.");
			}
			try {
				ParameterizedQuery data = new ParameterizedQuery(request.query(), request.parameters(),
						request.limit());
				ParameterizedQuery count = new ParameterizedQuery(
						"SELECT COUNT(*) FROM (" + request.query() + ") AUTOMATION_BOUND_COUNT",
						request.parameters(), 1);
				RawPreparedRDBMSSelectWrapper wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(database,
						new CompiledQuery(data, count), 0);
				closeTask(task, "parameterized", "authorization");
				BasicIteratorTask preparedTask = new BasicIteratorTask(queryStruct, wrapper);
				preparedTask.setNumCollect(request.limit());
				preparedTask.setCollectLimit(request.limit());
				return preparedTask;
			} catch (RuntimeException e) {
				closeTask(task, "parameterized", "authorization");
				throw e;
			} catch (Exception e) {
				closeTask(task, "parameterized", "authorization");
				throw new IllegalStateException("Unable to execute the parameterized database query.", e);
			}
		}
		return task;
	}

	private static List<Object> throwInvalidParameters() {
		throw new IllegalArgumentException("Database query parameters must resolve to an array.");
	}

	private static String requiredString(Object value, String key) {
		if (!(value instanceof String text) || text.isBlank()) {
			throw new IllegalArgumentException("Database query " + key + " must resolve to a nonblank string.");
		}
		return text;
	}

	private static int requiredLimit(Object value) {
		if (!(value instanceof Number number)) {
			throw new IllegalArgumentException("Database query limit must resolve to a number.");
		}
		double configured = number.doubleValue();
		if (!Double.isFinite(configured) || configured != Math.rint(configured)
				|| configured < AutomationConstants.DB_QUERY_MIN_LIMIT
				|| configured > AutomationConstants.DB_QUERY_MAX_LIMIT) {
			throw new IllegalArgumentException("Database query limit is outside the supported range.");
		}
		return (int) configured;
	}

	private static void closeTask(BasicIteratorTask task, String runId, String nodeId) {
		try {
			task.close();
		} catch (IOException closeError) {
			classLogger.warn("Unable to close database query task for Automation run '{}', node '{}'", runId, nodeId,
					closeError);
		}
	}

	record QueryRequest(String databaseId, String query, int limit, List<Object> parameters) {
	}
}
