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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationCleanOperation;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.PurgeReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Applies existing BI cleaning reactors to an isolated, run-owned table. */
final class AutomationCleanExecutor {
	private static final Logger LOGGER = LogManager.getLogger(AutomationCleanExecutor.class);
	private AutomationCleanExecutor() {
	}

	/**
	 * Copies an upstream Automation frame, applies one existing BI cleaning reactor,
	 * and registers the isolated result in the execution Insight.
	 *
	 * @param insight        execution Insight that owns the input and output frames
	 * @param outputVariable output alias registered in the Insight
	 * @param type           persisted Automation node type
	 * @param config         immutable node configuration
	 * @param scope          current Automation scope
	 * @param nodeId         executing node identifier used for diagnostics
	 * @return internal reference to the registered cleaned frame
	 */
	static AutomationFrameOutput.RegisteredFrame execute(Insight insight, String outputVariable,
			AutomationNodeType type, Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		AutomationCleanOperation operation = operationType(type, config, scope, nodeId);
		Object source = AutomationScopeResolver.resolveExactOrLiteral(config.get("source"), scope,
				"Clean node '" + nodeId + "'");
		if (!(source instanceof AutomationFrameOutput.RegisteredFrame input)) {
			throw new IllegalArgumentException(
					"Clean node '" + nodeId + "' requires a live upstream table, not JSON or a preview.");
		}
		if (input.rowCount() > AutomationConstants.SAVED_RESULT_MAX_ROWS) {
			throw new IllegalArgumentException("Clean input exceeds the supported table row limit.");
		}
		AbstractReactor reactor = reactor(operation, input.frame().getFrameType());
		NounStore nouns = new NounStore("clean." + operation.getValue());
		reactor.setInsight(insight);
		reactor.setNounStore(nouns);
		reactor.In();
		if (operation == AutomationCleanOperation.PURGE) {
			SelectQueryStruct filters = purgeFilters(config.get("filters"), input.frame(), scope);
			reactor.getCurRow().add(new NounMetadata(filters, PixelDataType.QUERY_STRUCT));
		} else if (operation == AutomationCleanOperation.CHANGE_TYPE) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame()).get(0);
			String dataType = resolveRequiredString(config, "dataType", scope, nodeId);
			if (!List.of("STRING", "FACTOR", "DOUBLE", "DATE").contains(dataType)) {
				throw new IllegalArgumentException("Unsupported target column type: " + dataType);
			}
			nouns.makeGenRowStruct("column").add(new NounMetadata(column, PixelDataType.CONST_STRING));
			nouns.makeGenRowStruct("dataType").add(new NounMetadata(dataType, PixelDataType.CONST_STRING));
		} else if (operation == AutomationCleanOperation.RENAME_COLUMN) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame()).get(0);
			String newColumn = resolveRequiredString(config, "newColumn", scope, nodeId);
			if (!newColumn.matches("[A-Za-z_][A-Za-z0-9_]*")) {
				throw new IllegalArgumentException("New column name must contain only letters, digits, and underscores.");
			}
			if (!column.equals(newColumn) && Arrays.asList(input.frame().getColumnHeaders()).contains(newColumn)) {
				throw new IllegalArgumentException("A column named '" + newColumn + "' already exists.");
			}
			nouns.makeGenRowStruct("column").add(new NounMetadata(column, PixelDataType.CONST_STRING));
			nouns.makeGenRowStruct("newCol").add(new NounMetadata(newColumn, PixelDataType.CONST_STRING));
		} else if (operation != AutomationCleanOperation.REMOVE_DUPLICATES) {
			List<String> columns = columns(AutomationScopeResolver.resolveExactOrLiteral(config.get("columns"), scope,
					"Clean columns"), input.frame());
			String key = operation == AutomationCleanOperation.REPLACE ? "column" : "columns";
			for (String column : columns) {
				nouns.makeGenRowStruct(key).add(new NounMetadata(column, PixelDataType.CONST_STRING));
			}
			if (operation == AutomationCleanOperation.REPLACE) {
				String value = replacement(config.getOrDefault("value", ""), scope);
				String newValue = replacement(config.getOrDefault("newValue", ""), scope);
				validateReplacement(columns, input.frame(), value, newValue);
				nouns.makeGenRowStruct("value").add(new NounMetadata(value, PixelDataType.CONST_STRING));
				nouns.makeGenRowStruct("newValue").add(new NounMetadata(newValue, PixelDataType.CONST_STRING));
			}
		}
		ITableDataFrame copy = null;
		ITableDataFrame resultFrame = null;
		boolean completed = false;
		try {
			copy = copy(insight, input.frame(), outputVariable);
			nouns.makeGenRowStruct(PixelDataType.FRAME.getKey()).add(new NounMetadata(copy, PixelDataType.FRAME));
			NounMetadata result = reactor.execute();
			if (!(result.getValue() instanceof ITableDataFrame frame)) {
				throw new IllegalStateException("Clean node '" + nodeId + "' did not return a table.");
			}
			resultFrame = frame;
			synchronizePythonFrame(resultFrame, outputVariable);
			resultFrame.clearCachedMetrics();
			resultFrame.clearQueryCache();
			AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, outputVariable,
					resultFrame);
			LOGGER.debug("Cleaned frame '{}' for Automation node '{}' in execution Insight '{}': {} rows, {} columns",
					outputVariable, nodeId, insight.getInsightId(), output.rowCount(), output.summary().get("columnCount"));
			completed = true;
			return output;
		} catch (Exception error) {
			if (resultFrame != null && resultFrame != copy) {
				close(insight, outputVariable, resultFrame);
			}
			if (copy != null && !sharesPythonBindings(copy, resultFrame)) {
				close(insight, outputVariable, copy);
			}
			throw new IllegalStateException("Unable to clean table for node '" + nodeId + "': " + error.getMessage(), error);
		} finally {
			if (completed && copy != null && copy != resultFrame && !sharesPythonBindings(copy, resultFrame)) {
				close(insight, outputVariable, copy);
			}
		}
	}

	static AutomationCleanOperation operationType(AutomationNodeType type, Map<String, Object> config,
			Map<String, Object> scope, String nodeId) {
		if (type != AutomationNodeType.CLEAN_PROCESS) {
			throw new IllegalArgumentException("Unsupported Clean node type: " + type.getType());
		}
		return AutomationCleanOperation.fromValue(resolveRequiredString(config, "operation", scope, nodeId));
	}

	private static String resolveRequiredString(Map<String, Object> config, String key, Map<String, Object> scope,
			String nodeId) {
		Object resolved = AutomationScopeResolver.resolveExactOrLiteral(config.get(key), scope,
				"Clean node '" + nodeId + "' " + key);
		if (!(resolved instanceof String value) || value.isBlank()) {
			throw new IllegalArgumentException("Clean node '" + nodeId + "' requires a nonblank " + key + ".");
		}
		return value.trim();
	}

	private static boolean sharesPythonBindings(ITableDataFrame first, ITableDataFrame second) {
		return first instanceof PandasFrame original && second instanceof PandasFrame replacement
				&& ((original.getName() != null && original.getName().equals(replacement.getName()))
						|| (original.getWrapperName() != null && original.getWrapperName().equals(replacement.getWrapperName())));
	}

	/** Keeps row-count and Python scope bindings aligned with the BI wrapper after row filtering. */
	static void synchronizePythonFrame(ITableDataFrame frame, String outputVariable) {
		if (frame instanceof PandasFrame python) {
			python.runScript(outputVariable + " = " + python.getWrapperName() + ".cache['data']");
		}
	}

	static AbstractReactor reactor(AutomationCleanOperation type, DataFrameTypeEnum frameType) {
		boolean python = frameType == DataFrameTypeEnum.PYTHON;
		if (!python) {
			throw new IllegalArgumentException("clean." + type.getValue() + " requires a Python frame.");
		}
		return switch (type) {
		case TRIM -> new prerna.reactor.frame.py.TrimColumnsReactor();
		case REPLACE -> new prerna.reactor.frame.py.ReplaceColumnValueReactor();
		case REMOVE_DUPLICATES -> new prerna.reactor.frame.py.RemoveDuplicateRowsReactor();
		case FILTER_EMPTY -> {
			yield new prerna.reactor.frame.py.FilterEmptyValuesReactor();
		}
		case IMPUTE_NULLS -> new prerna.reactor.frame.py.ImputeNullValuesReactor();
		case CHANGE_TYPE -> new prerna.reactor.frame.py.ChangeColumnTypeReactor();
		case DROP_COLUMN -> new prerna.reactor.frame.py.DropColumnReactor();
		case RENAME_COLUMN -> new prerna.reactor.frame.py.RenameColumnReactor();
		case PURGE -> new PurgeReactor();
		default -> throw new IllegalArgumentException("Unsupported clean operation: " + type);
		};
	}

	static List<String> columns(Object configured, ITableDataFrame frame) {
		if (!(configured instanceof List<?> selected) || selected.isEmpty() || selected.size() > 1000) {
			throw new IllegalArgumentException("Select between 1 and 1000 columns to clean.");
		}
		List<String> available = Arrays.asList(frame.getColumnHeaders());
		List<String> columns = new ArrayList<>();
		for (Object item : selected) {
			if (!(item instanceof String column) || !available.contains(column)
					|| !column.matches("[A-Za-z_][A-Za-z0-9_]*")) {
				throw new IllegalArgumentException("Unknown or unsupported clean column: " + item);
			}
			if (!columns.contains(column)) {
				columns.add(column);
			}
		}
		return columns;
	}

	static SelectQueryStruct purgeFilters(Object configured, ITableDataFrame frame, Map<String, Object> scope) {
		if (!(configured instanceof List<?> filters) || filters.isEmpty() || filters.size() > 100) {
			throw new IllegalArgumentException("Purge requires between 1 and 100 filters specifying rows to remove.");
		}
		SelectQueryStruct query = new SelectQueryStruct();
		OrQueryFilter retainedRows = new OrQueryFilter();
		for (Object item : filters) {
			if (!(item instanceof Map<?, ?> filter)
					|| !(filter.get("column") instanceof String column)
					|| !(filter.get("operator") instanceof String operator)
					|| !List.of("==", "!=", ">", ">=", "<", "<=").contains(operator)
					|| !filter.containsKey("value")) {
				throw new IllegalArgumentException("Invalid Purge filter.");
			}
			columns(List.of(column), frame);
			Object value = AutomationScopeResolver.resolveConfig(filter.get("value"), scope, "Purge filter value");
			if (!(value instanceof String || value instanceof Boolean
					|| value instanceof Number number && Double.isFinite(number.doubleValue()))) {
				throw new IllegalArgumentException("Purge filter values must be non-null scalars.");
			}
			String inverse = switch (operator) {
			case "==" -> "!=";
			case "!=" -> "==";
			case ">" -> "<=";
			case ">=" -> "<";
			case "<" -> ">=";
			case "<=" -> ">";
			default -> throw new IllegalArgumentException("Invalid Purge comparator.");
			};
			retainedRows.addFilter(SimpleQueryFilter.makeColToValFilter(column, inverse, value));
		}
		query.addExplicitFilter(retainedRows);
		return query;
	}

	private static String replacement(Object configured, Map<String, Object> scope) {
		Object value = AutomationScopeResolver.resolveExactOrLiteral(configured, scope, "Replacement value");
		if (!(value instanceof String text)) {
			throw new IllegalArgumentException("Replacement values must resolve to strings.");
		}
		if (text.contains("\"") || text.contains("\\") || text.chars().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException(
					"The BI replacement routine does not safely support quotes, backslashes, or control characters.");
		}
		return text;
	}

	private static void validateReplacement(List<String> columns, ITableDataFrame frame, String value, String newValue) {
		for (String column : columns) {
			String type = frame.getMetaData().getHeaderTypeAsString(frame.getName() + "__" + column);
			if ("INT".equals(type) || "DOUBLE".equals(type)) {
				for (String literal : List.of(value, newValue)) {
					if (!literal.isEmpty() && !literal.matches("-?\\d+(\\.\\d+)?")
						&& !List.of("null", "na", "nan").contains(literal.toLowerCase(java.util.Locale.ROOT))) {
						throw new IllegalArgumentException("Numeric replacement values must be numeric or null.");
					}
				}
			} else if ("STRING".equals(type) && "null".equalsIgnoreCase(value)
					&& frame.getFrameType() == DataFrameTypeEnum.PYTHON) {
				throw new IllegalArgumentException(
						"The BI Python replacement routine does not support safe null replacement for text columns.");
			}
		}
	}

	private static ITableDataFrame copy(Insight insight, ITableDataFrame source, String alias) throws Exception {
		ITableDataFrame target = FrameFactory.getFrame(insight, source.getFrameType().getTypeAsString(), alias);
		try {
			SelectQueryStruct query = source.getMetaData().getFlatTableQs(false);
			query.setFrame(source);
			query.setDistinct(false);
			query.setExplicitFilters(source.getFrameFilters().copy());
			try (IRawSelectWrapper rows = source.query(query)) {
				IImporter importer = ImportFactory.getImporter(target, query, rows);
				importer.setInsight(insight);
				importer.insertData();
				if (target instanceof PandasFrame pythonFrame) {
					normalizeDateColumns(pythonFrame);
				}
			}
			return target;
		} catch (Exception error) {
			close(insight, alias, target);
			throw error;
		}
	}

	/** Restores DATE and TIMESTAMP dtypes only on the isolated Clean working frame. */
	private static void normalizeDateColumns(PandasFrame frame) {
		String tableName = frame.getName();
		String data = frame.getWrapperName() + ".cache['data']";
		for (String column : frame.getColumnHeaders()) {
			String metadataType = frame.getMetaData().getHeaderTypeAsString(tableName + "__" + column);
			SemossDataType dataType = SemossDataType.convertStringToDataType(metadataType);
			if (dataType == SemossDataType.DATE || dataType == SemossDataType.TIMESTAMP) {
				String dataColumn = data + "[" + AutomationRuntimeUtils.GSON.toJson(column) + "]";
				frame.runScript(dataColumn + " = pd.to_datetime(" + dataColumn + ")");
			}
		}
	}

	private static void close(Insight insight, String alias, ITableDataFrame frame) {
		NounMetadata noun = insight.getVarStore().get(alias);
		if (noun != null && noun.getValue() == frame) {
			insight.getVarStore().remove(alias);
		}
		try {
			frame.close();
		} catch (RuntimeException error) {
			LOGGER.warn("Unable to close failed or superseded Clean frame '{}'", alias, error);
		} finally {
			insight.getVarStore().getAllCreatedFrames().remove(frame);
		}
	}
}
