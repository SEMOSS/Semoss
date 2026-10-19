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
import prerna.reactor.automation.definition.AutomationFrameOperation;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.PurgeReactor;
import prerna.reactor.imports.MergeReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Applies existing BI cleaning reactors to an isolated, run-owned table. */
final class AutomationCleanExecutor {
	private static final Logger LOGGER = LogManager.getLogger(AutomationCleanExecutor.class);

	private AutomationCleanExecutor() {
	}

	/**
	 * Copies an upstream Automation frame, applies one existing BI cleaning
	 * reactor,
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
		AutomationFrameOperation operation = operationType(type, config, scope, nodeId);
		Object source = AutomationScopeResolver.resolveExactOrLiteral(config.get("source"), scope,
				"Clean node '" + nodeId + "'");
		if (!(source instanceof AutomationFrameOutput.RegisteredFrame input)) {
			throw new IllegalArgumentException(
					"Clean node '" + nodeId + "' requires a live upstream table, not JSON or a preview.");
		}
		if (input.rowCount() > AutomationConstants.SAVED_RESULT_MAX_ROWS) {
			throw new IllegalArgumentException("Clean input exceeds the supported table row limit.");
		}
		DataFrameTypeEnum frameType = input.frame().getFrameType();
		if (frameType == DataFrameTypeEnum.R
				|| operation != AutomationFrameOperation.PURGE && operation != AutomationFrameOperation.JOIN
						&& frameType != DataFrameTypeEnum.PYTHON) {
			throw new IllegalArgumentException("Automation Clean and Transform operations require a Python frame.");
		}
		if (operation == AutomationFrameOperation.JOIN) {
			return executeJoin(insight, outputVariable, operation, input, config, scope, nodeId);
		}
		AbstractReactor reactor = reactor(operation, input.frame().getFrameType());
		NounStore nouns = new NounStore(type.getType());
		reactor.setInsight(insight);
		reactor.setNounStore(nouns);
		reactor.In();
		if (operation == AutomationFrameOperation.PURGE) {
			SelectQueryStruct filters = purgeFilters(config.get("filters"), input.frame(), scope);
			reactor.getCurRow().add(new NounMetadata(filters, PixelDataType.QUERY_STRUCT));
		} else if (operation == AutomationFrameOperation.CHANGE_TYPE) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame())
					.get(0);
			String dataType = resolveRequiredString(config, "dataType", scope, nodeId);
			if (!List.of("STRING", "FACTOR", "DOUBLE", "DATE").contains(dataType)) {
				throw new IllegalArgumentException("Unsupported target column type: " + dataType);
			}
			nouns.makeGenRowStruct("column").add(new NounMetadata(column, PixelDataType.CONST_STRING));
			nouns.makeGenRowStruct("dataType").add(new NounMetadata(dataType, PixelDataType.CONST_STRING));
		} else if (operation == AutomationFrameOperation.RENAME_COLUMN) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame())
					.get(0);
			String newColumn = resolveRequiredString(config, "newColumn", scope, nodeId);
			if (!newColumn.matches("[A-Za-z_][A-Za-z0-9_]*")) {
				throw new IllegalArgumentException(
						"New column name must contain only letters, digits, and underscores.");
			}
			if (!column.equals(newColumn) && Arrays.asList(input.frame().getColumnHeaders()).contains(newColumn)) {
				throw new IllegalArgumentException("A column named '" + newColumn + "' already exists.");
			}
			nouns.makeGenRowStruct("column").add(new NounMetadata(column, PixelDataType.CONST_STRING));
			nouns.makeGenRowStruct("newCol").add(new NounMetadata(newColumn, PixelDataType.CONST_STRING));
		} else if (operation == AutomationFrameOperation.ADD_COLUMN) {
			String newColumn = newColumn(config, scope, nodeId, input.frame());
			String dataType = resolveRequiredString(config, "dataType", scope, nodeId);
			if (!List.of("STRING", "FACTOR", "DOUBLE", "DATE").contains(dataType)) {
				throw new IllegalArgumentException("Unsupported new column type: " + dataType);
			}
			addString(nouns, ReactorKeysEnum.NEW_COLUMN.getKey(), newColumn);
			addString(nouns, ReactorKeysEnum.DATA_TYPE.getKey(), dataType);
		} else if (operation == AutomationFrameOperation.DUPLICATE_COLUMN) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame())
					.get(0);
			String newColumn = newColumn(config, scope, nodeId, input.frame());
			addString(nouns, ReactorKeysEnum.COLUMN.getKey(), column);
			addString(nouns, ReactorKeysEnum.NEW_COLUMN.getKey(), newColumn);
		} else if (operation == AutomationFrameOperation.CONCATENATE) {
			List<String> selected = columns(config.get("columns"), input.frame());
			String newColumn = newColumn(config, scope, nodeId, input.frame());
			String delimiter = safeScriptLiteral(config, "delimiter", scope, nodeId, " ");
			addString(nouns, ReactorKeysEnum.NEW_COLUMN.getKey(), newColumn);
			addString(nouns, ReactorKeysEnum.DELIMITER.getKey(), delimiter);
			if (input.frame().getFrameType() == DataFrameTypeEnum.R) {
				for (String column : selected) {
					nouns.makeGenRowStruct(ReactorKeysEnum.VALUES.getKey())
							.add(new NounMetadata(column, PixelDataType.COLUMN));
				}
			} else {
				addColumns(nouns, selected);
			}
		} else if (operation == AutomationFrameOperation.PIVOT) {
			String pivotColumn = columns(List.of(resolveRequiredString(config, "pivotColumn", scope, nodeId)),
					input.frame()).get(0);
			String valueColumn = columns(List.of(resolveRequiredString(config, "valueColumn", scope, nodeId)),
					input.frame()).get(0);
			if (pivotColumn.equals(valueColumn)) {
				throw new IllegalArgumentException("Pivot and value columns must be different.");
			}
			List<String> maintained = optionalColumns(config.get("maintainColumns"), input.frame());
			String function = optionalString(config.get("aggregateFunction"), "aggregateFunction");
			if (!List.of("", "sum", "mean", "median").contains(function.toLowerCase(java.util.Locale.ROOT))) {
				throw new IllegalArgumentException("Choose a supported pivot aggregate.");
			}
			String naReplace = safeOptionalScriptLiteral(config.get("naReplace"), scope, nodeId);
			addString(nouns, "pivotCol", pivotColumn);
			addString(nouns, "valueCol", valueColumn);
			if (!maintained.isEmpty()) {
				for (String column : maintained) {
					addString(nouns, ReactorKeysEnum.MAINTAIN_COLUMNS.getKey(), column);
				}
			}
			if (!function.isEmpty())
				addString(nouns, "function", function);
			if (naReplace != null)
				addString(nouns, "naReplace", naReplace);
		} else if (operation == AutomationFrameOperation.LOWER_CASE
				|| operation == AutomationFrameOperation.PROPER_CASE
				|| operation == AutomationFrameOperation.UPPER_CASE) {
			addColumns(nouns, stringColumns(config.get("columns"), input.frame()));
		} else if (operation == AutomationFrameOperation.SPLIT) {
			addColumns(nouns, columns(config.get("columns"), input.frame()));
			addString(nouns, ReactorKeysEnum.DELIMITER.getKey(), safeScriptLiteral(config, "delimiter", scope, nodeId));
			addString(nouns, "search", "Regex");
		} else if (operation == AutomationFrameOperation.SPLIT_UNPIVOT) {
			addColumns(nouns, columns(config.get("columns"), input.frame()));
			addString(nouns, ReactorKeysEnum.DELIMITER.getKey(), safeScriptLiteral(config, "delimiter", scope, nodeId));
		} else if (operation == AutomationFrameOperation.TIMESTAMP_DATA) {
			String newColumn = newColumn(config, scope, nodeId, input.frame());
			Object includeTime = config.getOrDefault("includeTime", Boolean.FALSE);
			if (!(includeTime instanceof Boolean)) {
				throw new IllegalArgumentException("includeTime must be a boolean.");
			}
			addString(nouns, ReactorKeysEnum.NEW_COLUMN.getKey(), newColumn);
			addString(nouns, "time", includeTime.toString());
		} else if (operation == AutomationFrameOperation.TO_PERCENT) {
			String column = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), input.frame())
					.get(0);
			validateNumericColumn(input.frame(), column);
			String newColumn = newColumn(config, scope, nodeId, input.frame());
			Object significantDigits = config.get("sigDigits");
			if (!(significantDigits instanceof Number number) || number.intValue() < 0 || number.intValue() > 20) {
				throw new IllegalArgumentException("Significant digits must be between 0 and 20.");
			}
			Object by100 = config.getOrDefault("by100", Boolean.FALSE);
			if (!(by100 instanceof Boolean))
				throw new IllegalArgumentException("by100 must be a boolean.");
			addString(nouns, ReactorKeysEnum.COLUMN.getKey(), column);
			nouns.makeGenRowStruct("sigDigits").add(new NounMetadata(number.intValue(), PixelDataType.CONST_INT));
			nouns.makeGenRowStruct("by100").add(new NounMetadata(by100, PixelDataType.BOOLEAN));
			addString(nouns, ReactorKeysEnum.NEW_COLUMN.getKey(), newColumn);
		} else if (operation == AutomationFrameOperation.UNPIVOT) {
			addColumns(nouns, columns(config.get("columns"), input.frame()));
		} else if (operation != AutomationFrameOperation.REMOVE_DUPLICATES
				&& operation != AutomationFrameOperation.TRANSPOSE) {
			List<String> columns = columns(AutomationScopeResolver.resolveExactOrLiteral(config.get("columns"), scope,
					"Clean columns"), input.frame());
			if (operation == AutomationFrameOperation.REPLACE) {
				for (String column : columns)
					addString(nouns, ReactorKeysEnum.COLUMN.getKey(), column);
			} else {
				addColumns(nouns, columns);
			}
			if (operation == AutomationFrameOperation.REPLACE) {
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
					outputVariable, nodeId, insight.getInsightId(), output.rowCount(),
					output.summary().get("columnCount"));
			completed = true;
			return output;
		} catch (Exception error) {
			if (resultFrame != null && resultFrame != copy) {
				close(insight, outputVariable, resultFrame);
			}
			if (copy != null && !sharesPythonBindings(copy, resultFrame)) {
				close(insight, outputVariable, copy);
			}
			throw new IllegalStateException("Unable to clean table for node '" + nodeId + "': " + error.getMessage(),
					error);
		} finally {
			if (completed && copy != null && copy != resultFrame && !sharesPythonBindings(copy, resultFrame)) {
				close(insight, outputVariable, copy);
			}
		}
	}

	static AutomationFrameOperation operationType(AutomationNodeType type, Map<String, Object> config,
			Map<String, Object> scope, String nodeId) {
		if (type != AutomationNodeType.CLEAN_PROCESS && type != AutomationNodeType.DATA_TRANSFORM) {
			throw new IllegalArgumentException("Unsupported frame operation node type: " + type.getType());
		}
		AutomationFrameOperation operation = AutomationFrameOperation.fromValue(
				resolveRequiredString(config, "operation", scope, nodeId));
		if (operation.isTransform() != (type == AutomationNodeType.DATA_TRANSFORM)) {
			throw new IllegalArgumentException("The selected operation does not belong to this node category.");
		}
		return operation;
	}

	private static AutomationFrameOutput.RegisteredFrame executeJoin(Insight insight, String outputVariable,
			AutomationFrameOperation operation, AutomationFrameOutput.RegisteredFrame left,
			Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		Object rightValue = AutomationScopeResolver.resolveExactOrLiteral(config.get("rightSource"), scope,
				"Transform node '" + nodeId + "' rightSource");
		if (!(rightValue instanceof AutomationFrameOutput.RegisteredFrame right)) {
			throw new IllegalArgumentException("Transform node '" + nodeId + "' requires a live right-side table.");
		}
		if (right.frame().getFrameType() == DataFrameTypeEnum.R) {
			throw new IllegalArgumentException("Automation Join operations do not support R frames.");
		}
		if (right.rowCount() > AutomationConstants.SAVED_RESULT_MAX_ROWS) {
			throw new IllegalArgumentException("Join input exceeds the supported table row limit.");
		}
		String leftColumn = columns(List.of(resolveRequiredString(config, "column", scope, nodeId)), left.frame())
				.get(0);
		String rightColumn = columns(List.of(resolveRequiredString(config, "rightColumn", scope, nodeId)),
				right.frame())
				.get(0);
		if (operation == AutomationFrameOperation.JOIN) {
			String joinType = optionalString(config.get("joinType"), "joinType");
			if (joinType.isEmpty())
				joinType = "inner.join";
			return executeStandardJoin(insight, outputVariable, left.frame(), right.frame(), leftColumn, rightColumn,
					joinType, nodeId);
		}
		throw new IllegalArgumentException("Unsupported multi-frame Transform operation: " + operation.getValue());
	}

	private static AutomationFrameOutput.RegisteredFrame executeStandardJoin(Insight insight, String outputVariable,
			ITableDataFrame left, ITableDataFrame right, String leftColumn, String rightColumn, String joinType,
			String nodeId) {
		if (!List.of("inner.join", "left.outer.join", "right.outer.join").contains(joinType)) {
			throw new IllegalArgumentException("Unsupported Join type: " + joinType);
		}
		ITableDataFrame working = null;
		ITableDataFrame resultFrame = null;
		boolean completed = false;
		try {
			working = copy(insight, left, outputVariable);
			SelectQueryStruct rightQuery = frameQuery(right);
			NounStore nouns = new NounStore("Merge");
			nouns.makeGenRowStruct(ReactorKeysEnum.FRAME.getKey())
					.add(new NounMetadata(working, PixelDataType.FRAME));
			nouns.makeGenRowStruct(ReactorKeysEnum.QUERY_STRUCT.getKey())
					.add(new NounMetadata(rightQuery, PixelDataType.QUERY_STRUCT));
			nouns.makeGenRowStruct(ReactorKeysEnum.JOINS.getKey())
					.add(new NounMetadata(new Join(leftColumn, joinType, rightColumn), PixelDataType.JOIN));
			MergeReactor reactor = new MergeReactor();
			reactor.setInsight(insight);
			reactor.setNounStore(nouns);
			reactor.In();
			NounMetadata result = reactor.execute();
			if (!(result.getValue() instanceof ITableDataFrame frame)) {
				throw new IllegalStateException("Join did not return a table.");
			}
			resultFrame = frame;
			validateOutputSize(resultFrame, outputVariable);
			synchronizePythonFrame(resultFrame, outputVariable);
			resultFrame.clearCachedMetrics();
			resultFrame.clearQueryCache();
			AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, outputVariable,
					resultFrame);
			completed = true;
			return output;
		} catch (Exception error) {
			if (resultFrame != null && resultFrame != working)
				close(insight, outputVariable, resultFrame);
			if (working != null && !sharesPythonBindings(working, resultFrame))
				close(insight, outputVariable, working);
			throw new IllegalStateException(
					"Unable to join tables for node '" + nodeId + "': " + error.getMessage(), error);
		} finally {
			if (completed && working != null && working != resultFrame && !sharesPythonBindings(working, resultFrame)) {
				close(insight, outputVariable, working);
			}
		}
	}

	private static SelectQueryStruct frameQuery(ITableDataFrame frame) {
		SelectQueryStruct query = frame.getMetaData().getFlatTableQs(false);
		query.setFrame(frame);
		query.setDistinct(false);
		query.setExplicitFilters(frame.getFrameFilters().copy());
		return query;
	}

	private static void validateOutputSize(ITableDataFrame frame, String outputVariable) {
		if (frame.size(outputVariable) > AutomationConstants.SAVED_RESULT_MAX_ROWS) {
			throw new IllegalArgumentException("Transform output exceeds the supported table row limit.");
		}
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

	private static void addString(NounStore nouns, String key, String value) {
		nouns.makeGenRowStruct(key).add(new NounMetadata(value, PixelDataType.CONST_STRING));
	}

	private static void addColumns(NounStore nouns, List<String> columns) {
		for (String column : columns) {
			addString(nouns, ReactorKeysEnum.COLUMNS.getKey(), column);
		}
	}

	private static String newColumn(Map<String, Object> config, Map<String, Object> scope, String nodeId,
			ITableDataFrame frame) {
		String name = resolveRequiredString(config, "newColumn", scope, nodeId);
		if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("New column name must contain only letters, digits, and underscores.");
		}
		if (Arrays.asList(frame.getColumnHeaders()).contains(name)) {
			throw new IllegalArgumentException("A column named '" + name + "' already exists.");
		}
		return name;
	}

	private static String safeScriptLiteral(Map<String, Object> config, String key, Map<String, Object> scope,
			String nodeId) {
		return safeScriptLiteral(config, key, scope, nodeId, null);
	}

	private static String safeScriptLiteral(Map<String, Object> config, String key, Map<String, Object> scope,
			String nodeId, String fallback) {
		Object configured = config.containsKey(key) ? config.get(key) : fallback;
		Object resolved = AutomationScopeResolver.resolveExactOrLiteral(configured, scope,
				"Transform " + key + " for node '" + nodeId + "'");
		if (!(resolved instanceof String value) || value.isBlank()) {
			throw new IllegalArgumentException("Transform node '" + nodeId + "' requires a nonblank " + key + ".");
		}
		if (value.length() > 100 || value.contains("'") || value.contains("\"") || value.contains("\\")
				|| value.chars().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException(key + " contains characters unsupported by the BI reactor.");
		}
		return value;
	}

	private static String safeOptionalScriptLiteral(Object configured, Map<String, Object> scope, String nodeId) {
		if (configured == null)
			return null;
		Object resolved = AutomationScopeResolver.resolveExactOrLiteral(configured, scope,
				"Pivot replacement value for node '" + nodeId + "'");
		if (!(resolved instanceof String value)) {
			throw new IllegalArgumentException("Pivot replacement value must resolve to a string.");
		}
		if (value.length() > 100 || value.contains("'") || value.contains("\"") || value.contains("\\")
				|| value.chars().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException("Pivot replacement value contains unsupported characters.");
		}
		return value;
	}

	private static String optionalString(Object configured, String key) {
		if (configured == null)
			return "";
		if (!(configured instanceof String value)) {
			throw new IllegalArgumentException(key + " must be a string.");
		}
		return value.trim();
	}

	private static List<String> optionalColumns(Object configured, ITableDataFrame frame) {
		if (configured == null)
			return List.of();
		if (!(configured instanceof List<?> selected) || selected.size() > 1000) {
			throw new IllegalArgumentException("maintainColumns must be a list of at most 1000 column names.");
		}
		if (selected.isEmpty())
			return List.of();
		return columns(selected, frame);
	}

	private static List<String> stringColumns(Object configured, ITableDataFrame frame) {
		List<String> selected = columns(configured, frame);
		for (String column : selected) {
			String type = frame.getMetaData().getHeaderTypeAsString(frame.getName() + "__" + column);
			if (!"STRING".equalsIgnoreCase(type)) {
				throw new IllegalArgumentException(
						"Case conversion requires string columns; '" + column + "' is " + type + ".");
			}
		}
		return selected;
	}

	private static void validateNumericColumn(ITableDataFrame frame, String column) {
		String type = frame.getMetaData().getHeaderTypeAsString(frame.getName() + "__" + column);
		if (!"INT".equalsIgnoreCase(type) && !"DOUBLE".equalsIgnoreCase(type)) {
			throw new IllegalArgumentException(
					"To Percent requires a numeric column; '" + column + "' is " + type + ".");
		}
	}

	private static boolean sharesPythonBindings(ITableDataFrame first, ITableDataFrame second) {
		return first instanceof PandasFrame original && second instanceof PandasFrame replacement
				&& ((original.getName() != null && original.getName().equals(replacement.getName()))
						|| (original.getWrapperName() != null
								&& original.getWrapperName().equals(replacement.getWrapperName())));
	}

	/**
	 * Keeps bindings and cached query selectors aligned with the BI result before
	 * publication.
	 */
	static void synchronizePythonFrame(ITableDataFrame frame, String outputVariable) {
		if (frame instanceof PandasFrame python) {
			python.runScript(outputVariable + " = " + python.getWrapperName() + ".cache['data']");
			python.syncHeaders();
		}
	}

	static AbstractReactor reactor(AutomationFrameOperation type, DataFrameTypeEnum frameType) {
		if (frameType == DataFrameTypeEnum.R) {
			throw new IllegalArgumentException(type.getValue() + " does not support R frames in Automation.");
		}
		if (type == AutomationFrameOperation.PURGE) {
			return new PurgeReactor();
		}
		if (frameType != DataFrameTypeEnum.PYTHON) {
			throw new IllegalArgumentException(type.getValue() + " requires a Python frame.");
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
			case ADD_COLUMN -> new prerna.reactor.frame.py.AddColumnReactor();
			case DUPLICATE_COLUMN -> new prerna.reactor.frame.py.DuplicateColumnReactor();
			case CONCATENATE -> new prerna.reactor.frame.py.JoinColumnsReactor();
			case PIVOT -> new prerna.reactor.frame.py.PivotReactor();
			case LOWER_CASE -> new prerna.reactor.frame.py.ToLowerCaseReactor();
			case PROPER_CASE -> new prerna.reactor.frame.py.ToProperCaseReactor();
			case SPLIT -> new prerna.reactor.frame.py.SplitColumnsReactor();
			case SPLIT_UNPIVOT -> new prerna.reactor.frame.py.SplitUnpivotReactor();
			case TIMESTAMP_DATA -> new prerna.reactor.frame.py.TimestampDataReactor();
			case TO_PERCENT -> new prerna.reactor.frame.py.ToPercentReactor();
			case TRANSPOSE -> new prerna.reactor.frame.py.TransposeReactor();
			case UNPIVOT -> new prerna.reactor.frame.py.UnpivotReactor();
			case UPPER_CASE -> new prerna.reactor.frame.py.ToUpperCaseReactor();
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

	private static void validateReplacement(List<String> columns, ITableDataFrame frame, String value,
			String newValue) {
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

	/**
	 * Restores DATE and TIMESTAMP dtypes only on the isolated Clean working frame.
	 */
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
