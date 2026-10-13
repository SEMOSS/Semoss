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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.reactor.automation.run;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.task.BasicIteratorTask;

/**
 * Immutable, bounded table data exchanged by Automation nodes in Java scope.
 */
final class AutomationTableValue {

	static final String TYPE_KEY = "__semossAutomationType";
	static final String TABLE_TYPE = "table";

	private final List<Column> columns;
	private final List<List<Object>> rows;
	private final long byteCount;

	private AutomationTableValue(List<Column> columns, List<List<Object>> rows, long byteCount) {
		this.columns = List.copyOf(columns);
		this.rows = rows.stream()
				.map(row -> Collections.unmodifiableList(new ArrayList<>(row)))
				.toList();
		this.byteCount = byteCount;
	}

	static AutomationTableValue fromTask(BasicIteratorTask task, String nodeId) {
		List<Map<String, Object>> headerInfo = task.getHeaderInfo();
		List<Column> columns = new ArrayList<>(headerInfo == null ? 0 : headerInfo.size());
		if (headerInfo != null) {
			for (int index = 0; index < headerInfo.size(); index++) {
				Map<String, Object> header = headerInfo.get(index);
				Object rawName = header.get("alias");
				if (rawName == null) {
					rawName = header.get("header");
				}
				String name = rawName == null ? "column_" + index : rawName.toString();
				Object rawType = header.get("dataType");
				columns.add(new Column(name, rawType == null ? null : rawType.toString()));
			}
		}

		List<List<Object>> rows = new ArrayList<>();
		long bytes = encodedSize(columns);
		while (task.hasNext()) {
			IHeadersDataRow row = task.next();
			if (columns.isEmpty()) {
				String[] headers = row.getHeaders();
				Object[] values = row.getValues();
				for (int index = 0; index < values.length; index++) {
					columns.add(new Column(headers != null && index < headers.length ? headers[index] : "column_" + index,
							null));
				}
				bytes = encodedSize(columns);
			}
			List<Object> normalized = normalizeRow(row.getValues(), null);
			if (normalized.size() != columns.size()) {
				throw new IllegalStateException("Automation node '" + nodeId
						+ "' produced a row that does not match its table schema.");
			}
			bytes += encodedSize(normalized);
			if (rows.size() >= AutomationConstants.DB_QUERY_MAX_LIMIT
					|| bytes > AutomationConstants.RUN_SCOPE_MAX_BYTES) {
				throw new IllegalStateException("Automation node '" + nodeId
						+ "' exceeded the live Java table limit of "
						+ AutomationConstants.DB_QUERY_MAX_LIMIT + " rows or "
						+ AutomationConstants.RUN_SCOPE_MAX_BYTES + " encoded bytes.");
			}
			rows.add(normalized);
		}
		return new AutomationTableValue(columns, rows, bytes);
	}

	static AutomationTableValue fromEnvelope(Map<?, ?> envelope, String nodeId) {
		if (!TABLE_TYPE.equals(envelope.get(TYPE_KEY))) {
			throw new IllegalArgumentException("Value is not an Automation table envelope.");
		}
		Object rawColumns = envelope.get("columns");
		Object rawRows = envelope.get("rows");
		if (!(rawColumns instanceof List<?> columnList) || !(rawRows instanceof List<?> rowList)) {
			throw new IllegalStateException("Automation node '" + nodeId + "' returned a malformed table.");
		}
		List<Column> columns = new ArrayList<>(columnList.size());
		for (Object rawColumn : columnList) {
			if (!(rawColumn instanceof Map<?, ?> column) || !(column.get("name") instanceof String name)
					|| name.isBlank()) {
				throw new IllegalStateException("Automation node '" + nodeId + "' returned a malformed table schema.");
			}
			Object type = column.get("type");
			columns.add(new Column(name, type == null ? null : type.toString()));
		}
		List<List<Object>> rows = new ArrayList<>(rowList.size());
		long bytes = encodedSize(columns);
		for (Object rawRow : rowList) {
			if (!(rawRow instanceof List<?> row) || row.size() != columns.size()) {
				throw new IllegalStateException("Automation node '" + nodeId + "' returned a malformed table row.");
			}
			List<Object> normalized = normalizeRow(row.toArray(), null);
			bytes += encodedSize(normalized);
			if (rows.size() >= AutomationConstants.DB_QUERY_MAX_LIMIT
					|| bytes > AutomationConstants.RUN_SCOPE_MAX_BYTES) {
				throw new IllegalStateException("Automation node '" + nodeId
						+ "' exceeded the live Java table limit.");
			}
			rows.add(normalized);
		}
		return new AutomationTableValue(columns, rows, bytes);
	}

	List<Column> columns() {
		return columns;
	}

	List<List<Object>> rows() {
		return rows;
	}

	long rowCount() {
		return rows.size();
	}

	long byteCount() {
		return byteCount;
	}

	Map<String, Object> envelope() {
		Map<String, Object> value = new LinkedHashMap<>();
		value.put(TYPE_KEY, TABLE_TYPE);
		value.put("columns", columns.stream().map(Column::asMap).toList());
		value.put("rows", rows);
		return value;
	}

	Map<String, Object> promptValue() {
		int includedRows = Math.min(rows.size(), 100);
		Map<String, Object> value = new LinkedHashMap<>();
		value.put("dataType", TABLE_TYPE);
		value.put("rowCount", (long) rows.size());
		value.put("columns", columns.stream().map(Column::asMap).toList());
		value.put("rows", rows.subList(0, includedRows));
		value.put("truncated", rows.size() > includedRows);
		return value;
	}

	Map<String, Object> summary() {
		Map<String, Object> value = new LinkedHashMap<>();
		value.put("dataType", TABLE_TYPE);
		value.put("rowCount", (long) rows.size());
		value.put("columnCount", columns.size());
		return value;
	}

	List<Object> pageAsMaps(long offset, int limit) {
		if (offset < 0 || limit < 0 || offset + limit > rows.size()) {
			throw new IllegalArgumentException("Table page is outside the live row range.");
		}
		List<Object> page = new ArrayList<>(limit);
		for (int rowIndex = Math.toIntExact(offset); rowIndex < offset + limit; rowIndex++) {
			Map<String, Object> row = new LinkedHashMap<>();
			for (int column = 0; column < columns.size(); column++) {
				row.put(columns.get(column).name(), rows.get(rowIndex).get(column));
			}
			page.add(row);
		}
		return page;
	}

	private static long encodedSize(Object value) {
		return AutomationRuntimeUtils.toRuntimeJson(value).getBytes(StandardCharsets.UTF_8).length;
	}

	static List<Object> normalizeRow(Object[] values, SemossDataType[] types) {
		List<Object> normalized = new ArrayList<>(values.length);
		for (int index = 0; index < values.length; index++) {
			Object value = values[index];
			SemossDataType type = types != null && index < types.length ? types[index] : null;
			if (value instanceof SemossDate date && date.getZonedDateTime() != null) {
				normalized.add(date.getZonedDateTime().toString());
			} else if (value instanceof java.sql.Date date) {
				normalized.add(date.toLocalDate().toString());
			} else if (value instanceof Timestamp timestamp) {
				normalized.add(timestamp.toInstant().toString());
			} else if (value instanceof java.util.Date date) {
				normalized.add(date.toInstant().toString());
			} else if (value instanceof TemporalAccessor) {
				normalized.add(value.toString());
			} else if (type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				normalized.add(value == null ? null : value.toString());
			} else {
				normalized.add(value);
			}
		}
		return normalized;
	}

	record Column(String name, String type) {
		Map<String, Object> asMap() {
			Map<String, Object> value = new LinkedHashMap<>();
			value.put("name", name);
			value.put("type", type);
			return value;
		}
	}
}
