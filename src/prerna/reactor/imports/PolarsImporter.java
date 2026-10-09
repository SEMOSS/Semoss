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
 * Licensed under the Apache License, Version 2.0 or GPLv2, as applicable.
 *******************************************************************************/
package prerna.reactor.imports;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PolarsFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.Join;
import prerna.query.querystruct.filters.IQueryFilter;

public class PolarsImporter extends AbstractImporter {

	private final PolarsFrame frame;
	private final SelectQueryStruct queryStruct;
	private Iterator<IHeadersDataRow> iterator;

	public PolarsImporter(PolarsFrame frame, SelectQueryStruct queryStruct) {
		this(frame, queryStruct, null);
	}

	public PolarsImporter(PolarsFrame frame, SelectQueryStruct queryStruct, Iterator<IHeadersDataRow> iterator) {
		this.frame = frame;
		this.queryStruct = queryStruct;
		this.iterator = iterator;
		if (this.iterator == null) {
			try {
				this.iterator = ImportUtility.generateIterator(queryStruct, frame);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error executing the source query for Polars import", e);
			}
		}
	}

	@Override
	public void insertData() {
		ImportUtility.parseQueryStructToFlatTable(this.frame, this.queryStruct, this.frame.getName(), this.iterator,
				false);
		this.frame.addRowsViaIterator(this.iterator);
	}

	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.frame.setMetaData(metaData);
		this.frame.addRowsViaIterator(this.iterator);
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		if (joins == null || joins.isEmpty()) {
			throw new IllegalArgumentException("At least one join condition is required");
		}
		List<String> headers = new ArrayList<>();
		this.queryStruct.getSelectors().forEach(selector -> headers.add(selector.getAlias()));
		Map<String, SemossDataType> typeMap = ImportUtility.getTypesFromQs(this.queryStruct, this.iterator);
		Map<String, String> schema = new LinkedHashMap<>();
		for (String header : headers) {
			SemossDataType type = typeMap.get(header);
			if (type == null) {
				type = typeMap.get(this.frame.getName() + "__" + header);
			}
			schema.put(header, SemossDataType.convertDataTypeToString(type));
		}

		List<List<Object>> rows = new ArrayList<>();
		while (this.iterator.hasNext()) {
			Object[] values = this.iterator.next().getValues();
			List<Object> row = new ArrayList<>(values.length);
			for (Object value : values) {
				row.add(value);
			}
			rows.add(row);
		}

		List<String> leftOn = new ArrayList<>();
		List<String> rightOn = new ArrayList<>();
		String joinType = null;
		for (Join join : joins) {
			if (!IQueryFilter.comparatorIsEquals(join.getComparator())) {
				throw new IllegalArgumentException("Polars merge currently supports equality joins only");
			}
			if (joinType != null && !joinType.equals(join.getJoinType())) {
				throw new IllegalArgumentException("Mixed Polars join types are unsupported");
			}
			joinType = join.getJoinType();
			leftOn.add(cleanColumn(join.getLColumn()));
			rightOn.add(cleanColumn(join.getRColumn()));
		}
		this.frame.mergeRows(headers, rows, schema, leftOn, rightOn, mapJoinType(joinType));
		return this.frame;
	}

	private static String cleanColumn(String column) {
		return column != null && column.contains("__") ? column.split("__", 2)[1] : column;
	}

	private static String mapJoinType(String joinType) {
		String normalized = joinType == null ? "inner" : joinType.toLowerCase();
		if (normalized.contains("left")) {
			return "left";
		}
		if (normalized.contains("right")) {
			return "right";
		}
		if (normalized.contains("outer") || normalized.contains("full")) {
			return "full";
		}
		if (normalized.contains("anti")) {
			return "anti";
		}
		if (normalized.contains("semi")) {
			return "semi";
		}
		return "inner";
	}
}
