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
package prerna.query.parsers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class SqlTranslator {

	private Iterator<Collection<String>> it = null;
	private Map<String, List<String>> translationMap = null;

	public SqlTranslator(Map<String, List<String>> translationMap) {
		this.it = new CombinatorIterator(translationMap.values());
		this.translationMap = translationMap;

	}

	public Set<String> processQuery(String query) throws Exception {
		// get translation map combinations
		Set<String> mapKeys = translationMap.keySet();
		Object[] keyObj = mapKeys.toArray();
		Set<String> translatedQueries = new HashSet<>();
		while (it.hasNext()) {
			Map<String, String> mapCombo = new HashMap<>();
			Collection<String> mappings = it.next();
			Object[] values = mappings.toArray();
			for (int i = 0; i < values.length; i++) {
				String orgKey = (String) keyObj[i];
				String mapKey = (String) values[i];
				mapCombo.put(orgKey, mapKey);
			}
			translatedQueries.add(translateQuery(query, mapCombo));
		}

		return translatedQueries;
	}

	/**
	 * Replace query with new column/table names
	 * 
	 * @param query    Query to replace
	 * @param mapCombo {"oldName":"newName"}
	 * @return
	 * @throws JSQLParserException
	 */
	private String translateQuery(String query, Map<String, String> mapCombo) throws JSQLParserException {
		Select select = (Select) CCJSqlParserUtil.parse(query);
		StringBuilder buffer = new StringBuilder();
		ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
			@Override
			public <S> StringBuilder visit(Column tableColumn, S context) {
				Table table = tableColumn.getTable();
				if (table != null) {
					String tableName = table.getName();
					// replace table name
					if (mapCombo.containsKey(tableName)) {
						String newTableName = mapCombo.get(tableName);
						table.setName(newTableName);
					}
				}
				String colName = tableColumn.getColumnName();
				// replace column name
				if (mapCombo.containsKey(colName)) {
					String newColumnName = mapCombo.get(colName);
					tableColumn.setColumnName(newColumnName);
				}
				return super.visit(tableColumn, context);
			}
		};
		SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer) {
			@Override
			public <S> StringBuilder visit(Table table, S context) {
				String tableName = table.getName();
				// replace table name
				if (mapCombo.containsKey(tableName)) {
					String newTableName = mapCombo.get(tableName);
					table.setName(newTableName);
				}
				return super.visit(table, context);
			}
		};
		expressionDeParser.setSelectVisitor(deparser);
		expressionDeParser.setBuilder(buffer);
		// SelectDeParser is both a SelectVisitor and a FromItemVisitor, and Select
		// accepts
		// either, so the visitor type has to be pinned down for overload resolution
		select.accept((SelectVisitor<StringBuilder>) deparser, null);
		return buffer.toString();
	}

	/**
	 * Generate combinations for Lists
	 */
	private class CombinatorIterator implements Iterator<Collection<String>> {
		private final String[][] arrays;
		private final int[] indices;
		private final int total;
		private int counter;

		public CombinatorIterator(Collection<List<String>> collection) {
			Object[] col = collection.toArray();
			String[][] test = new String[col.length][];

			for (int i = 0; i < col.length; i++) {
				List<String> vals = (List) col[i];
				test[i] = new String[vals.size()];
				for (int j = 0; j < vals.size(); j++) {
					test[i][j] = vals.get(j);
				}
			}
			arrays = test;
			indices = new int[arrays.length];
			total = Arrays.stream(arrays).mapToInt(arr -> arr.length).reduce((x, y) -> x * y).orElse(0);
			counter = 0;
		}

		@Override
		public boolean hasNext() {
			return counter < total;
		}

		@Override
		public Collection<String> next() {
			List<String> nextValue = IntStream.range(0, arrays.length).mapToObj(i -> arrays[i][indices[i]])
					.collect(Collectors.toList());

			// rolling carry over the indices
			for (int j = 0; j < arrays.length && ++indices[j] == arrays[j].length; j++) {
				indices[j] = 0;
			}

			counter++;
			return nextValue;
		}
	}

}
