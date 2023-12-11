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
	 * @param query      Query to replace
	 * @param mapCombo   {"oldName":"newName"}
	 * @return
	 * @throws JSQLParserException
	 */
	private String translateQuery(String query, Map<String, String> mapCombo) throws JSQLParserException {
		Select select = (Select) CCJSqlParserUtil.parse(query);
		StringBuilder buffer = new StringBuilder();
		ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
			@Override
			public void visit(Column tableColumn) {
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
				super.visit(tableColumn);
			}
		};
		SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer) {
			@Override
			public void visit(Table table) {
				String tableName = table.getName();
				// replace table name
				if (mapCombo.containsKey(tableName)) {
					String newTableName = mapCombo.get(tableName);
					table.setName(newTableName);
				}
				super.visit(table);
			}
		};
		expressionDeParser.setSelectVisitor(deparser);
		expressionDeParser.setBuffer(buffer);
		select.getSelectBody().accept(deparser);
		return buffer.toString();
	}

//	public static void main(String args[]) throws JSQLParserException {
//
//		Map<String, List<String>> map = new HashMap<>();
//		map.put("a", Arrays.asList(new String[] { "x", "y" }));
//		map.put("b", Arrays.asList(new String[] { "z", "d" }));
//		map.put("c", Arrays.asList(new String[] { "d", "f" }));
//		String sql = "select a , b from t";
//
//		sql = "SELECT t1.a, t2.b FROM t t1 INNER JOIN t t2 ON t1.a = t2.b";
//		sql = "select min(a)from(select distinct b from t order by salary desc)where rownum<=2;";
//		sql = "Select * from Employee a where rowid <>( select max(rowid) from Employee b where a.Employee_num=b.Employee_num);";
//		sql = " Select * from Employee where t =1;";
//		sql = "SELECT DISTINCT TEDI.a AS \"DeersEnrollmentFacilityName\" , TEDI.b AS \"totalPatientCostShare\" FROM t TEDI";
//		sql = "select distinct salary from employee a where 3 >= (select count(distinct salary) from employee b where a.salary <= b.salary) order by a.salary desc;";
//		sql = "select a, b from c;";
//		SqlTranslator translator = new SqlTranslator(map);
//		try {
//			Set<String> queries = translator.processQuery(sql);
//			for(String q: queries) {
//				System.out.println(q);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//	}
	
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


