package prerna.unit.ds.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.ds.QueryStruct;
import prerna.ds.util.QueryStructConverter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.RelationSet;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryStructConverterUnitTests {

	@Test
	void testConvertOldQueryStruct() {

		QueryStruct oldQs = new QueryStruct();

		Map<String, List<String>> selectors = new Hashtable<>();
		selectors.put("table1", Arrays.asList("column1", "column2"));
		selectors.put("table2", Arrays.asList("column3", "column4"));
		oldQs.selectors = selectors;

		Map<String, Map<String, List>> andfilters = new Hashtable<>();
		Map<String, List> numericFilterMap = new Hashtable<>();
		numericFilterMap.put(">", Arrays.asList(100, 200)); // Numeric values
		andfilters.put("column1", numericFilterMap);

		Map<String, List> stringFilterMap = new Hashtable<>();
		stringFilterMap.put("=", Arrays.asList("value1", "value2")); // String values
		andfilters.put("column2", stringFilterMap);

		Map<String, List> tableFilterMap = new Hashtable<>();
		tableFilterMap.put("<", Arrays.asList(50)); // Numeric value
		andfilters.put("table1", tableFilterMap);

		oldQs.andfilters = andfilters;

		Map<String, Map<String, List>> relations = new Hashtable<>();
		Map<String, List> relationMap = new Hashtable<>();
		relationMap.put("joinType", Arrays.asList("table2"));
		relations.put("table1", relationMap);
		oldQs.relations = relations;

		oldQs.addGroupBy("table1", "column1");

		oldQs.setOrderBy("table1", "column1");

		SelectQueryStruct newQs = QueryStructConverter.convertOldQueryStruct(oldQs);

		List<QueryColumnSelector> expectedSelectors = Arrays.asList(new QueryColumnSelector("table2"),
				new QueryColumnSelector("table2__column3"), new QueryColumnSelector("table2__column4"),
				new QueryColumnSelector("table1"), new QueryColumnSelector("table1__column1"),
				new QueryColumnSelector("table1__column2")

		);
		assertEquals(expectedSelectors, newQs.getSelectors());

		List<QueryColumnSelector> expectedGroupBys = Arrays.asList(new QueryColumnSelector("table1__column1"));
		assertEquals(expectedGroupBys, newQs.getGroupBy());

		List<QueryColumnOrderBySelector> expectedOrderBys = Arrays
				.asList(new QueryColumnOrderBySelector("table1__column1"));
		assertEquals(expectedOrderBys, newQs.getOrderBy());
	}
}
