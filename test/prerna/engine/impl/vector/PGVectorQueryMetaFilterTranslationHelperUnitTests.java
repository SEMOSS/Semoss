package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;

public class PGVectorQueryMetaFilterTranslationHelperUnitTests {
	
	private List<IQueryFilter> filters;
	private String tableName;
	private AndQueryFilter andFilter;
	private OrQueryFilter orFilter;
	private BetweenQueryFilter betweenFilter;
	
	@BeforeEach
	void setUp() {
		filters = new Vector<>();
		tableName = "TEST_TABLE";
		andFilter = new AndQueryFilter();
		orFilter = new OrQueryFilter();
	}

	@Test
	void testConvertFilters() {
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__IS_LATEST", "==", true));
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__IS_DELETED", "==", false));
		
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__TOKEN", ">", 10));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__TOKEN", "<=", 5));
		
		SimpleQueryFilter simpleFilter = SimpleQueryFilter.makeColToValFilter(tableName+"__Attribute", "==", "sourceDoc");
		
		filters.add(simpleFilter);
		filters.add(andFilter);
		filters.add(orFilter);
		
		List<IQueryFilter> output = PGVectorQueryMetaFitlerTranslationHelper.convertFilters(filters, tableName);
		assertEquals(filters.size(), output.size());
		for(int filterIdx = 0; filterIdx < filters.size(); filterIdx++) {
			IQueryFilter inputFilter = filters.get(filterIdx);
			IQueryFilter outputFilter = output.get(filterIdx);
			if (inputFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				assertEquals(IQueryFilter.QUERY_FILTER_TYPE.AND, outputFilter.getQueryFilterType());
			} else {
				assertEquals(inputFilter.getQueryFilterType(), outputFilter.getQueryFilterType());
			}
			outputFilter.containsColumn("Attribute");
			Set<String> usedTables = outputFilter.getAllUsedTables();
			usedTables.forEach(tblName -> assertEquals(tblName, tableName));
		}
	}
	
	@Test
	void testConvertFiltersEmptyOrNull() {
		List<IQueryFilter> origFilters = new Vector<>();
		List<IQueryFilter> returnFilters = PGVectorQueryMetaFitlerTranslationHelper.convertFilters(origFilters, "table_name");
		assertEquals(origFilters.size(), returnFilters.size());
		// the method returns the same filters object if it is empty
		assertTrue(origFilters == returnFilters);
		
		returnFilters = PGVectorQueryMetaFitlerTranslationHelper.convertFilters(null, "table_name");
		assertNull(returnFilters);
	}
}
