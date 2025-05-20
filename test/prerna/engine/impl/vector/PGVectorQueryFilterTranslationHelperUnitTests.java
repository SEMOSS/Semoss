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
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class PGVectorQueryFilterTranslationHelperUnitTests {
	
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
		betweenFilter = new BetweenQueryFilter();
	}

	@Test
	void testConvertFilters() {
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__IS_LATEST", "==", true));
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__IS_DELETED", "==", false));
		
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__TOKEN", ">", 10));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName+"__TOKEN", "<=", 5));
		
		betweenFilter.setColumn(new QueryColumnSelector(tableName+"__CHUNK_SIZE"));
		betweenFilter.setStart(4);
		betweenFilter.setEnd(8);
		
		SimpleQueryFilter simpleFilter = SimpleQueryFilter.makeColToValFilter(tableName+"__SOURCE", "==", "sourceDoc");
		
		filters.add(simpleFilter);
		filters.add(andFilter);
		filters.add(orFilter);
		filters.add(betweenFilter);
		
		List<IQueryFilter> output = PGVectorQueryFitlerTranslationHelper.convertFilters(filters, tableName);
		assertEquals(filters.size(), output.size());
		for(int filterIdx = 0; filterIdx < filters.size(); filterIdx++) {
			IQueryFilter inputFilter = filters.get(filterIdx);
			IQueryFilter outputFilter = output.get(filterIdx);
			assertEquals(inputFilter.getQueryFilterType(), outputFilter.getQueryFilterType());			
			Set<String> targetCols = inputFilter.getAllUsedColumns();
			targetCols.forEach(column -> assertTrue(outputFilter.containsColumn(column)));
			Set<String> usedTables = outputFilter.getAllUsedTables();
			usedTables.forEach(tblName -> assertEquals(tblName, tableName));
		}
	}
	
	@Test
	void testConvertFiltersEmptyOrNull() {
		List<IQueryFilter> origFilters = new Vector<>();
		List<IQueryFilter> returnFilters = PGVectorQueryFitlerTranslationHelper.convertFilters(origFilters, "table_name");
		assertEquals(origFilters.size(), returnFilters.size());
		// the method returns the same filters object if it is empty
		assertTrue(origFilters == returnFilters);
		
		returnFilters = PGVectorQueryFitlerTranslationHelper.convertFilters(null, "table_name");
		assertNull(returnFilters);
	}
	
	@Test
	void testConvertSelector() {
		QueryColumnSelector qcs1 = new QueryColumnSelector(tableName + "__OPTION_1");
		QueryConstantSelector qcs2 = new QueryConstantSelector("VALUE");
		QueryFunctionSelector queryFunctionSelector = new QueryFunctionSelector();
		queryFunctionSelector.addInnerSelector(qcs1);
		queryFunctionSelector.addInnerSelector(qcs2);
		queryFunctionSelector.setAlias("OPTION_CONCAT");
		queryFunctionSelector.setFunction(QueryFunctionHelper.CONCAT);
		
		IQuerySelector outputSelector = PGVectorQueryFitlerTranslationHelper.convertSelector(queryFunctionSelector, tableName);
		assertEquals(queryFunctionSelector.getAlias(), outputSelector.getAlias());	
		assertEquals(queryFunctionSelector.getSelectorType(), outputSelector.getSelectorType());
		assertEquals(queryFunctionSelector.getFunction(), ((QueryFunctionSelector)outputSelector).getFunction());
		List<IQuerySelector> inputInnerSelectors = queryFunctionSelector.getInnerSelector();
		List<IQuerySelector> outputInnerSelectors = ((QueryFunctionSelector)outputSelector).getInnerSelector();
		assertEquals(inputInnerSelectors.size(), outputInnerSelectors.size());
		for (int slctrIdx = 0; slctrIdx < inputInnerSelectors.size(); slctrIdx++) {
			IQuerySelector inputInnerSelector = inputInnerSelectors.get(slctrIdx);
			IQuerySelector outputInnerSelector = outputInnerSelectors.get(slctrIdx);
			assertEquals(inputInnerSelector.getSelectorType(), outputInnerSelector.getSelectorType());
		}
	}
}
