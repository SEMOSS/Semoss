package prerna.query.interpreters;

import java.util.List;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.solr.SolrEngine;
import prerna.engine.impl.solr.SolrIterator;
import prerna.query.interpreters.AbstractQueryInterpreter;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.QueryFilter;
import prerna.sablecc2.om.QueryFilter.FILTER_TYPE;

public class SolrInterpreter2 extends AbstractQueryInterpreter {

	public static final String QUERY_ALL = "*:*";

	// Solr api to query docs
	private SolrQuery query;

	public SolrInterpreter2() {
		this.query = new SolrQuery();
	}

	/**
	 * Compose solr query from query struct
	 * 
	 * @return
	 */
	public SolrQuery composeSolrQuery() {
		query.set("q", QUERY_ALL);
		addSelector();
		addFilters();
		addOrderBy();
		addLimitOffset();
		return query;
	}

	@Override
	public String composeQuery() {
		return query.toString();
	}

	private void addFilters() {
		List<QueryFilter> filters = qs.getFilters().getFilters();
		for (QueryFilter filter : filters) {
			FILTER_TYPE filterType = QueryFilter.determineFilterType(filter);
			NounMetadata leftComp = filter.getLComparison();
			NounMetadata rightComp = filter.getRComparison();
			String thisComparator = filter.getComparator();
			if (filterType == filterType.COL_TO_COL) {
				// TODO
			} else if (filterType == filterType.COL_TO_VALUES) {
				// column = ['value'] 
				filterColToValues(leftComp, rightComp, thisComparator);
			} else if (filterType == filterType.VALUES_TO_COL) {
				// here the left and rightcomps are reversed, so send them to
				// the method in opposite order and reverse comparator
				// value > column gets sent as column < value
				filterColToValues(rightComp, leftComp, QueryFilter.getReverseNumericalComparator(thisComparator));
			} else if (filterType == filterType.VALUE_TO_VALUE) {
				// ?????????
			}
		}
	}

	private void filterColToValues(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
		String fieldName = (String) leftComp.getValue();
		Object filterValue = rightComp.getValue();
		if (comparator.equals("=")) {
			// case where comparator is equal
			if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
				query.addFilterQuery(fieldName + ":" + (String) filterValue);
			} else if (rightComp.getNounType().equals(PixelDataType.CONST_INT)) {
				query.addFilterQuery(fieldName + ":[" + filterValue + " TO " + filterValue + "]");
			}
		} else if (comparator.equals("<")) {
			// TODO: less than?
		} else if (comparator.equals(">")) {
			// TODO
		} else if (comparator.equals("<=")) {
			query.addFilterQuery(fieldName + ":[ * TO " + filterValue + "]");
		} else if (comparator.equals(">=")) {
			query.addFilterQuery(fieldName + ":[" + filterValue + " TO * ]");
		} else if (comparator.equals("!=")) {
			if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
				query.addFilterQuery("!" + fieldName + ":" + filterValue);
			} else if (rightComp.getNounType().equals(PixelDataType.CONST_INT)) {
				query.addFilterQuery("!" + fieldName + ":" + "[" + filterValue + " TO " + filterValue + "]");
			}
		}

	}

	/**
	 * Add a sort by for the query
	 */
	private void addOrderBy() {
		List<QueryColumnOrderBySelector> orderBy = qs.getOrderBy();
		// get the list of keys
		for (QueryColumnOrderBySelector columnSelector : orderBy) {
			String table = columnSelector.getTable();
			String key = columnSelector.getAlias();
			String orderByCol = null;
			String sort = columnSelector.getSortDir().toString();
			if (sort.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
				orderByCol = key;
			} else {
				orderByCol = sort;
			}
			query.addSort(orderByCol, ORDER.asc);
		}
	}

	/**
	 * Add a limit/offset to the query
	 */
	private void addLimitOffset() {
		long limit = qs.getLimit();
		long offset = qs.getOffset();
		if (limit > 0) {
			query.setRows((int) limit);
		}
		if (offset >= 0) {
			query.setStart((int) offset);
		}
	}

	/**
	 * Add the solr fields for the selector
	 */
	private void addSelector() {
		List<IQuerySelector> selectorData = qs.getSelectors();
		List<String> selectorValues = new Vector<String>();
		// the keyset contains the unique key for the solr core
		// there should only be one
		// and everything else is a field
		for (IQuerySelector selector : selectorData) {
			String uniqueKey = selector.getAlias();
			selectorValues.add(uniqueKey);
			// List<String> fields = selectorData.get(uniqueKey);
			List<String> fields = new Vector<>();
			for (String field : fields) {
				if (field.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					continue;
				}
				selectorValues.add(field);
			}
		}
		this.query.setFields(selectorValues.toArray(new String[] {}));
	}

	@Override
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
	}

	public static void main(String[] args) {
		String solrBaseURL = "http://localhost:8080/solr/";
		String coreName = "insightCore";
		SolrEngine engine = new SolrEngine(solrBaseURL, coreName);
		engine.setEngineName("insightCore");
		// solr.openDB(null);
		System.out.println("Active " + engine.serverActive());
		QueryStruct2 qs = new QueryStruct2();
		qs.addSelector("id", null);
		qs.addSelector("id", "core_engine");
		qs.addSelector("id", "layout");

		// test filter
		NounMetadata test2 = new NounMetadata("core_engine", PixelDataType.COLUMN);
		NounMetadata test3 = new NounMetadata("movie", PixelDataType.CONST_STRING);
		QueryFilter filter1 = new QueryFilter(test2, "=", test3);
		qs.addFilter(filter1);
		// qs.addFilter("view_count", ">=", Arrays.asList(new Object[]{10}));

		SolrInterpreter2 solrInterp = new SolrInterpreter2();
		solrInterp.setQueryStruct(qs);
		SolrQuery query = solrInterp.composeSolrQuery();

		IRawSelectWrapper solrWrapper = new QueryStructExpressionIterator(new SolrIterator(((SolrEngine) engine).execSolrQuery(query), qs), qs);
		while (solrWrapper.hasNext()) {
			System.out.println(solrWrapper.next());
		}
	}

}
