package prerna.engine.impl.solr;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;

import prerna.ds.QueryStruct;
import prerna.rdf.query.builder.IQueryInterpreter;

public class SolrInterpreter implements IQueryInterpreter {
	
	public static final String QUERY_ALL = "*:*";

	private QueryStruct qs;
	private SolrQuery query;
	private int performCount;

	public SolrInterpreter() {
		this.query = new SolrQuery();
	}

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
	

	public void addFilters() { 
		Map<String, Map<String, List>> filters = qs.andfilters;
		for (String concept_property : filters.keySet()) {
			Map<String, List> compHash = filters.get(concept_property);
			String fieldName = concept_property;
			if(concept_property.contains("__")) {
				fieldName = concept_property.split("__")[1];
			}

			for (String filterType : compHash.keySet()) {
				List filterVals = compHash.get(filterType);
				for (Object s : filterVals) {
					if (filterType.equals("=")) {
						// case where comparator is equal
						if (s instanceof String) {
							query.addFilterQuery(fieldName + ":" + (String) s);
						} else if (s instanceof Number) {
							query.addFilterQuery(fieldName + ":[" + s + " TO " + s + "]");
						}
					} else if (filterType.equals("<")) {
						// TODO: less than?
					} else if (filterType.equals(">")) {
						
					} else if (filterType.equals("<=")) {
						query.addFilterQuery(fieldName + ":[ * TO " + s + "]");

					} else if (filterType.equals(">=")) {
						query.addFilterQuery(fieldName + ":[" + s + " TO * ]");

					} else if (filterType.equals("!=")) {
						if (s instanceof String) {
							query.addFilterQuery("!" + fieldName + ":" + s);
						} else if (s instanceof Number) {
							query.addFilterQuery("!" + fieldName + ":" + "[" + s + " TO " + s + "]");
						}
					}
				}
			}
		}
	}

	/**
	 * Add a sort by for the query
	 */
	public void addOrderBy() {
		Map<String, String> orderBy = qs.getOrderBy();
		// get the list of keys
		for (String key : orderBy.keySet()) {
			String orderByCol = null;
			String sort = orderBy.get(key);
			if(sort.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
	public void addLimitOffset() {
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
	 * Add the fields for the selector
	 */
	private void addSelector() {
		Map<String, List<String>> selectors = qs.getSelectors();
		List<String> selectorValues = new Vector<String>();
		// the keyset contains the unique key for the solr core
		// there should only be one
		// and everything else is a field
		for (String unqiueKey : selectors.keySet()) {
			selectorValues.add(unqiueKey);
			List<String> fields = selectors.get(unqiueKey);
			for(String field : fields) {
				if(field.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) {
					continue;
				}
				selectorValues.add(field);
			}
		}
		this.query.setFields(selectorValues.toArray(new String[]{}));
	}

	@Override
	public void setPerformCount(int performCount) {
		this.performCount = performCount;
	}
	
	@Override
	public int isPerformCount() {
		return performCount;
	}

	
	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}

	public void addCount() {
		int performCountInt = qs.getPerformCount();
		if (performCountInt == QueryStruct.NO_COUNT) {
			return;
		} else if (performCountInt == QueryStruct.COUNT_CELLS) {
			} else if (performCountInt == QueryStruct.COUNT_DISTINCT_SELECTORS) {

		}
	}

	@Override
	public void clear() {

	}

	public static void main(String[] args) {
		SolrEngine solr = new SolrEngine();
		solr.setEngineName("insightCore");
		solr.openDB(null);
		System.out.println("Active " + solr.serverActive());
	}

}
