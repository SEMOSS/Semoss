//package prerna.query.interpreters;
//
//import java.util.List;
//import java.util.Vector;
//
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrQuery.ORDER;
//
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.filters.IQueryFilter;
//import prerna.query.querystruct.filters.SimpleQueryFilter;
//import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
//import prerna.query.querystruct.selectors.IQuerySelector;
//import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//
//public class SolrInterpreter extends AbstractQueryInterpreter {
//
//	public static final String QUERY_ALL = "*:*";
//
//	// Solr api to query docs
//	private SolrQuery query;
//
//	public SolrInterpreter() {
//		this.query = new SolrQuery();
//	}
//
//	/**
//	 * Compose solr query from query struct
//	 * 
//	 * @return
//	 */
//	public SolrQuery composeSolrQuery() {
//		query.set("q", QUERY_ALL);
//		addSelector();
//		addFilters();
//		addOrderBy();
//		addLimitOffset();
//		return query;
//	}
//
//	@Override
//	public String composeQuery() {
//		return query.toString();
//	}
//
//	private void addFilters() {
//		List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
//		for (IQueryFilter f : filters) {
//			if(f.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
//				SimpleQueryFilter filter = (SimpleQueryFilter) f;
//				FILTER_TYPE filterType = filter.getFilterType();
//				NounMetadata leftComp = filter.getLComparison();
//				NounMetadata rightComp = filter.getRComparison();
//				String thisComparator = filter.getComparator();
//				if (filterType == FILTER_TYPE.COL_TO_COL) {
//					// TODO:
//					// TODO:
//					// TODO:
//					// TODO:
//				} else if (filterType == FILTER_TYPE.COL_TO_VALUES) {
//					// column = ['value'] 
//					filterColToValues(leftComp, rightComp, thisComparator);
//				} else if (filterType == FILTER_TYPE.VALUES_TO_COL) {
//					// here the left and rightcomps are reversed, so send them to
//					// the method in opposite order and reverse comparator
//					// value > column gets sent as column < value
//					filterColToValues(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
//				}
//			}
//		}
//	}
//
//	private void filterColToValues(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
//		String fieldName = ((IQuerySelector) leftComp.getValue()).getAlias();
//		Object filterValue = rightComp.getValue();
//		if (comparator.equals("=")) {
//			// case where comparator is equal
//			if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
//				query.addFilterQuery(fieldName + ":" + (String) filterValue);
//			} else if (rightComp.getNounType().equals(PixelDataType.CONST_INT)) {
//				query.addFilterQuery(fieldName + ":[" + filterValue + " TO " + filterValue + "]");
//			}
//		} else if (comparator.equals("<")) {
//			// TODO: less than?
//		} else if (comparator.equals(">")) {
//			// TODO
//		} else if (comparator.equals("<=")) {
//			query.addFilterQuery(fieldName + ":[ * TO " + filterValue + "]");
//		} else if (comparator.equals(">=")) {
//			query.addFilterQuery(fieldName + ":[" + filterValue + " TO * ]");
//		} else if (comparator.equals("!=")) {
//			if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
//				query.addFilterQuery("!" + fieldName + ":" + filterValue);
//			} else if (rightComp.getNounType().equals(PixelDataType.CONST_INT)) {
//				query.addFilterQuery("!" + fieldName + ":" + "[" + filterValue + " TO " + filterValue + "]");
//			}
//		}
//
//	}
//
//	/**
//	 * Add a sort by for the query
//	 */
//	private void addOrderBy() {
//		List<QueryColumnOrderBySelector> orderBy = qs.getOrderBy();
//		// get the list of keys
//		for (QueryColumnOrderBySelector columnSelector : orderBy) {
//			String table = columnSelector.getTable();
//			String key = columnSelector.getAlias();
//			String orderByCol = null;
//			String sort = columnSelector.getSortDir().toString();
//			if (sort.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
//				orderByCol = key;
//			} else {
//				orderByCol = sort;
//			}
//			query.addSort(orderByCol, ORDER.asc);
//		}
//	}
//
//	/**
//	 * Add a limit/offset to the query
//	 */
//	private void addLimitOffset() {
//		long limit = qs.getLimit();
//		long offset = qs.getOffset();
//		if (limit > 0) {
//			query.setRows((int) limit);
//		}
//		if (offset >= 0) {
//			query.setStart((int) offset);
//		}
//	}
//
//	/**
//	 * Add the solr fields for the selector
//	 */
//	private void addSelector() {
//		List<IQuerySelector> selectorData = qs.getSelectors();
//		List<String> selectorValues = new Vector<String>();
//		// the keyset contains the unique key for the solr core
//		// there should only be one
//		// and everything else is a field
//		for (IQuerySelector selector : selectorData) {
//			String uniqueKey = selector.getAlias();
//			selectorValues.add(uniqueKey);
//			// List<String> fields = selectorData.get(uniqueKey);
//			List<String> fields = new Vector<>();
//			for (String field : fields) {
//				if (field.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
//					continue;
//				}
//				selectorValues.add(field);
//			}
//		}
//		this.query.setFields(selectorValues.toArray(new String[] {}));
//	}
//}
