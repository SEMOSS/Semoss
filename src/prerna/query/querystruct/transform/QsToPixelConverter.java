package prerna.query.querystruct.transform;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QsToPixelConverter {
	
	private QsToPixelConverter() {

	}
	
	public static String getHardQsPixel(HardSelectQueryStruct qs) {
		StringBuilder b = new StringBuilder();
		b.append("Database(").append(qs.getEngineId()).append(") | Query(")
			.append(qs.getQuery().replace("\"", "\\\"")).append(")");
		return b.toString();
	}

	public static String getPixel(SelectQueryStruct qs) {
		if(qs instanceof HardSelectQueryStruct) {
			return getHardQsPixel((HardSelectQueryStruct) qs);
		}

		// grab all the selectors
		StringBuilder selectBuilder = new StringBuilder();
		selectBuilder.append("Select(");
		List<IQuerySelector> origSelectors = qs.getSelectors();
		for(int i = 0; i < origSelectors.size(); i++) {
			if(i != 0) {
				selectBuilder.append(",");
			}
			IQuerySelector origS = origSelectors.get(i);
			selectBuilder.append(convertSelector(origS));
		}
		selectBuilder.append(").as([");
		for(int i = 0; i < origSelectors.size(); i++) {
			if(i != 0) {
				selectBuilder.append(",");
			}
			IQuerySelector origS = origSelectors.get(i);
			selectBuilder.append(origS.getAlias());
		}
		selectBuilder.append("])");
		
		// now go through the filters
		boolean hasFilters = false;
		StringBuilder filterBuilder = new StringBuilder();
		String implicitFilters = convertGenRowFilters(qs.getExplicitFilters());
		if(implicitFilters != null) {
			hasFilters = true;
			filterBuilder.append("Filter(").append(implicitFilters).append(")");
		}
		
		boolean hasHaving = false;
		StringBuilder havingFilterBuilder = new StringBuilder();
		String havingFilters = convertGenRowFilters(qs.getHavingFilters());
		if(havingFilters != null) {
			hasHaving = true;
			havingFilterBuilder.append("Having(").append(havingFilters).append(")");
		}
		
		// now go through the joins
		boolean hasJoins = false;
		StringBuilder joinBuilder = new StringBuilder();
		String joins = convertJoins(qs.getRelations());
		if(joins != null && !joins.isEmpty()) {
			hasJoins = true;
			joinBuilder.append("Join(").append(joins).append(")");
		}

		// now go through group bys
		boolean hasGroups = false;
		StringBuilder groupBuilder = new StringBuilder();
		List<QueryColumnSelector> origGroups = qs.getGroupBy();
		if(!origGroups.isEmpty()) {
			hasGroups = true;
			groupBuilder.append("Group(");
			for(int i = 0; i < origGroups.size(); i++) {
				if(i != 0) {
					groupBuilder.append(",");
				}
				IQuerySelector origGroupS = origGroups.get(i);
				groupBuilder.append(convertSelector(origGroupS));
			}
			groupBuilder.append(")");
		}
		
		//TODO: add order!!!
//		// now go through the order by
//		List<QueryColumnOrderBySelector> origOrders = qs.getOrderBy();
//		if(origOrders != null && !origOrders.isEmpty()) {
//			List<QueryColumnOrderBySelector> convertedOrderBys =  new Vector<QueryColumnOrderBySelector>();
//			for(int i = 0; i < origOrders.size(); i++) {
//				QueryColumnOrderBySelector origOrderS = origOrders.get(i);
//				QueryColumnOrderBySelector convertedOrderByS = convertOrderBySelector(origOrderS);
//				convertedOrderBys.add(convertedOrderByS);
//			}
//			convertedQs.setOrderBy(convertedOrderBys);
//		}
		
		StringBuilder pixel = new StringBuilder();
		pixel.append(selectBuilder.toString());
		if(hasFilters) {
			pixel.append(" | ").append(filterBuilder.toString());
		}
		if(hasJoins) {
			 pixel.append(" | ").append(joinBuilder.toString());
		}
		if(hasGroups) {
			pixel.append(" | ").append(groupBuilder.toString());
		}
		if(hasHaving) {
			pixel.append(" | ").append(havingFilterBuilder.toString());
		}
		if(qs.getLimit() > 0) {
			pixel.append(" | Limit(").append(qs.getLimit()).append(")");
		}
		if(qs.getOffset() > 0) {
			pixel.append(" | Offset(").append(qs.getOffset()).append(")");
		}
		return pixel.toString();
	}

	/**
	 * Convert the joins into its pixel string
	 * Does not include the Join syntax but only provides the contents inside
	 * @param joins
	 * @return
	 */
	public static String convertJoins(Set<String[]> joins) {
		StringBuilder b = new StringBuilder();
		boolean first = true;
		// iterate through and construct the joins
		for(String[] j : joins) {
			if(!first) {
				b.append(", ");
			}
			String startCol = j[0];
			String comparator = j[1];
			String endCol = j[2];
			
			b.append("(").append(startCol).append(", ").append(comparator)
				.append(", ").append(endCol).append(")");
			first = false;
		}
		return b.toString();
	}

	/**
	 * Converts a selector into its pixel string
	 * @param selector
	 * @return
	 */
	public static String convertSelector(IQuerySelector selector) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return convertConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return convertColumnSelector((QueryColumnSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return convertFunctionSelector((QueryFunctionSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return convertArithmeticSelector((QueryArithmeticSelector) selector);
		}
		return null;
	}

	/**
	 * Converts a basic column selector into its pixel string
	 * @param selector
	 * @return
	 */
	private static String convertColumnSelector(QueryColumnSelector selector) {
		String qsName = selector.getQueryStructName();
		return qsName;
	}

	/**
	 * Converts a math selector into its pixel string
	 * @param selector
	 * @return
	 */
	private static String convertArithmeticSelector(QueryArithmeticSelector selector) {
		StringBuilder b = new StringBuilder();
		b.append("(").append(convertSelector(selector.getLeftSelector())).append(selector.getMathExpr())
			.append(convertSelector(selector.getRightSelector())).append(")");
		return b.toString();
	}

	/**
	 * Converts a function selector into its pixel string
	 * @param selector
	 * @return
	 */
	private static String convertFunctionSelector(QueryFunctionSelector selector) {
		StringBuilder b = new StringBuilder();
		b.append(selector.getFunction()).append("(");
		
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		int numInner = innerSelectors.size();
		for(int i = 0; i < numInner; i++) {
			if(i != 0) {
				b.append(",");
			}
			b.append(convertSelector(innerSelectors.get(i)));
		}
		b.append(")");
		return b.toString();
	}

	/**
	 * Converts a constant selector into its pixel string
	 * @param selector
	 * @return
	 */
	private static String convertConstantSelector(QueryConstantSelector selector) {
		Object val = selector.getConstant();
		if(val instanceof Number) {
			return val + "";
		} else {
			return "\"" + val + "\"";
		}
	}
	
	/**
	 * Convert a GenRowFilter into its pixel string
	 * Since GenRowFilter is used in many situations, the 
	 * method calling this is responsible for adding a Filter or Having 
	 * around the contents it returns
	 * @param grs
	 * @return
	 */
	public static String convertGenRowFilters(GenRowFilters grs) {
		List<IQueryFilter> origGrf = grs.getFilters();
		if(origGrf != null && !origGrf.isEmpty()) {
			StringBuilder b = new StringBuilder();
			for(int i = 0; i < origGrf.size(); i++) {
				if(i != 0) {
					b.append(",");
				}
				b.append(convertFilter(origGrf.get(i)));
			}
			return b.toString();
		}
		// return null
		return null;
	}

	/**
	 * Convert a filter
	 * Look at left hand side and right hand side
	 * If either is a column, try to convert
	 * @param queryFilter
	 * @param meta
	 * @return
	 */
	public static String convertFilter(IQueryFilter queryFilter) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter);
		} else {
			return null;
		}
	}
	
	/**
	 * Convert and Or Query Filter into its pixel string
	 * @param queryFilter
	 * @return
	 */
	private static String convertOrQueryFilter(OrQueryFilter queryFilter) {
		List<String> orFilters = new Vector<String>();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			orFilters.add(convertFilter(f));
		}
		
		StringBuilder b = new StringBuilder();
		b.append("(");
		for(int i = 0; i < orFilters.size(); i++) {
			if(i != 0) {
				b.append(" OR ");
			}
			b.append(orFilters.get(i));
		}
		b.append(")");
		return b.toString();
	}

	/**
	 * Convert a And Query Filter into its pixel string
	 * @param queryFilter
	 * @return
	 */
	private static String convertAndQueryFilter(AndQueryFilter queryFilter) {
		List<String> andFilters = new Vector<String>();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			andFilters.add(convertFilter(f));
		}
		
		StringBuilder b = new StringBuilder();
		b.append("(");
		for(int i = 0; i < andFilters.size(); i++) {
			if(i != 0) {
				b.append(" AND ");
			}
			b.append(andFilters.get(i));
		}
		b.append(")");
		return b.toString();
	}

	/**
	 * Convert a simple filter into its pixel string
	 * Takes into account when we are doing this and have a parameter
	 * Which must be a single item and starts with "<" and ends with ">"
	 * @param queryFilter
	 * @return
	 */
	private static String convertSimpleQueryFilter(SimpleQueryFilter queryFilter) {
		StringBuilder b = new StringBuilder();
		// add left hand side
		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String col = convertSelector((IQuerySelector) origL.getValue());
			b.append(col);
		} else {
			Object values = origL.getValue();
			appendValuestoStringBuilder(b, values);
		}
		
		// add comparator
		b.append(" ").append(queryFilter.getComparator()).append(" ");
		
		// add right hand side
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String col = convertSelector((IQuerySelector) origR.getValue());
			b.append(col);
		} else {
			Object values = origR.getValue();
			appendValuestoStringBuilder(b, values);
		}

		return b.toString();
	}
	
	/**
	 * Process the values in a simple query filter when it is not a column
	 * @param b
	 * @param values
	 */
	private static void appendValuestoStringBuilder(StringBuilder b, Object values) {
		List<Object> valList = new Vector<Object>();
		if(values instanceof List) {
			valList = (List) values;
		} else {
			valList.add(values);
		}
		
		// if single element
		if(valList.size() == 1) {
			// account for numbers or for a parameter!
			Object val = valList.get(0);
			if(val instanceof Number) {
				b.append(val);
			} else if(val.toString().startsWith("<") && val.toString().endsWith(">")){
				b.append("[").append(values).append("]");
			} else {
				b.append("\"").append(values).append("\"");
			}
		} else {
			// otherwise, it is a list
			b.append("[");
			for(int i = 0; i < valList.size(); i++) {
				if(i != 0) {
					b.append(",");
				}
				if(values instanceof Number) {
					b.append(values);
				} else {
					b.append("\"").append(values).append("\"");
				}
			}
			b.append("]");
		}
	}
	
//	/**
//	 * Convert an order by selector
//	 * Same as conversion of a column selector, but adding the sort direction
//	 * @param selector
//	 * @param meta
//	 * @return
//	 */
//	public static QueryColumnOrderBySelector convertOrderBySelector(QueryColumnOrderBySelector selector) {
//		String newQsName = meta.getUniqueNameFromAlias(selector.getQueryStructName());
//		if(newQsName == null) {
//			// nothing to do
//			// return the original
//			return selector;
//		}
//		QueryColumnOrderBySelector newS = new QueryColumnOrderBySelector();
//		if(newQsName.contains("__")) {
//			String[] split = newQsName.split("__");
//			newS.setTable(split[0]);
//			newS.setColumn(split[1]);
//		} else {
//			newS.setTable(newQsName);
//			newS.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
//		}
//		newS.setSortDir(selector.getSortDirString());
//		newS.setAlias(selector.getAlias());
//		return newS;
//	}
	
	
	
}
