package prerna.query.querystruct.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QsToPixelConverter {
	
	private QsToPixelConverter() {

	}
	
	public static String getHardQsPixel(HardQueryStruct qs) {
		StringBuilder b = new StringBuilder();
		b.append("Database(").append(qs.getEngineName()).append(") | Query(")
			.append(qs.getQuery().replace("\"", "\\\"")).append(")");
		return b.toString();
	}

	public static String getPixel(QueryStruct2 qs) {
		if(qs instanceof HardQueryStruct) {
			return getHardQsPixel((HardQueryStruct) qs);
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

		
//		// now go through the group by
//		List<QueryColumnSelector> origGroups = qs.getGroupBy();
//		if(origGroups != null && !origGroups.isEmpty()) {
//			List<QueryColumnSelector> convertedGroups =  new Vector<QueryColumnSelector>();
//			for(int i = 0; i < origGroups.size(); i++) {
//				IQuerySelector origGrupS = origGroups.get(i);
//				QueryColumnSelector convertedGroupS = (QueryColumnSelector) convertSelector(origGrupS);
//				convertedGroups.add(convertedGroupS);
//			}
//			convertedQs.setGroupBy(convertedGroups);
//		}
//		
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

	public static String convertJoins(Map<String, Map<String, List>> joins) {
		StringBuilder b = new StringBuilder();
		// iterate through and construct the joins
		for(String startCol : joins.keySet()) {
			Map<String, List> compMap = joins.get(startCol);
			
			Map<String, List> convertedCompHash = new HashMap<String, List>();
			for(String comparator : compMap.keySet()) {
				List<String> endColList = compMap.get(comparator);
				
				for(String endCol : endColList) {
					b.append("(").append(startCol).append(" ").append(comparator)
						.append(" ").append(endCol).append(")");
				}
			}
		}
		
		return b.toString();
	}

	/**
	 * Modify the selectors
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

	private static String convertColumnSelector(QueryColumnSelector selector) {
		String qsName = selector.getQueryStructName();
		return qsName;
	}

	private static String convertArithmeticSelector(QueryArithmeticSelector selector) {
		StringBuilder b = new StringBuilder();
		b.append("(").append(convertSelector(selector.getLeftSelector())).append(selector.getMathExpr())
			.append(convertSelector(selector.getRightSelector())).append(")");
		return b.toString();
	}

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

	private static String convertConstantSelector(QueryConstantSelector selector) {
		Object val = selector.getConstant();
		if(val instanceof Number) {
			return val + "";
		} else {
			return "\"" + val + "\"";
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

	private static String convertSimpleQueryFilter(SimpleQueryFilter queryFilter) {
		StringBuilder b = new StringBuilder();
		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String col = convertSelector((IQuerySelector) origL.getValue());
			b.append(col);
		} else {
			List<Object> valList = new Vector<Object>();
			Object values = origL.getValue();
			if(values instanceof List) {
				valList = (List) values;
			} else {
				valList.add(values);
			}
			
			if(valList.size() == 1) {
				Object val = valList.get(0);
				if(val instanceof Number) {
					b.append(val);
				} else if(val.toString().startsWith("<") && val.toString().endsWith(">")){
					b.append("[").append(values).append("]");
				} else {
					b.append("\"").append(values).append("\"");
				}
			} else {
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
		b.append(" ").append(queryFilter.getComparator()).append(" ");
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// need to convert
			String col = convertSelector((IQuerySelector) origR.getValue());
			b.append(col);
		} else {
			List<Object> valList = new Vector<Object>();
			Object values = origR.getValue();
			if(values instanceof List) {
				valList = (List) values;
			} else {
				valList.add(values);
			}
			
			if(valList.size() == 1) {
				Object val = valList.get(0);
				if(val instanceof Number) {
					b.append(val);
				} else if(val.toString().startsWith("<") && val.toString().endsWith(">")){
					b.append("[").append(values).append("]");
				} else {
					b.append("\"").append(values).append("\"");
				}
			} else {
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

		return b.toString();
	}
	
	private static String getDatasource(QueryStruct2 qs) {
		StringBuilder b = new StringBuilder();
		if(qs instanceof CsvQueryStruct) {
			b.append("FileRead(");
			CsvQueryStruct csvQs = (CsvQueryStruct) qs;
			b.append(ReactorKeysEnum.FILE_PATH.getKey()).append("=[\"").append(csvQs.getCsvFilePath()).append("\"]");
			b.append(",");
			b.append(ReactorKeysEnum.DATA_TYPE_MAP.getKey()).append("=[").append(convertMap(csvQs.getColumnTypes())).append("]");
			b.append(",");
			b.append(ReactorKeysEnum.DELIMITER.getKey()).append("=[\"").append(csvQs.getDelimiter()).append("\"]");
			b.append(",");
			b.append(ReactorKeysEnum.NEW_HEADER_NAMES.getKey()).append("=[").append(convertMap(csvQs.getNewHeaderNames())).append("]");
			b.append(")");
		} else if(qs instanceof ExcelQueryStruct) {
			b.append("FileRead(");
			ExcelQueryStruct xlQs = (ExcelQueryStruct) qs;
			b.append(ReactorKeysEnum.FILE_PATH.getKey()).append("=[\"").append(xlQs.getExcelFilePath()).append("\"]");
			b.append(",");
			b.append(ReactorKeysEnum.DATA_TYPE_MAP.getKey()).append("=[").append(convertMap(xlQs.getColumnTypes())).append("]");
			b.append(",");
			b.append("sheetName=[\"").append(xlQs.getSheetName()).append("\"]");
			b.append(",");
			b.append(ReactorKeysEnum.NEW_HEADER_NAMES.getKey()).append("=[").append(convertMap(xlQs.getNewHeaderNames())).append("]");
			b.append(")");
		} else {
			String engineName = qs.getEngineName();
			if(engineName != null) {
				b.append("Database(").append(engineName).append(")");
			} else {
				b.append("Frame()");
			}
		}
		return b.toString();
	}
	
	private static String convertMap(Map<String, String> map) {
		StringBuilder b = new StringBuilder("{");
		
		int counter = 0;
		Set<String> keys = map.keySet();
		for(String key : keys) {
			if(counter != 0) {
				b.append(",");
			}
			b.append("\"").append(key).append("\":\"").append(map.get(key)).append("\"");
			counter++;
		}
		b.append("}");
		return b.toString();
	}

}
