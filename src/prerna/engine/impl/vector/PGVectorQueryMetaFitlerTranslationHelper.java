package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class PGVectorQueryMetaFitlerTranslationHelper {

	public static List<IQueryFilter> convertFilters(List<IQueryFilter> origFilters, String tableName) {
		if(origFilters != null && !origFilters.isEmpty()) {
			List<IQueryFilter> convertedFilters = new ArrayList<IQueryFilter>();
			for(int i = 0; i < origFilters.size(); i++) {
				convertedFilters.add(convertFilter(origFilters.get(i), tableName));
			}
			return convertedFilters;
		}
		// return the empty filters
		return origFilters;
	}

	/**
	 * Convert a filter
	 * Look at left hand side and right hand side
	 * If either is a column, try to convert
	 * @param queryFilter
	 * @param meta
	 * @return
	 */
	public static IQueryFilter convertFilter(IQueryFilter queryFilter, String tableName) {
		if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, tableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) queryFilter, tableName);
		} else if(queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) queryFilter, tableName);
		} else {
			return null;
		}
	}
	
	private static IQueryFilter convertOrQueryFilter(OrQueryFilter queryFilter, String tableName) {
		OrQueryFilter newF = new OrQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, tableName));
		}
		return newF;
	}

	private static IQueryFilter convertAndQueryFilter(AndQueryFilter queryFilter, String tableName) {
		AndQueryFilter newF = new AndQueryFilter();
		List<IQueryFilter> andFilterList = queryFilter.getFilterList();
		for(IQueryFilter f : andFilterList) {
			newF.addFilter(convertFilter(f, tableName));
		}
		return newF;
	}

	private static IQueryFilter convertSimpleQueryFilter(SimpleQueryFilter queryFilter, String tableName) {
		/*
		 * We need to convert a simple query filter into an AND filter
		 * Where we filter on the attribute name and the associated value
		 */
		
		AndQueryFilter newAndFilter = new AndQueryFilter();
		
		NounMetadata origL = queryFilter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			// this is to filter to the attribute
			SimpleQueryFilter attributeFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.ATTRIBUTE), PixelDataType.COLUMN),
					"==",
					new NounMetadata( ((QueryColumnSelector) origL.getValue()).getTable(), PixelDataType.CONST_STRING)
					);
			newAndFilter.addFilter(attributeFilter);
		} 
		// Not going to handle a subquery against the pgvector at this point..
//		else if(origL.getNounType() == PixelDataType.QUERY_STRUCT) {
//			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origL.getValue(), tableName);
//			newL = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
//		} 
		else {
			// this is to filter to the value
			SimpleQueryFilter valueFilter = constructValueFilter(origL, tableName, queryFilter.getComparator());
			newAndFilter.addFilter(valueFilter);
		}
		
		NounMetadata origR = queryFilter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			// this is to filter to the attribute
			SimpleQueryFilter attributeFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.ATTRIBUTE), PixelDataType.COLUMN),
					"==",
					new NounMetadata( ((QueryColumnSelector) origR.getValue()).getTable(), PixelDataType.CONST_STRING)
					);
			newAndFilter.addFilter(attributeFilter);
		} 
		// Not going to handle a subquery against the pgvector at this point..
//		else if(origR.getNounType() == PixelDataType.QUERY_STRUCT) {
//			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origR.getValue(), tableName);
//			newR = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
//		} 
		else {
			SimpleQueryFilter valueFilter = constructValueFilter(origR, tableName, queryFilter.getComparator());
			newAndFilter.addFilter(valueFilter);
		}

		return newAndFilter;
	}
	
	private static SimpleQueryFilter constructValueFilter(NounMetadata existingValue, String tableName, String comparator) {
		Object filterVal = existingValue.getValue();
		PixelDataType dataType = existingValue.getNounType();
		if(dataType == PixelDataType.CONST_STRING) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.STR_VALUE), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.CONST_STRING)
					);
			return valueFilter;
		} else if(dataType == PixelDataType.CONST_INT) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.INT_VALUE), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.CONST_INT)
					);
			return valueFilter;
		} else if(dataType == PixelDataType.CONST_DECIMAL) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.NUM_VALUE), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.CONST_DECIMAL)
					);
			return valueFilter;
		} else if(dataType == PixelDataType.BOOLEAN) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.BOOL_VALUE), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.BOOLEAN)
					);
			return valueFilter;
		} else if(dataType == PixelDataType.CONST_DATE) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.DATE_VAL), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.CONST_DATE)
					);
			return valueFilter;
		} else if(dataType == PixelDataType.CONST_TIMESTAMP) {
			SimpleQueryFilter valueFilter = new SimpleQueryFilter(
					new NounMetadata( new QueryColumnSelector(tableName+"__"+VectorDatabaseMetadataCSVTable.TIMESTAMP_VAL), PixelDataType.COLUMN),
					comparator,
					new NounMetadata( filterVal, PixelDataType.CONST_TIMESTAMP)
					);
			return valueFilter;
		}
		
		return null;
	}
	
}
