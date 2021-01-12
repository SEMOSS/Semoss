package prerna.query.querystruct.transform;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.LEVEL;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QsFilterParameterizeConverter2 {
	
	private static final Logger logger = LogManager.getLogger(QsFilterParameterizeConverter2.class);
	
	private QsFilterParameterizeConverter2() {

	}
	
	public static IQueryFilter modifyFilter(IQueryFilter filter, ParamStructDetails paramDetails, ParamStruct paramStruct) {
		if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return convertSimpleQueryFilter((SimpleQueryFilter) filter, paramDetails, paramStruct);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return convertAndQueryFilter((AndQueryFilter) filter, paramDetails, paramStruct);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return convertOrQueryFilter((OrQueryFilter) filter, paramDetails, paramStruct);
		}
		
		return null;
	}

	public static IQueryFilter convertOrQueryFilter(OrQueryFilter filter, ParamStructDetails paramDetails, ParamStruct paramStruct) {
		OrQueryFilter newFilter = new OrQueryFilter();
		for(IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, paramDetails, paramStruct);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertAndQueryFilter(AndQueryFilter filter, ParamStructDetails paramDetails, ParamStruct paramStruct) {
		AndQueryFilter newFilter = new AndQueryFilter();
		for(IQueryFilter f : filter.getFilterList()) {
			IQueryFilter newF = modifyFilter(f, paramDetails, paramStruct);
			newFilter.addFilter(newF);
		}
		return newFilter;
	}

	public static IQueryFilter convertSimpleQueryFilter(SimpleQueryFilter filter, ParamStructDetails paramDetails, ParamStruct paramStruct) {
		NounMetadata newL = null;
		NounMetadata newR = null;

		LEVEL paramLevel = paramDetails.getLevel();
		
		String comparator = filter.getComparator();
		boolean parameterizeLeft = false;
		boolean parameterizeRight = false;
		
		NounMetadata origL = filter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			String qsName = selector.getQueryStructName();
			String table = null;
			String column = null;
			if(qsName.contains("__")) {
				String[] split = qsName.split("__");
				table = split[0];
				column = split[1];
			} else {
				column = qsName;
			}
			if(paramLevel == LEVEL.COLUMN) {
				if(paramDetails.getColumnName().equals(column)) {
					parameterizeRight = true;
				}
			} else if(paramLevel == LEVEL.TABLE) {
				if(paramDetails.getColumnName().equals(column) && paramDetails.getTableName().equals(table) ) {
					parameterizeRight = true;
				}
			} else if(paramLevel == LEVEL.OPERATOR || paramLevel == LEVEL.OPERATORU) {
				if(paramDetails.getColumnName().equals(column) && paramDetails.getTableName().equals(table)
						&& paramDetails.getOperator().equals(comparator)) {
					parameterizeRight = true;
				}
			} else if(paramLevel == LEVEL.OPERATORU) {
				// this doesn't exist yet...
				
			}
		}
		NounMetadata origR = filter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			String qsName = selector.getQueryStructName();
			String table = null;
			String column = null;
			if(qsName.contains("__")) {
				String[] split = qsName.split("__");
				table = split[0];
				column = split[1];
			} else {
				column = qsName;
			}
			if(paramLevel == LEVEL.COLUMN) {
				if(paramDetails.getColumnName().equals(column)) {
					parameterizeLeft = true;
				}
			} else if(paramLevel == LEVEL.TABLE) {
				if(paramDetails.getColumnName().equals(column) && paramDetails.getTableName().equals(table) ) {
					parameterizeLeft = true;
				}
			} else if(paramLevel == LEVEL.OPERATOR || paramLevel == LEVEL.OPERATORU) {
				if(paramDetails.getColumnName().equals(column) && paramDetails.getTableName().equals(table)
						&& paramDetails.getOperator().equals(comparator)) {
					parameterizeLeft = true;
				}
			} else if(paramLevel == LEVEL.OPERATORU) {
				// this doesn't exist yet...
				
			}
		}
		
		if(parameterizeLeft) {
			// keep the same right
			newR = origR;
			// create the new left hand side
			newL = new NounMetadata("<" + paramStruct.getParamName() + ">", PixelDataType.CONST_STRING);

		} else if(parameterizeRight) {
			// keep the same left
			newL = origL;
			// create the new right hand side
			newR = new NounMetadata("<" + paramStruct.getParamName() + ">", PixelDataType.CONST_STRING);
			
		} else {
			// return the original
			return filter;
		}

		SimpleQueryFilter newF = new SimpleQueryFilter(newL, filter.getComparator(), newR);
		return newF;
	}
	
	///////////////////////////////////////////////////////////
	
	
	public static void findSelectorsForAlias(IQueryFilter filter, String colToParameterize, List<String> qsList) {
		if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			searchSimpleQueryFilter((SimpleQueryFilter) filter, colToParameterize, qsList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			searchAndQueryFilter((AndQueryFilter) filter, colToParameterize, qsList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			searchOrQueryFilter((OrQueryFilter) filter, colToParameterize, qsList);
		}
	}

	private static void searchOrQueryFilter(OrQueryFilter filter, String colToParameterize, List<String> qsList) {
		for(IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchAndQueryFilter(AndQueryFilter filter, String colToParameterize, List<String> qsList) {
		for(IQueryFilter f : filter.getFilterList()) {
			findSelectorsForAlias(f, colToParameterize, qsList);
		}
	}

	private static void searchSimpleQueryFilter(SimpleQueryFilter filter, String colToParameterize, List<String> qsList) {
		NounMetadata origL = filter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
		NounMetadata origR = filter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			IQuerySelector selector = (IQuerySelector) origL.getValue();
			if(selector.getAlias().equals(colToParameterize)) {
				qsList.add(selector.getQueryStructName());
			}
		}
	}
	
}
