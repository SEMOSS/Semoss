package prerna.query.querystruct.transform;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QSParseParamStruct {
	
	private static final Logger logger = LogManager.getLogger(QSParseParamStruct.class);
	
	private QSParseParamStruct() {

	}
	
	/**
	 * This is the main method for this class.  Parses a generic filter
	 * @param filter
	 * @param paramList
	 */
	public static void parseFilter(IQueryFilter filter, List<ParamStruct> paramList) {
		if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			convertSimpleQueryFilter((SimpleQueryFilter) filter, paramList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			parseAndQueryFilter((AndQueryFilter) filter, paramList);
		} else if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			parseOrQueryFilter((OrQueryFilter) filter, paramList);
		}
		
	}

	public static void parseOrQueryFilter(OrQueryFilter filter, List<ParamStruct> paramList) {
		for(IQueryFilter f : filter.getFilterList()) {
			parseFilter(f, paramList);
		}
	}

	public static void parseAndQueryFilter(AndQueryFilter filter, List<ParamStruct> paramList) {
		for(IQueryFilter f : filter.getFilterList()) {
			parseFilter(f, paramList);
		}
	}

	public static void convertSimpleQueryFilter(SimpleQueryFilter filter, List<ParamStruct> paramList) {
		boolean parameterizeLeft = false;
		boolean parameterizeRight = false;
		
		IQuerySelector selector = null;
		NounMetadata origL = filter.getLComparison();
		if(origL.getNounType() == PixelDataType.COLUMN) {
			selector = (IQuerySelector) origL.getValue();
			parameterizeRight = true;
		}
		
		NounMetadata origR = filter.getRComparison();
		if(origR.getNounType() == PixelDataType.COLUMN) {
			selector = (IQuerySelector) origL.getValue();
			parameterizeLeft = true;
		}
		
		if((parameterizeLeft && parameterizeRight)
				|| (!parameterizeLeft && !parameterizeRight)) {
			// cannot have both be columns
			// and need at least one to be a column
			// that is already in a sense a dynamically defined filter
			logger.debug("Must have at one part of the filter be a column");
			return;
		}
		
		if(!(selector instanceof QueryColumnSelector)) {
			logger.debug("Cannot parameterize on derived columns");
			return;
		}
		String comparator = filter.getComparator();
		
		ParamStruct param = new ParamStruct();
		ParamStructDetails paramDetails = new ParamStructDetails();
		paramDetails.setOperator(comparator);
		
		QueryColumnSelector columnSelector = (QueryColumnSelector) selector;
		paramDetails.setTableName(columnSelector.getTable());
		paramDetails.setColumnName(columnSelector.getColumn());
		if(parameterizeLeft) {
			// rhs is the column, lhs is the value
			Object paramValue = origL.getValue();
			paramDetails.setCurrentValue(paramValue);
			paramDetails.setType(origL.getNounType());
		} else if(parameterizeRight) {
			// lhs is the column, rhs is the value
			Object paramValue = origR.getValue();
			paramDetails.setCurrentValue(paramValue);
			paramDetails.setType(origR.getNounType());
		}
		// additional options based on the comparator
		if(IQueryFilter.comparatorIsNumeric(comparator)
				|| IQueryFilter.isRegexComparator(comparator)) {
			// single valued
			param.setMultiple(false);
			param.setSearchable(false);
		} else {
			// multiple values allowed
			param.setMultiple(true);
			param.setSearchable(true);
		}
		param.addParamStructDetails(paramDetails);
		// add to the list of params
		paramList.add(param);
	}
}
