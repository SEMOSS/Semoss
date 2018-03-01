package prerna.query.interpreters;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JsonInterpreter implements IQueryInterpreter2{

	
	QueryStruct2 qs = null;
	StringBuffer selectors = null;
	StringBuffer filters = null;
	// for fda this is a +
	public String separator = "&";
	IEngine engine = null;
	
	// equalizer
	// for FDA this is a :
	public String equal = "=";
	

	public JsonInterpreter(IEngine engine)
	{
		this.engine = engine;
	}
	
	
	@Override
	public String composeQuery() {
		addSelectors();
		addFilters();
		
		String retString = selectors.toString();
		
		if(filters != null)
			retString = selectors.toString() + "@@@" + filters.toString();
		
		return retString;
	}

	@Override
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
		
	}

	@Override
	public void setDistinct(boolean isDistinct) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isDistinct() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setLogger(Logger logger) {
		// TODO Auto-generated method stub
		
	}
	
	 /* Loops through the selectors defined in the QS to add them to the selector string
	 * and considers if the table should be added to the from string
	 */
	public void addSelectors() {
		List<IQuerySelector> selectorData = qs.getSelectors();
		for(IQuerySelector selector : selectorData) {
			addSelector(selector);
		}
	}
	
	private void addSelector(IQuerySelector selector) {
		String alias = selector.getAlias();
		
		String pathPattern = ((QueryColumnSelector)selector).getColumn();
		
		// this is basically the same thing
		// the path pattern sits on the prop file
		if(alias.equalsIgnoreCase(pathPattern))
		{
			if(selectors == null)
				selectors = new StringBuffer(pathPattern);
			else
				selectors = selectors.append(";").append(pathPattern);
		}

		//alias=pathPattern
		
		else
		{
			String totalString = alias + "=" + pathPattern;
			if(selectors == null)
				selectors = new StringBuffer(totalString);
			else
				selectors = selectors.append(";").append(totalString);			
		}		
	}
	
	public void addFilters() {
		List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if(filterSyntax != null)
				this.filters.append(filterSyntax);
			}
	}
	
	private StringBuilder processFilter(IQueryFilter filter) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) 
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		return null;
	}
 

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		String leftVal = leftComp.getValue().toString();
		
		String rightVal = rightComp.getValue().toString();
		
		// need someway to encode if this is an array
		if(rightComp.getNounType() == prerna.sablecc2.om.PixelDataType.VECTOR)
		{
			StringBuffer rightValMaker = new StringBuffer("ARRAY");
			List list = (List) rightComp.getValue();
			for(int valIndex = 0;valIndex < list.size();valIndex++)
			{
				if(valIndex != 0)
					rightValMaker.append("<>");
				rightValMaker.append(list.get(valIndex));
			}
			rightValMaker.append("ARRAY");
			
			rightVal = rightValMaker.toString();
		}
		return new StringBuilder(leftVal + "=" + rightVal);
	}
}
