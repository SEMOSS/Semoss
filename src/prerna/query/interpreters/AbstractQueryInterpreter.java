package prerna.query.interpreters;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.Filter2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter2 {

	protected int performCount;
	protected QueryStruct2 qs;

	protected Map<String, Map<String, List<Object>>> colToValFilters;
	protected Map<String, Map<String, List<String>>> colToColFilters;

	protected enum FILTER_TYPE {COL_TO_COL, COL_TO_VALUES, VALUES_TO_COL, VALUE_TO_VALUE};

	@Override
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
		this.performCount = qs.getPerformCount();
	}

	@Override
	public void setPerformCount(int performCount) {
		this.performCount = performCount;
	}

	@Override
	public int isPerformCount() {
		return this.performCount;
	}

	protected FILTER_TYPE determineFilterType(Filter2 filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();

		// DIFFERENT PROCESSING BASED ON THE TYPE OF VALUE
		PkslDataTypes lCompType = leftComp.getNounName();
		PkslDataTypes rCompType = rightComp.getNounName();

		if(lCompType == PkslDataTypes.COLUMN && rCompType == PkslDataTypes.COLUMN) {
			return FILTER_TYPE.COL_TO_COL;
		} else if(lCompType == PkslDataTypes.COLUMN && 
				(rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_INT || rCompType == PkslDataTypes.CONST_STRING)
				) {
			return FILTER_TYPE.COL_TO_VALUES;
		} else if((lCompType == PkslDataTypes.CONST_DECIMAL || lCompType == PkslDataTypes.CONST_INT || lCompType == PkslDataTypes.CONST_STRING) 
				&& rCompType == PkslDataTypes.COLUMN) {
			return FILTER_TYPE.VALUES_TO_COL;
		} else if((rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_INT || rCompType == PkslDataTypes.CONST_STRING) &&
				(lCompType == PkslDataTypes.CONST_DECIMAL || lCompType == PkslDataTypes.CONST_INT || lCompType == PkslDataTypes.CONST_STRING)
				) {
			// WHY ARE YOU DOING THIS? 
			// COMPARING A NUMBER TO A NUMBER
			// THIS IS DUMB
			return FILTER_TYPE.VALUE_TO_VALUE;
		}

		return null;
	}
}
