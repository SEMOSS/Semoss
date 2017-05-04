package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.Filter2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;

public class OpFilter extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		if(this.parentReactor instanceof QueryFilterReactor) {
			// we want to return a filter object
			// so it can be integrated with the query struct
			Filter2 filter = new Filter2(this.nouns[0], values[1].toString().trim(), this.nouns[2]);
			return new NounMetadata(filter, PkslDataTypes.FILTER);
		}
		
		// there are 3 things being passed
		// index 0 is left term
		// index 1 is comparator
		// index 2 is right term
		Object left = values[0];
		Object right = values[2];
		String comparator = values[1].toString().trim();
		boolean evaluation = false;
		if(comparator.equals("==")) {
			evaluation = left == right;
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			evaluation = left != right;
		} 
		// we have some numerical stuff
		// everything needs to be a valid number
		else if(comparator.equals(">=")) {
			evaluation = ((Number)left).doubleValue() >= ((Number)right).doubleValue();
		} else if(comparator.equals(">")) {
			evaluation = ((Number)left).doubleValue() > ((Number)right).doubleValue();
		} else if(comparator.equals("<=")) {
			evaluation = ((Number)left).doubleValue() <= ((Number)right).doubleValue();
		} else if(comparator.equals("<")) {
			evaluation = ((Number)left).doubleValue() < ((Number)right).doubleValue();
		} else {
			throw new IllegalArgumentException("Cannot handle comparator " + comparator);
		}
		
		return new NounMetadata(evaluation, PkslDataTypes.BOOLEAN);
	}
}
