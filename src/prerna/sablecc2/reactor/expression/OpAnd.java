package prerna.sablecc2.reactor.expression;

import java.util.List;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.PKSLPlanner;

public class OpAnd extends OpBasic {

	boolean result = false;

	@Override
	protected NounMetadata evaluate(Object[] values) {

		PKSLPlanner planner = this.planner;

		for (Object obj : values) {
			result = ((Filter) obj).evaluate(planner);
			if (result == false) {
				break;
			}
		}

		NounMetadata andResult = new NounMetadata(result, PkslDataTypes.BOOLEAN);
		System.out.println("andResult...." + andResult);
		return andResult;
	}
}
