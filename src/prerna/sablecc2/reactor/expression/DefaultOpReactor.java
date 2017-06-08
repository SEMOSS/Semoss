package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class DefaultOpReactor extends OpBasic {
	@Override
	protected NounMetadata evaluate(Object[] values) {
		// TODO Auto-generated method stub
		return new NounMetadata(0, PkslDataTypes.CONST_DECIMAL);
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "double";
	}
}
