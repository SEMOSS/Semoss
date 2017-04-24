package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpPower extends OpBasicMath {

	public OpPower() {
		this.operation = "power";
	}
	
	@Override
	protected NounMetadata evaluate(Object[] values) {
		double number = ((Number)values[0]).doubleValue();
		double power = ((Number)values[1]).doubleValue();
		double powerVal = Math.pow(number, power);
		NounMetadata powerNoun = new NounMetadata(powerVal, PkslDataTypes.CONST_DECIMAL);
		return powerNoun;
	}
}
