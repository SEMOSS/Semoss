package prerna.sablecc2.reactor;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class NegReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return this.parentReactor;
	}

	@Override
	public NounMetadata execute() {
		NounMetadata inverseNoun = null;
		// grab the noun to inverse
		// 1) check if it is a result
		// 2) check if it in the curRow 
		NounMetadata termNounResult = this.planner.getVariable("$RESULT");
		if(termNounResult != null) {
			inverseNoun = createAdditiveInverseNoun(termNounResult);
		} else {
			termNounResult = curRow.getNoun(0);
			inverseNoun = createAdditiveInverseNoun(termNounResult);
		}
		
		// return the noun
		return inverseNoun;
	}

	/**
	 * Return the additive inverse of a number
	 * @param noun
	 * @return
	 */
	private NounMetadata createAdditiveInverseNoun(NounMetadata noun) {
		if(noun.getNounName() == PkslDataTypes.CONST_INT) {
			noun = new NounMetadata(-1 * ((Number) noun.getValue()).intValue(), PkslDataTypes.CONST_INT);
		} else if(noun.getNounName() == PkslDataTypes.CONST_DECIMAL) {
			noun = new NounMetadata(-1.0 * ((Number) noun.getValue()).doubleValue(), PkslDataTypes.CONST_DECIMAL);
		} else if(noun.getNounName() == PkslDataTypes.COLUMN) {
			NegEvaluator neg = new NegEvaluator();
			neg.setPKSLPlanner(this.planner);
			neg.In();
			neg.getCurRow().add(noun);
			noun = new NounMetadata(neg, PkslDataTypes.LAMBDA);
		}

		// ugh.. you messed up at this point
		else {
			throw new IllegalArgumentException("Cannot take a negative of the value : " + noun.getValue());
		}

		return noun;
	}

}
