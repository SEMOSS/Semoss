package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NegEvaluator extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		return evalAdditiveInverseNoun(noun);
	}
	
	/**
	 * Return the additive inverse of a number
	 * @param noun
	 * @return
	 */
	private NounMetadata evalAdditiveInverseNoun(NounMetadata noun) {
		if(noun.getNounType() == PixelDataType.CONST_INT) {
			noun = new NounMetadata(-1 * ((Number) noun.getValue()).intValue(), PixelDataType.CONST_INT);
		} else if(noun.getNounType() == PixelDataType.CONST_DECIMAL) {
			noun = new NounMetadata(-1.0 * ((Number) noun.getValue()).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if(noun.getNounType() == PixelDataType.COLUMN) {
			String varName = noun.getValue().toString().trim();
			noun = planner.getVariableValue(varName);
			noun = evalAdditiveInverseNoun(noun);
		}
		// ugh.. you messed up at this point
		else {
			throw new IllegalArgumentException("Cannot take a negative of the value : " + noun.getValue());
		}

		return noun;
	}
	

}
