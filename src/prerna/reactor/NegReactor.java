package prerna.reactor;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NegReactor extends AbstractReactor implements JavaExecutable {

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
		if(noun.getNounType() == PixelDataType.CONST_INT) {
			noun = new NounMetadata(-1 * ((Number) noun.getValue()).intValue(), PixelDataType.CONST_INT);
		} else if(noun.getNounType() == PixelDataType.CONST_DECIMAL) {
			noun = new NounMetadata(-1.0 * ((Number) noun.getValue()).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if(noun.getNounType() == PixelDataType.COLUMN) {
			NegEvaluator neg = new NegEvaluator();
			neg.setPixelPlanner(this.planner);
			neg.In();
			neg.getCurRow().add(noun);
			noun = new NounMetadata(neg, PixelDataType.LAMBDA);
		}

		// ugh.. you messed up at this point
		else {
			throw new IllegalArgumentException("Cannot take a negative of the value : " + noun.getValue());
		}

		return noun;
	}

	@Override
	public String getJavaSignature() {
		NounMetadata noun = getInputs().get(0);
		Object obj = noun.getValue();
		if(obj instanceof JavaExecutable) {
			return "-("+((JavaExecutable)obj).getJavaSignature()+")";
		} else {
			return "-"+obj.toString();
		}
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		List<NounMetadata> noun = new ArrayList<>(1);
		noun.add(this.curRow.getNoun(0));
		return noun;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "double";
	}

}
