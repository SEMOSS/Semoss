package prerna.sablecc2.reactor.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.IReactor;

public abstract class OpReactor extends AbstractReactor {

	/*
	 * The specific operation would only need to override
	 * what the execute function does
	 */
	
	/**
	 * Merge the curRow from the expr reactor
	 * which generated this operation
	 * @param grs
	 */
	public void mergeCurRow(GenRowStruct grs) {
		this.curRow.merge(grs);
	}
	
	/**
	 * Flush out the gen row into the values
	 * Takes into consideration the lambdas that still need to be executed
	 * @return
	 */
	public NounMetadata[] getValues() {
		int numVals = curRow.size();
		NounMetadata[] retValues = new NounMetadata[numVals];
		
		for(int cIndex = 0; cIndex < numVals; cIndex++) {
			NounMetadata curNoun = curRow.getNoun(cIndex);
			retValues[cIndex] = executeNoun(curNoun);
		}
		
		return retValues;
	}
	
	/**
	 * 
	 * @param noun
	 * @return
	 * 
	 * The method will take the noun, evaluate the noun if necessary or grab the variable value
	 * If the noun is a list, it will recursively execute each noun in that list
	 */
	private NounMetadata executeNoun(NounMetadata noun) {
		PkslDataTypes nounType = noun.getNounName();
		NounMetadata evaluatedNoun;
		if(nounType == PkslDataTypes.LAMBDA) {
			evaluatedNoun = ((IReactor) noun.getValue()).execute();
		} else if(nounType == PkslDataTypes.COLUMN) {
			// column might be a variable that is already stored
			// if it is, do a replacement with the assignment noun
			NounMetadata assignmentNoun = this.planner.getVariableValue((String)noun.getValue());
			if(assignmentNoun != null) {
				evaluatedNoun = assignmentNoun;
			} else {
				evaluatedNoun = noun;
			}
		} else if(nounType == PkslDataTypes.VECTOR) {
			
			//For each noun in the list we also want to execute
			List<NounMetadata> nounVector = (List<NounMetadata>)noun.getValue();
			List<NounMetadata> evaluatedNounVector = new ArrayList<>(nounVector.size());
			for(NounMetadata nextNoun : nounVector) {
				evaluatedNounVector.add(executeNoun(nextNoun));
			}
			evaluatedNoun = new NounMetadata(evaluatedNounVector, PkslDataTypes.VECTOR);
		} else {
			evaluatedNoun = noun;
		}
		
		return evaluatedNoun;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		//Default operation for the abstract is to return the asName aliases as the outputs
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs == null || outputs.size() == 0) {
			outputs = new Vector<NounMetadata>();
			NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.CONST_DECIMAL);
			outputs.add(output);
		}
		return outputs;
	}
}
