package prerna.sablecc2.reactor.expression;

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
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

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
			PkslDataTypes curType = curNoun.getNounName();
			if(curType == PkslDataTypes.LAMBDA) {
				NounMetadata nounOutput = ((IReactor) curNoun.getValue()).execute();
				retValues[cIndex] = nounOutput;
			} else if(curType == PkslDataTypes.COLUMN) {
				// column might be a variable that is already stored
				// if it is, do a replacement with the assignment noun
				NounMetadata assignmentNoun = this.planner.getVariableValue((String)curNoun.getValue());
				if(assignmentNoun != null) {
					retValues[cIndex] = assignmentNoun;
				} else {
					retValues[cIndex] = curNoun;
				}
			} else {
				// if not a lambda or a column
				// just return it
				retValues[cIndex] = curNoun;
			}
		}
		
		return retValues;
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
