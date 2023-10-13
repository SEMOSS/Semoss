package prerna.reactor.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.JavaExecutable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class OpReactor extends AbstractReactor implements JavaExecutable {

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
		PixelDataType nounType = noun.getNounType();
		NounMetadata evaluatedNoun;
		if(nounType == PixelDataType.LAMBDA) {
			evaluatedNoun = ((IReactor) noun.getValue()).execute();
		} else if(nounType == PixelDataType.COLUMN) {
			// column might be a variable that is already stored
			// if it is, do a replacement with the assignment noun
			Object value = noun.getValue();
			if(value instanceof String) {
				NounMetadata assignmentNoun = this.planner.getVariableValue((String) value);
				if(assignmentNoun != null) {
					evaluatedNoun = assignmentNoun;
				} else {
					evaluatedNoun = noun;
				}
			} 
			// this is some kind of selector for a query filter 
			// since op filter is used
			else {
				evaluatedNoun = noun;
			}
		} else if(nounType == PixelDataType.VECTOR) {
			//For each noun in the list we also want to execute
			List<NounMetadata> nounVector = (List<NounMetadata>)noun.getValue();
			List<NounMetadata> evaluatedNounVector = new ArrayList<>(nounVector.size());
			for(NounMetadata nextNoun : nounVector) {
				evaluatedNounVector.add(executeNoun(nextNoun));
			}
			evaluatedNoun = new NounMetadata(evaluatedNounVector, PixelDataType.VECTOR);
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
			NounMetadata output = new NounMetadata(this.signature, PixelDataType.CONST_DECIMAL);
			outputs.add(output);
		}
		return outputs;
	}
	
	@Override
	public void mergeUp() {
		super.mergeUp();
	}
	
	/**
	 * We want the result of this to return a string in the form of:
	 * 	prerna.sablecc2.OpSum.eval(1, 2, prerna.sablecc2.OpMin(x, y))
	 * 	if a reactor does not conform to this standard that reactor overrides the signature for itself, e.g. opFilter
	 */
	public String getJavaSignature() {
		//class name plus eval
		StringBuilder javaSignature = new StringBuilder(this.getClass().getName()+".eval(");
		
		//we only want the java inputs, i.e. the direct inputs for THIS reactor, not the inputs for any child reactor
		List<NounMetadata> inputs = this.getJavaInputs();
		for(int i = 0; i < inputs.size(); i++) {
			if(i > 0) {
				javaSignature.append(", ");
			}
			
			String nextArgument;
			NounMetadata nextNoun = inputs.get(i);
			Object nextInput = inputs.get(i).getValue();
			
			//if java executable add that as an argument
			if(nextInput instanceof JavaExecutable) {
				nextArgument = ((JavaExecutable)nextInput).getJavaSignature();
			} else {
				
				//if string add quotes
				if(nextNoun.getNounType() == PixelDataType.CONST_STRING) {
					nextArgument = "\""+nextInput.toString() +"\"";
				} else {
					nextArgument = nextInput.toString();
				}
			}
			javaSignature.append(nextArgument);
		}
		javaSignature.append(")");
		
		return javaSignature.toString();
	}
	
	public List<NounMetadata> getJavaInputs() {
		List<NounMetadata> inputs = new Vector<NounMetadata>();
		// grab all the nouns in the noun store
		Set<String> nounKeys = this.getNounStore().nounRow.keySet();
		for(String nounKey : nounKeys) {
			// grab the genrowstruct for the noun
			// and add its vector to the inputs list
			GenRowStruct struct = this.getNounStore().getNoun(nounKey);
			inputs.addAll(struct.vector);
		}

		return inputs;
	}
}
