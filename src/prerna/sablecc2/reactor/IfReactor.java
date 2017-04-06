package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.storage.StoreValue;

public class IfReactor extends AbstractReactor {

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	// execute it
	// once again this would be abstract
	public NounMetadata execute()
	{
		Object ifEvaluatorObject = this.curRow.get(0);
		PkslDataTypes ifEvaluatorType = this.curRow.getMeta(0);
		
		// the input can be any reactor or a filter within the if statment
		// grab it and evalute based on its type
		boolean caseEvaluation = false;
		
		if(ifEvaluatorType == PkslDataTypes.FILTER) {
			// we have a filter object
			// use its evaluate method
			Filter filter = (Filter) ifEvaluatorObject;
			caseEvaluation = filter.evaluate(this.planner);
		} else if(ifEvaluatorType == PkslDataTypes.LAMBDA) {
			// we have a full reactor
			// required that this returns a boolean
			AbstractReactor ifEvaluatorReactor = null;
			try {
				ifEvaluatorReactor = (AbstractReactor) ifEvaluatorObject;
				caseEvaluation = (boolean) ifEvaluatorReactor.execute().getValue();
			} catch(ClassCastException e) {
				if(ifEvaluatorReactor != null) {
					throw new IllegalArgumentException("If statement condition (" + ifEvaluatorReactor.getPKSL()[1] + ") could not be evaluated");
				} else {
					throw new IllegalArgumentException("If statement condition could not be evaluated");
				}
			}
		}
		// the if will always have 2 values in its curRow
		// the first value is the true object
		// the second value is the false object
		// based on the case evaluation, we will know which one to evaluate
		if(caseEvaluation == true) {
			Object trueObj = this.curRow.get(1);
			PkslDataTypes trueObjMeta = this.curRow.getMeta(1);
			// evaluate the true statement
			return evaluateStatement(trueObj, trueObjMeta);
		} else {
			Object falseObj = this.curRow.get(2);
			PkslDataTypes falseObjMeta = this.curRow.getMeta(2);
			// evaluate the false statement
			return evaluateStatement(falseObj, falseObjMeta);
		}
	}
	
	private NounMetadata evaluateStatement(Object statementObj, PkslDataTypes statementType) {
		// if it is another reactor
		// let the reactor execute and handle the returning of its data
		if(statementObj instanceof AbstractReactor) {
			AbstractReactor trueReactor = (AbstractReactor) statementObj;
			trueReactor.evaluate = true;
			return trueReactor.execute();
		} else {
			// ughh...
			// must be a constant value ?
			// unsure what else would ever end up here
			return getNounDataForConstant(statementObj, statementType);
		}
	}
	
	private NounMetadata getNounDataForConstant(Object obj, PkslDataTypes pkslDataTypes) {
		NounMetadata data = new NounMetadata(obj, pkslDataTypes);
		return data;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.LAMBDA);
		outputs.add(output);
		
		// if the child is a store value reactor
		// we just need to push its nouns as outputs
		for(IReactor childReactor : this.childReactor) {
			if(childReactor instanceof StoreValue) {
				GenRowStruct keyStruct = childReactor.getNounStore().getNoun(StoreValue.KEY_NOUN);
				outputs.addAll(keyStruct.vector);
			}
		}
		
		return outputs;
	}
}
