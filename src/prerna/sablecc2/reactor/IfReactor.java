package prerna.sablecc2.reactor;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class IfReactor extends AbstractReactor {

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		// TODO Auto-generated method stub
		System.out.println("Signature " + signature);
		System.out.println("Cur Row is >> " + this.curRow);
		System.out.println("In the out of IF !!");
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void updatePlan() {
		// TODO Auto-generated method stub

	}
	
	// execute it
	// once again this would be abstract
	public Object execute()
	{
		System.out.println("Execute the method.. " + signature);
		System.out.println("Printing NOUN Store so far.. " + store);
		System.out.println("Children are..." + childReactor);
		
		GenRowStruct filterGenRowStruct = this.getNounStore().getNoun("f");
		Filter filter = (Filter) filterGenRowStruct.get(0);
		// this is if the filter returned true or false
		// we need to set the pksl planner in case the filter is using variables
		boolean caseEvaluation = filter.evaluate(this.planner);

		// the if will always have 2 values in its curRow
		// the first value is the true object
		// the second value is the false object
		// based on the case evaluation, we will know which one to evaluate
		if(caseEvaluation == true) {
			Object trueObj = this.curRow.get(0);
			PkslDataTypes trueObjMeta = this.curRow.getMeta(0);
			// evaluate the true statement
			return evaluateStatement(trueObj, trueObjMeta);
		} else {
			Object falseObj = this.curRow.get(1);
			PkslDataTypes falseObjMeta = this.curRow.getMeta(1);
			// evaluate the false statement
			return evaluateStatement(falseObj, falseObjMeta);
		}
	}
	
	private Object evaluateStatement(Object statementObj, PkslDataTypes statementType) {
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
}
