package prerna.sablecc2.reactor;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class IfReactor extends AbstractReactor {

	@Override
	public void In() {
        curNoun("all");
        
        // even though we are parsing all the various traversal paths
        // we will only execute when enforced (look at the execute routine below)
        // or if this is the root node
        if(this.parentReactor == null || !(this.parentReactor instanceof IfReactor) ) {
        	// there is an existing boolean within the AbstractReactor
        	// that keeps track of evaluation
        	this.evaluate = true;
        }
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
		
		// we control evaluation of the if reactor
		// so while we parse all conditions
		// we only execute based on the filter evaluation
		if(evaluate) {
			GenRowStruct filterGenRowStruct = this.getNounStore().getNoun("f");
        	Filter filter = (Filter) filterGenRowStruct.get(0);
        	// this is if the filter returned true or false
        	boolean caseEvaluation = filter.evaluate();
			
        	// the if will always have 2 values in its curRow
        	// the first value is the true object
        	// the second value is the false object
        	// based on the case evaluation, we will know which one to evaluate
        	if(caseEvaluation == true) {
        		Object trueObj = this.curRow.get(0);
        		PkslDataTypes trueObjMeta = this.curRow.getMeta(0);
        		// if it is another reactor
        		// let the reactor execute and handle the returning of its data
        		if(trueObj instanceof AbstractReactor) {
        			AbstractReactor trueReactor = (AbstractReactor) trueObj;
        			trueReactor.evaluate = true;
        			return trueReactor.execute();
        		} else {
        			// ughh...
        			// must be a constant value ?
        			// unsure what else would ever end up here 
        			return getNounDataForConstant(trueObj, trueObjMeta);

        		}
			} else {
				Object falseObj = this.curRow.get(1);
        		PkslDataTypes falseObjMeta = this.curRow.getMeta(1);
        		// if it is another reactor
        		// let the reactor execute and handle the returning of its data
				if(falseObj instanceof AbstractReactor) {
        			AbstractReactor trueReactor = (AbstractReactor) falseObj;
        			trueReactor.evaluate = true;
        			return trueReactor.execute();
        		} else {
        			// ughh...
        			// must be a constant value ?
        			// unsure what else would ever end up here
        			return getNounDataForConstant(falseObj, falseObjMeta);
        		}
			}
		}
		
		return null;
	}
	
	private NounMetadata getNounDataForConstant(Object obj, PkslDataTypes pkslDataTypes) {
		NounMetadata data = new NounMetadata(obj, pkslDataTypes);
		return data;
	}
}
