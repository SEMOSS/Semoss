package prerna.reactor;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.reactor.tax.StoreValue;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class IfReactor extends AbstractReactor implements JavaExecutable {

	private static int ifMethodCount = 0;

	private boolean caseEvaluation = false;
	
	public IfReactor() {
		this.keysToGet = new String[] {"condition", "true", "false"};
	}
	
	@Override
	public NounMetadata execute() {
		// on the translation
		// we already push which result is the true/false case
		// so all we need to do is grab the only thing in the curRow
		// and push it up
		NounMetadata noun = null;
		if(this.curRow.size() == 1) {
			// we dont have anything else in the cur row except
			// the boolean condition
			noun = new NounMetadata("no value", PixelDataType.CONST_STRING);
		} else {
			noun = this.curRow.getNoun(1);
		}
		// evaluate the true statement
		return evaluateStatement(noun);
	}
	
	public boolean getBooleanEvaluation() {
		Object ifEvaluatorObject = this.curRow.get(0);
		PixelDataType ifEvaluatorType = this.curRow.getMeta(0);
		
		// the input can be any reactor or a filter within the if statment
		// grab it and evalute based on its type
		
		if(ifEvaluatorType == PixelDataType.BOOLEAN) {
			this.caseEvaluation = (boolean) ifEvaluatorObject;
		} else if(ifEvaluatorType == PixelDataType.COLUMN) {
			this.caseEvaluation = (boolean) this.planner.getVariableValue(ifEvaluatorObject.toString()).getValue();
		} else if(ifEvaluatorType == PixelDataType.BOOLEAN_METADATA) {
			BooleanValMetadata filterVal = (BooleanValMetadata) ifEvaluatorObject;
			this.caseEvaluation = filterVal.getFilterVal();
		} else if(ifEvaluatorType == PixelDataType.FILTER) {
			// we have a filter object
			// use its evaluate method
			Filter filter = (Filter) ifEvaluatorObject;
			this.caseEvaluation = filter.evaluate(this.planner);
		} else if(ifEvaluatorType == PixelDataType.LAMBDA) {
			// we have a full reactor
			// required that this returns a boolean
			AbstractReactor ifEvaluatorReactor = null;
			try {
				ifEvaluatorReactor = (AbstractReactor) ifEvaluatorObject;
				this.caseEvaluation = (boolean) ifEvaluatorReactor.execute().getValue();
			} catch(ClassCastException e) {
				if(ifEvaluatorReactor != null) {
					throw new IllegalArgumentException("If statement condition (" + ifEvaluatorReactor.getPixel()[1] + ") could not be evaluated");
				} else {
					throw new IllegalArgumentException("If statement condition could not be evaluated");
				}
			}
		}
		return this.caseEvaluation;
	}
	
	private NounMetadata evaluateStatement(NounMetadata evalNoun) {
		// if it is another reactor
		// let the reactor execute and handle the returning of its data
		if(evalNoun.getValue() instanceof AbstractReactor) {
			AbstractReactor trueReactor = (AbstractReactor) evalNoun.getValue();
			trueReactor.evaluate = true;
			return trueReactor.execute();
		} else if(evalNoun.getNounType() == PixelDataType.VECTOR) {
			List<NounMetadata> evalutedValues = new Vector<NounMetadata>();
			// need to loop through all the portions
			List<NounMetadata> values = (List<NounMetadata>) evalNoun.getValue();
			for(int i = 0; i < values.size(); i++) {
				evalutedValues.add(evaluateStatement(values.get(i)));
			}
			NounMetadata evalutedNoun = new NounMetadata(evalutedValues, PixelDataType.VECTOR, evalNoun.getOpType());
			evalutedNoun.addAllAdditionalReturn(evalNoun.getAdditionalReturn());
			return evalutedNoun;
		} else {
			// ughh...
			// must be an evaluted value
			// just return it
			return evalNoun;
		}
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.LAMBDA);
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
	
	
	
	
	
	
	
	///////////////////////////////////////
	///////////////////////////////////////
	///////////////////////////////////////
	///////////////////////////////////////
	///////////////////////////////////////
	
	// TODO sync up with TAX Team to get correct methods for below
	
	

	@Override
	public String getJavaSignature() {
		String returnType = getReturnType();
		if(returnType.equals("Object")) {
			return getUncertainReturnIfStatement();
		} else {
			return getTernaryIfStatement();
		}
	}
	
	
	//Todo: how do we handle types
	private String getUncertainReturnIfStatement() {
		return getTernaryIfStatement();
	}
	
	private static String createNewIfMethodName() {
		return "if"+ifMethodCount++;
	}
	
	private String createIfMethod(String ifMethodName, String returnType) {
		StringBuilder ifBody = new StringBuilder();
		ifBody.append("public Object "+ifMethodName+"(){");
		
		ifBody.append("}");
		return ifBody.toString();
	}
	
	private String getTernaryIfStatement() {
		Object trueCase = curRow.get(1);
		PixelDataType trueType = curRow.getMeta(1);
		
		Object falseCase;
		PixelDataType falseType;
		if(curRow.size() > 2) {
			falseCase = curRow.get(2);
			falseType = curRow.getMeta(2);
		} else {
			falseCase = getParentAssignment();
			falseType = PixelDataType.COLUMN;
		}
		
		String trueString;
		String falseString;
		
		if(trueCase instanceof JavaExecutable) {
			trueString = ((JavaExecutable)trueCase).getJavaSignature();
		} else if(trueType == PixelDataType.CONST_STRING){
			trueString = "\""+trueCase.toString()+"\"";
		} else {
			trueString = trueCase.toString();
		}
		
		if(falseCase instanceof JavaExecutable) {
			falseString = ((JavaExecutable)falseCase).getJavaSignature();
		} else if(falseType == PixelDataType.CONST_STRING) {
			
			//Hard coded special case, not sure how to handle this currently
			if(falseCase.toString().equals("Select Scenario")) {
				falseString = "1";
			} else if(trueType == PixelDataType.CONST_DECIMAL || trueType == PixelDataType.CONST_INT) {
				try {
					double number = Double.parseDouble(falseCase.toString().trim());
					falseString = number + "";
				} catch(Exception e) {
					falseString = "\""+falseCase.toString()+"\"";
				}
			}
			
			
			
			else {
				falseString = "\""+falseCase.toString()+"\"";
			}
		} else {
			falseString = falseCase.toString();
		}
		
		
		
		return "(" + getFilterString() + " ? " + trueString + " : "+ falseString + ")";
	}
	
	private String getFilterString() {
		Object filter = curRow.get(0);
		String filterString;
		if(filter instanceof JavaExecutable) {
			filterString = ((JavaExecutable)filter).getJavaSignature();
		} else {
			filterString = filter.toString();
		}
		return filterString;
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		return null;
	}

	@Override
	public String getReturnType() {
		NounMetadata trueNoun = curRow.getNoun(1);
		String trueReturn = getReturnType(trueNoun);
		
		//check return type for optional false return
		String falseReturn;
		if(curRow.size() > 2) {
			NounMetadata falseNoun = curRow.getNoun(2);
			falseReturn = getReturnType(falseNoun);
		} else {
			falseReturn = trueReturn;
		}
		
		return trueReturn;
//		//if they are not the same return object
//		if(trueReturn.equals(falseReturn)) {
//			return trueReturn;
//		} else {
//			return "Object";
//		}
 		
		
	}
	
	private String getReturnType(NounMetadata returnNoun) {
		String returnType = null;
		Object returnObj = returnNoun.getValue();
		if(returnObj instanceof JavaExecutable) {
			returnType = ((JavaExecutable)returnObj).getReturnType();
		} else if(returnNoun.getNounType() == PixelDataType.CONST_DECIMAL || returnNoun.getNounType() == PixelDataType.CONST_INT) {
			returnType = "double";
		} else if(returnNoun.getNounType() == PixelDataType.CONST_STRING) {
			returnType = "String";
		} else if(returnNoun.getNounType() == PixelDataType.BOOLEAN){
			returnType = "boolean";
		} else if(returnNoun.getNounType() == PixelDataType.COLUMN) {
			returnType = returnObj.toString();
		} else {
			returnType = "Object";
		}
		return returnType;
	}
	
	private String getParentAssignment() {
		if(this.parentReactor instanceof IfReactor) {
			return ((IfReactor)parentReactor).getParentAssignment();
		} else if(this.parentReactor instanceof AssignmentReactor) {
			return ((AssignmentReactor)parentReactor).operationName;
		} else {
			return "";
		}
	}
}
