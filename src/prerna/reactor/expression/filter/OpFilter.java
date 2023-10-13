package prerna.reactor.expression.filter;

import java.util.List;

import prerna.reactor.JavaExecutable;
import prerna.reactor.expression.OpBasic;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpFilter extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		// there are 3 things being passed
		// index 0 is left term
		// index 1 is comparator
		// index 2 is right term
		Object left = values[0];
		Object right = values[2];
		String comparator = values[1].toString().trim();
		boolean evaluation = false;
		if(comparator.equals("==")) {
			if(left instanceof Number && right instanceof Number) {
				evaluation = ((Number)left).doubleValue() == ((Number)right).doubleValue();
			} else if(left instanceof String && right instanceof String){
				evaluation = left.toString().equals(right.toString());
			} else {
				evaluation = left == right;
			}
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			if(left instanceof Number && right instanceof Number) {
				evaluation = ((Number)left).doubleValue() != ((Number)right).doubleValue();
			} else if(left instanceof String && right instanceof String){
				evaluation = !left.toString().equals(right.toString());
			} else {
				evaluation = left != right;
			}
		}
		// we have some numerical stuff
		// everything needs to be a valid number
		else if(comparator.equals(">=")) {
			evaluation = ((Number)left).doubleValue() >= ((Number)right).doubleValue();
		} else if(comparator.equals(">")) {
			evaluation = ((Number)left).doubleValue() > ((Number)right).doubleValue();
		} else if(comparator.equals("<=")) {
			evaluation = ((Number)left).doubleValue() <= ((Number)right).doubleValue();
		} else if(comparator.equals("<")) {
			evaluation = ((Number)left).doubleValue() < ((Number)right).doubleValue();
		} else {
			throw new IllegalArgumentException("Cannot handle comparator " + comparator);
		}
		
		return new NounMetadata(evaluation, PixelDataType.BOOLEAN);
	}
	
	public String getJavaSignature() {
		List<NounMetadata> inputs = this.getJavaInputs();
		
		NounMetadata leftSide = inputs.get(0);
		Object leftSideValue = leftSide.getValue();
		String leftString;
		String checkType;
		boolean needCheckType = false;
		if(leftSideValue instanceof JavaExecutable) {
			leftString = ((JavaExecutable)leftSideValue).getJavaSignature();
		} else if(leftSide.getNounType() == PixelDataType.CONST_STRING) {
			leftString = "\""+leftSideValue.toString()+"\"";
		} else {
			leftString = leftSideValue.toString();
		}
		
		String comparator = inputs.get(1).getValue().toString().trim();
		if(comparator.equals("<>")) {
			comparator = "!=";
		}
		
		NounMetadata rightSide = inputs.get(2);
		Object rightSideValue = rightSide.getValue();
		String rightString;
		if(rightSideValue instanceof JavaExecutable) {
			rightString = ((JavaExecutable)rightSideValue).getJavaSignature();
		} else if(rightSide.getNounType() == PixelDataType.CONST_STRING ) {
			needCheckType = true;
			rightString = "\""+rightSideValue.toString()+"\"";
		} else {
			rightString = rightSideValue.toString();
		}
		
		if(needCheckType){
			checkType = "compareString("+leftString+" , \""+comparator+"\" ,"+ rightString+" )";
			return checkType;
		}
		
		return leftString + " " +comparator + " " + rightString;

	}

	@Override
	public String getReturnType() {
		return "boolean";
	}
}
