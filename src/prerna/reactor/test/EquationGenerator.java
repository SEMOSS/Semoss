package prerna.reactor.test;

import java.util.Random;

public class EquationGenerator {

	String[] varNames;
	Random rand = new Random();
	
	EquationGenerator() {
		rand = new Random();
		generateVarNames(1);
	}
	
	public void setVarNames(String[] varNames) {
		this.varNames = varNames;
	}
	
	public void generateVarNames(int n) {
//		List<String> vars = new ArrayList<>(n);
//		for(int i = 0; i < n; i++) {
//			for(int j = 0; j < 26; j++) {
//				
//			}
//			
//		}
		varNames = new String[26];
		for(int i = 0; i < varNames.length; i++) {
			varNames[i] = ((char)('a' + i))+"";
		}
	}
	
	public String[] getRandomEquations(int n) {
		String[] equations = new String[n];
		for(int i = 0; i < equations.length; i++) {
			equations[i] = getRandomEquation();
		}
		return equations;
	}
	
	//no 0's used
	private String getRandomEquation() {
		//get value from Varnames
		//equals
		//some random math formula
		return getRandomVar() + " =  "+ getRandomFormula();
	}
	
	private String getRandomFormula() {
		int randomInt = rand.nextInt(5);
		if(randomInt == 0) {
			return getRandomValue() + getRandomOperator() + getRandomValue();
		} else {
			return getRandomValue() + getRandomOperator() + "(" + getRandomFormula() + ")";
		}
	}
	
	private String getRandomOperator() {
		int randomInt = rand.nextInt(4);
		if(randomInt == 0) {
			return " + ";
		} else if(randomInt == 1) {
			return " - ";
		} else if(randomInt == 2) {
			return " * ";
		} else if(randomInt == 3) {
			return " + ";
		} else {
			return " + ";
		}
	}
	
	private String getRandomValue() {
		int randomInt = rand.nextInt(2);
		if(randomInt == 0) {
			return (rand.nextInt(100)+1)+"";
		} else {
			return getRandomVar();
		}
	}
	
	private String getRandomVar() {
		int varLength = varNames.length;
		return varNames[rand.nextInt(varLength)];
	}
}
