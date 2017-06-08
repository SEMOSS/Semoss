package prerna.sablecc2.reactor.test;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class PkslGenerator {

	private String[] aliases; //these are used to create pksls
	
	
	private int count;
	private String[] reactorList;
	Random random;
	
	private static final int maxNumberOfArguments = 40;
	private static final int probabilityOfReactor = 6;
	
	public PkslGenerator() {
		initDefaults();
	}
	
	private void initDefaults() {
		count = 0;
		reactorList = new String[]{"Sum", "Max", "Min", "Mean", "Median"};
		random = new Random();
	}
	
	public void setConstants(String[] constants) {
		this.aliases = constants;
	}
	
	public Map<String, String> getRandomPksls(int n) {
		Map<String, String> randomPksls = new HashMap<>();
		for(int i = 0; i < n; i++) {
			randomPksls.put(generateNewAlias(), generatePkslFormula());
		}
		return randomPksls;
	}
	
//	private String generateRandomPksl() {
//		String assignment = generateNewAlias();
//		String pkslValue = generatePkslFormula();
//		return assignment + " = " + pkslValue;
//	}
	
	private String generatePkslFormula() {
		int randomint = random.nextInt(20);
		
		if(randomint == 0) return generateNewPksl(probabilityOfReactor);
		else return generateNewPkslValue(probabilityOfReactor) + getRandomOperator() + generateNewPkslValue(probabilityOfReactor);
		 
	}
	
	private String getRandomOperator() {
		int randomInt = random.nextInt(4);
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
	
	private String generateNewPksl(int reactorProbabilityInverse) {
		int randomInt = random.nextInt(reactorProbabilityInverse);
		if(randomInt == 0) {
			return generateNewPkslReactor();
		} else {
			return getRandomAlias();
		} 
	}
	//returns either an alias, a constant, or reactor with arguments
	private String generateNewPkslValue(int reactorProbabilityInverse) {
		int randomInt = random.nextInt(reactorProbabilityInverse);
		if(randomInt == 0) {
			return generateNewPkslReactor();
		} else {
			return getRandomAlias();
		} 
		
//		else {
//			DecimalFormat df2 = new DecimalFormat("#.##");
//			return df2.format(random.nextDouble()+1.1);
//		}
	}
	
	private String generateNewPkslReactor() {
		int numberOfArguments = random.nextInt(maxNumberOfArguments)+1;
		String reactor = getRandomReactor();
		reactor += "(";
		for(int i = 0; i < numberOfArguments; i++) {
			if(i > 0) {
				reactor += ", ";
			}
			reactor += generateNewPkslValue(probabilityOfReactor*5);
		}
		reactor += ")";
		return  reactor;
	}
	
	private String generateNewAlias() {
		return "a"+count++;
	}
	
	private String getRandomReactor() {
		return reactorList[random.nextInt(reactorList.length)];
	}
	
	private String getRandomAlias() {
		return aliases[random.nextInt(aliases.length)];
	}
}
