package prerna.reactor.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PixelGenerator {

	private String[] aliases; //these are used to create pixels
	
	
	private int count;
	private String[] reactorList;
	Random random;
	
	private static final int maxNumberOfArguments = 40;
	private static final int probabilityOfReactor = 6;
	
	public PixelGenerator() {
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
	
	public Map<String, String> getRandomPixels(int n) {
		Map<String, String> randomPksls = new HashMap<>();
		for(int i = 0; i < n; i++) {
			randomPksls.put(generateNewAlias(), generatePixelFormula());
		}
		return randomPksls;
	}
	
	private String generatePixelFormula() {
		int randomint = random.nextInt(20);
		
		if(randomint == 0) return generateNewPixel(probabilityOfReactor);
		else return generateNewPixelValue(probabilityOfReactor) + getRandomOperator() + generateNewPixelValue(probabilityOfReactor);
		 
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
	
	private String generateNewPixel(int reactorProbabilityInverse) {
		int randomInt = random.nextInt(reactorProbabilityInverse);
		if(randomInt == 0) {
			return generateNewixelReactor();
		} else {
			return getRandomAlias();
		} 
	}
	//returns either an alias, a constant, or reactor with arguments
	private String generateNewPixelValue(int reactorProbabilityInverse) {
		int randomInt = random.nextInt(reactorProbabilityInverse);
		if(randomInt == 0) {
			return generateNewixelReactor();
		} else {
			return getRandomAlias();
		} 
		
//		else {
//			DecimalFormat df2 = new DecimalFormat("#.##");
//			return df2.format(random.nextDouble()+1.1);
//		}
	}
	
	private String generateNewixelReactor() {
		int numberOfArguments = random.nextInt(maxNumberOfArguments)+1;
		String reactor = getRandomReactor();
		reactor += "(";
		for(int i = 0; i < numberOfArguments; i++) {
			if(i > 0) {
				reactor += ", ";
			}
			reactor += generateNewPixelValue(probabilityOfReactor*5);
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
