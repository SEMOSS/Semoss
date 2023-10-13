package prerna.reactor.json.validator;

import prerna.reactor.json.GreedyJsonReactor;

public class Parent2Validator extends GreedyJsonReactor{

	public Parent2Validator()
	{
		emailFields = new String[]{"email"};
		npiFields = new String[]{"npi"};
		phoneFields = new String[]{"phone"};
		ssnFields = new String[]{"ssn"};
		zipFields = new String [] {"zipcode"};
		mandatoryFields = new String [] {"somerandom", "somerandom2"};
	}
	// the method to implement here is validate
	// In our case the super parent will not throw any error
	public void process()
	{
		
		// shallow validation
		// do this only if you want to
		// if we want to do it for all.. I will move to the base method
		shallowValidate();
		
		// data validation
		// business rule validation
		System.out.println("parent value" + this.getValue("hello", true));

		// I will throw a random error here
		addError("Hello", "Child should behave like one.. right now it is not");
		// also add a stage here
		//addErrorWithStage("OMG", "Child should behave like one.. right now it is not", "BRE_VALIDATION");
	}	
}
