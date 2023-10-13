package prerna.reactor.json.validator;

import java.util.Hashtable;

import prerna.reactor.json.GreedyJsonReactor;

public class ParentValidator extends GreedyJsonReactor{

	// the method to implement here is validate
	// In our case the super parent will not throw any error
	public void process()
	{
		
		// shallow validation
		// data validation
		// business rule validation

		Hashtable<String, Object> allInputs = this.store.getDataHash();
		for(String key : allInputs.keySet()) {
			System.out.println("key = " + key + " , value = " + allInputs.get(key));
		}
		
//		// I will throw a random error here
//		addError("ABC", "Child should behave like one.. right now it is not");
//		// also add a stage here
//		addErrorWithStage("DEF", "Child should behave like one.. right now it is not", "BRE_VALIDATION");
	}	
}
