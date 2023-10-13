package prerna.reactor.json.validator;

import java.util.Hashtable;

import prerna.reactor.IReactor;

public class JsonValidatorReactorFactory {

	protected Hashtable <String, String> viewReactorHash = new Hashtable<String, String>();
	protected Hashtable <String, String> createReactorHash = new Hashtable<String, String>();
	protected Hashtable <String, String> editReactorHash = new Hashtable<String, String>();
	
	{
		// ADDING THE CREATE REACTOR VALIDATORS
		createReactorHash.put("Provider", "prerna.sablecc2.reactor.json.validator.ProviderValidator");
		createReactorHash.put("Profile", "prerna.sablecc2.reactor.json.validator.ProfileValidator");
		
		
		// ADDING THE EDIT REACTOR VALIDATORS
		
		
		// ADDING THE VIEW REACTOR VALIDATORS
		
		
		
		
		// testing!!!
		createReactorHash.put("SuperParent", "prerna.sablecc2.reactor.json.validator.SuperParentValidator");
		createReactorHash.put("Parent", "prerna.sablecc2.reactor.json.validator.ParentValidator");
		createReactorHash.put("Parent2", "prerna.sablecc2.reactor.json.validator.Parent2Validator");
		createReactorHash.put("Child2", "prerna.sablecc2.reactor.json.GreedyJsonReactor");
		//createReactorHash.put("Parent2", "prerna.sablecc2.reactor.json.validator.Parent2Validator");
//		reactorHash.put("array2", "prerna.sablecc2.reactor.json.validator.Parent2Validator");
	}
	
	// all the super nodes for json
	public IReactor getReactor(String keyName, String requestType)
	{
		Hashtable<String, String> reactorHash = getRequestTypeHash(requestType);
		String reactorName = reactorHash.get(keyName);
		IReactor finalReactor = null;
		if(reactorName != null) {
			try {
				finalReactor = (IReactor)Class.forName(reactorName).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return finalReactor;
	}
	
	private Hashtable <String, String> getRequestTypeHash(String requestType) {
		requestType = requestType.toUpperCase();
		if(requestType.equals("VIEW")) {
			return this.viewReactorHash;
		} else if(requestType.equals("CREATE")) {
			return this.createReactorHash;
		} else if(requestType.equals("EDIT")) {
			return this.editReactorHash;
		} else {
			throw new IllegalArgumentException("You need to define the request type as either: \"CREATE\", \"EDIT\", or \"VIEW\"");
		}
	}
	
}
