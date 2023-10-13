package prerna.reactor.json.processor;

import prerna.reactor.json.validator.JsonValidatorReactorFactory;

public class JsonProcessorReactorFactory extends JsonValidatorReactorFactory {

	{
		createReactorHash.clear();
		editReactorHash.clear();
		viewReactorHash.clear();

		// actual classes
		viewReactorHash.put("Provider", ProviderProcessor.class.getName());
		viewReactorHash.put("Profile", ProfileProcessor.class.getName());
		viewReactorHash.put("taxIds", TaxIdsProcessor.class.getName());
	}

}
