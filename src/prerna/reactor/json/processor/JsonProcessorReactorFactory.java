package prerna.reactor.json.processor;

import prerna.reactor.json.validator.JsonValidatorReactorFactory;

public class JsonProcessorReactorFactory extends JsonValidatorReactorFactory {

	{
		createReactorHash.clear();
		editReactorHash.clear();
		viewReactorHash.clear();

		// actual classes
		viewReactorHash.put("Provider", "prerna.sablecc2.reactor.json.processor.ProviderProcessor");
		viewReactorHash.put("Profile", "prerna.sablecc2.reactor.json.processor.ProfileProcessor");
		viewReactorHash.put("taxIds", "prerna.sablecc2.reactor.json.processor.TaxIdsProcessor");
	}

}
