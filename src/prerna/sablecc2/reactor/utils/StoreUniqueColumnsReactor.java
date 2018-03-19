package prerna.sablecc2.reactor.utils;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.OWLER;
import prerna.util.Utility;

public class StoreUniqueColumnsReactor extends AbstractReactor {
	
	public StoreUniqueColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get inputs - engine
		String engineName = this.keyValue.get(this.keysToGet[0]);
		IEngine engine = Utility.getEngine(engineName);
		// validate engine exists
		if (engine == null){
			throw new IllegalArgumentException("Engine doesnt exist");
		}
		String owlPath = engine.getOWL();

		// only executes for rdbms, tinker, and rdf
		ENGINE_TYPE engineType = engine.getEngineType();
		if (engineType.equals(ENGINE_TYPE.RDBMS) || engineType.equals(ENGINE_TYPE.SESAME) || engineType.equals(ENGINE_TYPE.TINKER)) {
			Vector<String> concepts = engine.getConcepts(false);
			OWLER owl = new OWLER(engine, owlPath);
			for (int i = 0; i < concepts.size(); i++) {
				String thisConcept = concepts.get(i);
				String base = "http://semoss.org/ontologies/Relation/Contains/" + Utility.getInstanceName(thisConcept)
						+ "/" + Utility.getInstanceName(thisConcept);
				
				// add unique count for concepts
				if (Utility.getInstanceName(base).equals(Utility.getClassName(base))){
					owl.addUniqueCounts(thisConcept, base, engine);
				}
				
				// iterate properties of concept and add unique
				// values for each to the owl file
				List<String> properties = engine.getProperties4Concept(thisConcept, true);
				for (int j = 0; j < properties.size(); j++) {
					owl.addUniqueCounts(thisConcept, properties.get(j), engine);
				}
			}
		}
		return null;
	}

}
