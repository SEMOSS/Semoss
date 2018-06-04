package prerna.sablecc2.reactor.utils;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
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
		engineName = MasterDatabaseUtility.testEngineIdIfAlias(engineName);

		IEngine engine = Utility.getEngine(engineName);

		// validate engine exists
		if (engine == null){
			throw new IllegalArgumentException("Engine doesnt exist");
		}
		String owlPath = engine.getOWL();

		// only executes for rdbms, tinker, and rdf
		ENGINE_TYPE engineType = engine.getEngineType();
		if (engineType.equals(ENGINE_TYPE.RDBMS) || engineType.equals(ENGINE_TYPE.SESAME) || engineType.equals(ENGINE_TYPE.TINKER)) {
			OWLER owl = new OWLER(engine, owlPath);
			owl.addUniqueCounts(engine);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
