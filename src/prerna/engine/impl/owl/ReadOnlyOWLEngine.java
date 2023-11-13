package prerna.engine.impl.owl;

import prerna.engine.impl.rdf.RDFFileSesameEngine;

public class ReadOnlyOWLEngine extends AbstractOWLEngine {

	public ReadOnlyOWLEngine(RDFFileSesameEngine baseDataEngine, String engineId, String engineName) {
		super(baseDataEngine, engineId, engineName);
	}

	@Override
	public RDFFileSesameEngine getBaseDataEngine() {
		return null;
	}

	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		
	}
}

