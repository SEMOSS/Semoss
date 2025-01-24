package prerna.reactor;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExecuteReactorFunctionEngineReactor extends AbstractReactor {

	public ExecuteReactorFunctionEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int [] {1};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Reactor Function Engine " + engineId + " does not exist or user does not have access to this function");
		}
		
		IEngine engine = Utility.getReactorEngine(engineId);
		if(!(engine instanceof IReactorFunctionEngine)) {
			throw new IllegalArgumentException("This function engine is not a Reactor Function Engine");
		}
		
		IReactorFunctionEngine reactorFunctionEngine = (IReactorFunctionEngine) engine;
		reactorFunctionEngine.setNounStore(getNounStore());
		return reactorFunctionEngine.execute();
	}

}
