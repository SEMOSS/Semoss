package prerna.reactor.function;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
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
			throw new IllegalArgumentException(getUnableToAccessError(engineId));
		}
		
		IReactorFunctionEngine reactorFunctionEngine = Utility.getReactorEngine(engineId);
		reactorFunctionEngine.setNounStore(getNounStore());
		return reactorFunctionEngine.execute();
	}
	
	String getUnableToAccessError(String engineId) {
		return "Reactor Function Engine " + engineId + " does not exist or user does not have access to this function";
	}

}
