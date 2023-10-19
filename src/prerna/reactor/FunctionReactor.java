package prerna.reactor;

import prerna.engine.api.IEngine;
import prerna.engine.api.IReactorEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class FunctionReactor extends AbstractReactor {

	public FunctionReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int [] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		String engineId = this.store.getNoun(keysToGet[0]).get(0) + "";
		
		IEngine engine = Utility.getFunctionEngine(engineId);
		NounMetadata retData = null;
		if(engine instanceof IReactorEngine)
		{
			System.err.println("Got the reactor engine");
			IReactor engine2 = (IReactor)engine;
			engine2.setNounStore(getNounStore());
			retData = engine2.execute();
		}
		
		return retData;
	}

}
