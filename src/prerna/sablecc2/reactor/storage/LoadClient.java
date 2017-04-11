package prerna.sablecc2.reactor.storage;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class LoadClient extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	//TODO: find a common place to put these
	public static final String STORE_NOUN = "store";
	public static final String ENGINE_NOUN = "engine";
	public static final String CLIENT_NOUN = "client";
	public static final String SCENARIO_NOUN = "scenario";
	public static final String VERSION_NOUN = "version";

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		//TODO: need to implement
		
		return null;
	}
}
