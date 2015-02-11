package prerna.rdf.engine.wrappers;

import prerna.rdf.engine.api.IConstructWrapper;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.IEngineWrapper;
import prerna.rdf.engine.api.ISelectWrapper;

public class WrapperManager {

	// main job of this class is to take an engine
	// find the type of the engine
	// and the type of query
	// and then give back a wrapper
	// I need to make this completely through reflection
	// I will do that later
	
	public static WrapperManager manager = null;
	
	protected WrapperManager()
	{
		
	}
	
	public static WrapperManager getInstance()
	{
		// cant get lazier than this :)
		if(manager == null)
		{
			manager = new WrapperManager();
			// some other routine to load it
		}
		return manager;
	}
	
	public ISelectWrapper getSWrapper(IEngine engine, String query)
	{
		ISelectWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameSelectWrapper();
				break;
			}
			case JENA: {
				returnWrapper = new JenaSelectWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameSelectWrapper();
				break;
			}
			case RDBMS:{
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			//ISelectWrapper doh = (ISelectWrapper)returnWrapper;
			returnWrapper.getVariables();
			
			return returnWrapper;
	}

	public IConstructWrapper getCWrapper(IEngine engine, String query)
	{
		IConstructWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameConstructWrapper();
				break;
			}
			case JENA: {
				returnWrapper = new JenaConstructWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameConstructWrapper();
				break;
			}
			case RDBMS:{
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			
			return returnWrapper;
	}

	public IConstructWrapper getChWrapper(IEngine engine, String query)
	{
		IConstructWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameSelectCheater();
				break;
			}
			case JENA: {
				returnWrapper = new JenaSelectCheater();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameSelectCheater();
				break;
			}
			case RDBMS:{
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			//ISelectWrapper doh = (ISelectWrapper)returnWrapper;
			//returnWrapper.getVariables();
			
			return returnWrapper;
	}
	
}
