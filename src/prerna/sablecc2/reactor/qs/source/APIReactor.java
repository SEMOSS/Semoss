package prerna.sablecc2.reactor.qs.source;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.impl.json.JsonAPIEngine;
import prerna.engine.impl.json.JsonAPIEngine2;
import prerna.engine.impl.web.WebScrapeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class APIReactor extends AbstractQueryStructReactor {

	public APIReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		createTemporalStruct();
		// I am hoping this is almost always engine
		// need to account if this is a hard query struct
		if(this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				this.qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		}

		return this.qs;
				
	}
	
	
	// this is not even create it is just modify
	// I will modify this into a temporal
	// may be I can move this into API later instead of force fitting into a database
	private void createTemporalStruct()
	{
		// need to get a couple of things for this to be a temporal query struct
		// I need the type of API engine
		// so I would say apiType = json
		// I also need alias = alias can be thought of as a full smss configuration I can load
		// This would include
		// url possibly - we will call this the source
		// type of operation - get or post
		// Select is already coming through as a select - I dont need to do much
		// Filter is also there so I dont need to do much in terms of input either
		
		String apiType = this.getNounStore().getNoun("api_type").getNoun(0).getValue() + "" ;
		
		// need some way of figuring out what api engine to use to which one
		// for now I will just force fit
		IEngine engine = null;
		
		if(apiType.equalsIgnoreCase("JSON"))	
			engine = new JsonAPIEngine();

		else if(apiType.equalsIgnoreCase("JSON2"))	
			engine = new JsonAPIEngine2();

		else if(apiType.equalsIgnoreCase("WEB"))	
			engine = new WebScrapeEngine();

		
		// if there is alias get it
		if(this.getNounStore().getNoun("aliasFile") != null)
		{
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			String alias = this.getNounStore().getNoun("alias").getNoun(0).getValue() + "" ;
			// load the alias
			// as the properties
			// asset lib
			
			// this alias
			Properties aliasProp = getAlias(baseFolder + "/" + alias);
					
			engine.setPropFile(baseFolder + "/" + alias);
			engine.setProp(aliasProp);
			
			// make up an engine name
			String engineName = aliasProp.getProperty("engine_name");
			qs.setEngineId(engineName);
			qs.setEngine(engine);
		}
		else if(this.getNounStore().getNoun("aliasMap") != null)
		{
			String source = "";
			String operation = "";
			// may be I can fit all these things into the alias map and call it a day. 
			HashMap map = null;
			
			// also need a headersmap
			
			if(this.getNounStore().getNoun("aliasMap") != null)
				map = (HashMap)this.getNounStore().getNoun("aliasMap").getNoun(0).getValue();
			
			if(this.getNounStore().getNoun("headersMap") != null)
				map.put("HEADERS", this.getNounStore().getNoun("headersMap").getNoun(0).getValue());
			
			Properties aliasProp = getAlias(null);
			
			Iterator keys = map.keySet().iterator();
			while(keys.hasNext())
			{
				Object thisKey = keys.next();
				aliasProp.put(thisKey, map.get(thisKey));
			}
			
			// mandatory input is not there set it
			if(!aliasProp.containsKey("mandatory_input"))	
				aliasProp.put("mandatory_input", "");
			engine.setProp(aliasProp);

			String engineName = apiType + Utility.getRandomString(6);
			qs.setEngineId(engineName);
			qs.setEngine(engine);
			engine.setEngineId(engineName);
		}
	}
	
	private Properties getAlias(String alias)
	{
		Properties retProp = new Properties();
		if(alias != null)
			// needs to implement this later
			retProp = Utility.loadProperties(alias);
		return new Properties();
	}
}
