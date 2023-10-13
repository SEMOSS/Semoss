package prerna.reactor.qs.source;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.json.JsonAPIEngine;
import prerna.engine.impl.json.JsonAPIEngine2;
import prerna.engine.impl.web.WebScrapeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.ConfigSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class APIReactor extends AbstractQueryStructReactor {

	public APIReactor() {
		this.keysToGet = new String[]{};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		createTemporalStruct();
		this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.DIRECT_API_QUERY);
		return this.qs;
	}

	// this is not even create it is just modify
	// I will modify this into a temporal
	// may be I can move this into API later instead of force fitting into a
	// database
	private void createTemporalStruct() {
		// need to get a couple of things for this to be a temporal query struct
		// I need the type of API engine
		// so I would say apiType = json
		// I also need alias = alias can be thought of as a full smss configuration I
		// can load
		// This would include
		// url possibly - we will call this the source
		// type of operation - get or post
		// Select is already coming through as a select - I dont need to do much
		// Filter is also there so I dont need to do much in terms of input either

		String apiType = this.getNounStore().getNoun("api_type").getNoun(0).getValue() + "";

		// need some way of figuring out what api engine to use to which one
		// for now I will just force fit
		IDatabaseEngine engine = null;

		if (apiType.equalsIgnoreCase("JSON")) {
			engine = new JsonAPIEngine();
		} else if (apiType.equalsIgnoreCase("JSON2")) {
			engine = new JsonAPIEngine2();
		} else if (apiType.equalsIgnoreCase("WEB")) {
			engine = new WebScrapeEngine();
		}
		
		// if there is alias get it
		if (this.getNounStore().getNoun("aliasFile") != null) {
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			String alias = this.getNounStore().getNoun("alias").getNoun(0).getValue() + "";
			// load the alias
			// as the properties
			// asset lib

			// this alias
			Properties aliasProp = getAlias(baseFolder + "/" + alias);

			if (engine != null) {
				engine.setSmssFilePath(baseFolder + "/" + alias);
				engine.setSmssProp(aliasProp);

				// make up an engine name
				String engineName = aliasProp.getProperty("engine_name");
				this.qs.setEngineId(engineName);
				this.qs.setEngine(engine);
			}
		} else if (this.getNounStore().getNoun("aliasMap") != null) {
			String source = "";
			String operation = "";
			// may be I can fit all these things into the alias map and call it a day.
			HashMap map = new HashMap();;

			// also need a headersmap
			if (this.getNounStore().getNoun("aliasMap") != null) {
				map = (HashMap) this.getNounStore().getNoun("aliasMap").getNoun(0).getValue();
			}
			
			if (this.getNounStore().getNoun("headersMap") != null) {
				map.put("HEADERS", this.getNounStore().getNoun("headersMap").getNoun(0).getValue());
			}
			
			Properties aliasProp = getAlias(null);
			Iterator keys = map.keySet().iterator();
			while (keys.hasNext()) {
				Object thisKey = keys.next();
				aliasProp.put(thisKey, map.get(thisKey));
			}

			// mandatory input is not there set it
			if (!aliasProp.containsKey("mandatory_input")) {
				aliasProp.put("mandatory_input", "");
			}
			
			if (engine != null) {
				engine.setSmssProp(aliasProp);
				String engineName = apiType + Utility.getRandomString(6);
				this.qs.setEngineId(engineName);
				this.qs.setEngine(engine);
				engine.setEngineId(engineName);
			}
		}
	}

	private Properties getAlias(String alias) {
		Properties retProp = new Properties();
		if (alias != null)
			// needs to implement this later
			retProp = Utility.loadProperties(alias);
		return new Properties();
	}
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		init();
		createQueryStructPlan();
		if(parentReactor != null) {
			// this is only called lazy
			// have to init to set the qs
			// to them add to the parent
			NounMetadata data = new NounMetadata(this.qs, PixelDataType.QUERY_STRUCT);
			if(parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
					|| parentReactor instanceof GenericReactor) {
				parentReactor.getCurRow().add(data);
			} else {
				GenRowStruct parentQSInput = parentReactor.getNounStore().makeNoun(PixelDataType.QUERY_STRUCT.getKey());
				parentQSInput.add(data);
			}
		}
	}

	private AbstractQueryStruct createQueryStructPlan() {
		// just loop through all the things
		Map<String, Object> configMap = new HashMap<>();
		for(String key : this.store.getNounKeys()) {
			 GenRowStruct grs = this.store.getNoun(key);
			 if(grs != null && !grs.isEmpty()) {
				 configMap.put(key, grs.get(0));
			 }
		}
		
		ConfigSelectQueryStruct qs = new ConfigSelectQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.DIRECT_API_QUERY);
		qs.setConfig(configMap);
		qs.setEngineId("FAKE_ENGINE");
		this.qs = qs;
		return this.qs;
	}
}
