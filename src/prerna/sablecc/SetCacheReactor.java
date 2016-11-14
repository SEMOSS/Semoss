package prerna.sablecc;

import java.util.Iterator;

import prerna.cache.CacheFactory;
import prerna.cache.CacheFactory.CACHE_TYPE;

public class SetCacheReactor extends AbstractReactor{

	public SetCacheReactor() {
		String[] thisReacts = { "CACHE_SETTING" }; // these are the input
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.USE_CACHE;
	}
	
	@Override
	public Iterator process() {	
		String cacheSetting = myStore.get("CACHE_SETTING").toString().trim().toUpperCase();
		boolean setting = cacheSetting.contains("TRUE");
		CacheFactory.getInsightCache(CACHE_TYPE.DB_INSIGHT_CACHE).setCacheMode(setting);
		return null;
	}

}
