package prerna.sablecc;

import java.util.Iterator;

import prerna.cache.CacheFactory;
import prerna.cache.DatabaseInsightCache;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

public class CacheReactor extends AbstractReactor {

	public CacheReactor() {
		
	}

	@Override
	public Iterator process() {
		String dbName = (String)myStore.get("ENGINE_NAME");
		String insightID = (String)myStore.get("ENGINE_ID");

		//delete all cache
		if(dbName == null) {
			CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).deleteAllCache();
		} 
		
		//delete the cache for a particular db
		else if(dbName != null && insightID == null) {
			DatabaseInsightCache dbCache = (DatabaseInsightCache)CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE);
			dbCache.deleteDBCache(dbName);
		} 
		
		//delete the cache for a particular insight
		else {
			IEngine engine = Utility.getEngine(dbName);
			Insight insightObj = ((AbstractEngine)engine).getInsight(insightID).get(0);
			CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).deleteInsightCache(insightObj);
		}
		
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
