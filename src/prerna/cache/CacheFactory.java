package prerna.cache;

@Deprecated
public class CacheFactory {

	private CacheFactory() {

	}

	public enum CACHE_TYPE {CSV_CACHE, DB_INSIGHT_CACHE};

	public static InsightCache getInsightCache(CACHE_TYPE cacheType) {
		InsightCache cache = null;

		switch(cacheType) {
		case CSV_CACHE : cache = CSVInsightCache.getInstance(); break;
		case DB_INSIGHT_CACHE : cache = DatabaseInsightCache.getInstance(); break;
		}

		return cache;
	}

}
