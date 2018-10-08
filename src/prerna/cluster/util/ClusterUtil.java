package prerna.cluster.util;

public class ClusterUtil {

	// Env vars used in clustered deployments
	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY)
			? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY)) : true;
	
	private static final String SYNC_ENGINES_FROM_REMOTE_STORAGE_KEY = "SEMOSS_SYNC_ENGINES_FROM_REMOTE_STORAGE";
	public static final boolean SYNC_ENGINES_FROM_REMOTE_STORAGE = System.getenv().containsKey(SYNC_ENGINES_FROM_REMOTE_STORAGE_KEY)
			? Boolean.parseBoolean(System.getenv(SYNC_ENGINES_FROM_REMOTE_STORAGE_KEY)) : false;
	
}
