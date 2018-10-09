package prerna.cluster.util;

public class ClusterUtil {

	// Env vars used in clustered deployments
	// TODO >>>timb: make sure that everything cluster related starts with this, also introduces tracibility
	private static final String IS_CLUSTER_KEY = "SEMOSS_IS_CLUSTER";
	public static final boolean IS_CLUSTER = System.getenv().containsKey(IS_CLUSTER_KEY)
			? Boolean.parseBoolean(System.getenv(IS_CLUSTER_KEY)) : false;
	
	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY)
			? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY)) : true;
		
}
