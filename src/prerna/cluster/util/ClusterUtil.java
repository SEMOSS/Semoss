package prerna.cluster.util;

public class ClusterUtil {

	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY)
			? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY)) : true;
	
}
