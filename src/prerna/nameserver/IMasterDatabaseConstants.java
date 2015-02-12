package prerna.nameserver;

public interface IMasterDatabaseConstants {

	// similarity cutoff value
	double similarityCutOff = 0.20;
	double mainNounWeight = 0.8;
	double otherNounWeight = 0.2;
	
	// keys for passing insights
	String DB_KEY = "database";
	String SCORE_KEY = "similarityScore";
	String QUESITON_KEY = "question";
	String TYPE_KEY = "type";
	String PERSPECTIVE_KEY = "perspective";
	String INSTANCE_KEY = "instances";
	String VIZ_TYPE_KEY = "viz";
	String ENGINE_URI_KEY = "engineURI";
}
