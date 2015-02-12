package prerna.nameserver;

import prerna.util.Constants;

public interface IMasterDatabaseURIs {

	String SEMOSS_URI = "http://semoss.org/ontologies";
	String SEMOSS_CONCEPT_URI = SEMOSS_URI + "/" + Constants.DEFAULT_NODE_CLASS;
	String SEMOSS_RELATION_URI = SEMOSS_URI + "/" + Constants.DEFAULT_RELATION_CLASS;
	String PROP_URI = SEMOSS_RELATION_URI + "/Contains";

	String RESOURCE_URI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	String MC_BASE_URI = SEMOSS_CONCEPT_URI+"/MasterConcept";
	String KEYWORD_BASE_URI = SEMOSS_CONCEPT_URI+"/Keyword";
	String ENGINE_BASE_URI = SEMOSS_CONCEPT_URI+"/Engine";
	
	String USER_BASE_URI = SEMOSS_CONCEPT_URI + "/User";
	String USER_INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/UserInsight";
	String INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/Insight";
	String PERSPECTIVE_BASE_URI = SEMOSS_CONCEPT_URI+"/Perspective";
	String ENGINE_KEYWORD_BASE_URI = SEMOSS_RELATION_URI + "/Has";
}
