package prerna.nameserver;

import prerna.util.Constants;

public interface IMasterDatabase {

	// useful queries
	String MC_PARENT_CHILD_QUERY = "SELECT DISTINCT ?parentMC ?childMC WHERE { {?parentMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?parentMC <http://semoss.org/ontologies/Relation/ParentOf> ?childMC} }";
	String KEYWORD_NOUN_QUERY = "SELECT DISTINCT ?keyword ?mc WHERE { {?keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?mc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?mc} }";
	
	String GET_RELATED_KEYWORDS_AND_THEIR_NOUNS = "SELECT DISTINCT ?engine ?retKeywords ?childMC WHERE { BIND(<@KEYWORD@> AS ?Keyword) {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?Noun} {?Noun <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Noun <http://semoss.org/ontologies/Relation/HasTopHypernym> ?MC} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://semoss.org/ontologies/Relation/ParentOf>+ ?childMC} {?retKeywords <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?retKeywords <http://semoss.org/ontologies/Relation/ComposedOf> ?childMC} {?engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?engine <http://semoss.org/ontologies/Relation/Has> ?retKeywords} }";
	String GET_INSIGHTS_FOR_KEYWORDS = "SELECT DISTINCT ?Engine ?InsightLabel ?Keyword ?PerspectiveLabel ?Viz WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?Perspective} {?Perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?Insight} {?Insight <INSIGHT:PARAM> ?Param}{?Param <PARAM:TYPE> ?Type} {?Keyword <http://semoss.org/ontologies/Relation/Has> ?Type}{?Perspective <http://semoss.org/ontologies/Relation/Contains/Label> ?PerspectiveLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Label> ?InsightLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?Viz}} BINDINGS ?Keyword {@KEYWORDS@}";
	
	String INSTANCE_EXISTS_QUERY = "SELECT DISTINCT ?keyword ?s WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?keyword ;} } BINDINGS ?s {@BINDINGS@}";
	String ENGINE_API_QUERY = "SELECT DISTINCT ?Engine ?API WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Contains/API> ?API}}";

	//uri variables
	String SEMOSS_URI = "http://semoss.org/ontologies";
	String SEMOSS_CONCEPT_URI = SEMOSS_URI + "/" + Constants.DEFAULT_NODE_CLASS;
	String SEMOSS_RELATION_URI = SEMOSS_URI + "/" + Constants.DEFAULT_RELATION_CLASS;
	String PROP_URI = SEMOSS_RELATION_URI + "/" + "Contains";

	String RESOURCE_URI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	String MC_BASE_URI = SEMOSS_CONCEPT_URI+"/MasterConcept";
	String KEYWORD_BASE_URI = SEMOSS_CONCEPT_URI+"/Keyword";
	String ENGINE_BASE_URI = SEMOSS_CONCEPT_URI+"/Engine";
	
	String USER_BASE_URI = SEMOSS_CONCEPT_URI + "/" + "User";
	String USER_INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/" + "UserInsight";
	String INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/" + "Insight";
	String PERSPECTIVE_BASE_URI = SEMOSS_CONCEPT_URI+"/Perspective";
	String ENGINE_KEYWORD_BASE_URI = SEMOSS_RELATION_URI + "/Has";
	
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
