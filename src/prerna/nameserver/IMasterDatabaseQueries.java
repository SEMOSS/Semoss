/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.nameserver;

public interface IMasterDatabaseQueries {

	String MC_PARENT_CHILD_QUERY = "SELECT DISTINCT ?parentMC ?childMC WHERE { {?parentMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?parentMC <http://semoss.org/ontologies/Relation/ParentOf> ?childMC} }";
	String KEYWORD_NOUN_QUERY = "SELECT DISTINCT ?keyword ?mc WHERE { {?keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?mc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?mc} }";
	
	String GET_RELATED_KEYWORDS_AND_THEIR_NOUNS = "SELECT DISTINCT ?engine ?retKeywords ?retMC WHERE { BIND(<@KEYWORD@> AS ?Keyword) {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?Noun} {?Noun <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Noun <http://semoss.org/ontologies/Relation/HasTopHypernym> ?MC} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://semoss.org/ontologies/Relation/ParentOf>+ ?childMC} {?retKeywords <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?retKeywords <http://semoss.org/ontologies/Relation/ComposedOf> ?childMC} {?retMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?retKeywords <http://semoss.org/ontologies/Relation/ComposedOf> ?retMC} {?engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?engine <http://semoss.org/ontologies/Relation/Has> ?retKeywords} }";
	String GET_RELATED_KEYWORDS_TO_SET_AND_THEIR_NOUNS = "SELECT DISTINCT ?engine ?retKeywords ?retMC WHERE { {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?Noun} {?Noun <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Noun <http://semoss.org/ontologies/Relation/HasTopHypernym> ?MC} {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://semoss.org/ontologies/Relation/ParentOf>+ ?childMC} {?retKeywords <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?retKeywords <http://semoss.org/ontologies/Relation/ComposedOf> ?childMC} {?retMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?retKeywords <http://semoss.org/ontologies/Relation/ComposedOf> ?retMC} {?engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?engine <http://semoss.org/ontologies/Relation/Has> ?retKeywords} } BINDINGS ?Keyword {@BINDINGS@}";
	
	String GET_INSIGHTS_FOR_KEYWORDS = "SELECT DISTINCT ?Engine ?InsightLabel ?Keyword ?PerspectiveLabel ?Viz WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?Perspective} {?Perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?Insight} {?Insight <INSIGHT:PARAM> ?Param}{?Param <PARAM:TYPE> ?Type} {?Keyword <http://semoss.org/ontologies/Relation/Has> ?Type}{?Perspective <http://semoss.org/ontologies/Relation/Contains/Label> ?PerspectiveLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Label> ?InsightLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?Viz}} BINDINGS ?Keyword {@KEYWORDS@}";
	String GET_ALL_INSIGHTS = "SELECT DISTINCT ?Engine ?InsightLabel ?Keyword ?PerspectiveLabel ?Viz WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?Perspective} {?Perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?Insight} {?Insight <INSIGHT:PARAM> ?Param}{?Param <PARAM:TYPE> ?Type} {?Keyword <http://semoss.org/ontologies/Relation/Has> ?Type}{?Perspective <http://semoss.org/ontologies/Relation/Contains/Label> ?PerspectiveLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Label> ?InsightLabel}{?Insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?Viz}} ";

	String GET_ENGINE_CONCEPTS_AND_SAMPLE_INSTANCE = "SELECT DISTINCT ?concept (SAMPLE(?instance) AS ?example) WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} } GROUP BY ?concept";
	
	String INSTANCE_EXISTS_QUERY = "SELECT DISTINCT ?keyword ?s WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?keyword ;} } BINDINGS ?s {@BINDINGS@}";
	String ENGINE_API_QUERY = "SELECT DISTINCT ?Engine ?API WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Contains/API> ?API}}";
	String ENGINE_LIST_QUERY = "SELECT DISTINCT ?Engine ?API WHERE { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} }";
	
	String GET_ALL_KEYWORDS = "SELECT DISTINCT ?keyword WHERE { {?keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} }";
	String GET_ALL_MASTER_CONCEPTS = "SELECT DISTINCT ?mc WHERE { {?mc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} }";
	
	String GET_ALL_KEYWORDS_FROM_MC_List = "SELECT DISTINCT ?Keyword WHERE { { {?childMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MC <http://semoss.org/ontologies/Relation/ParentOf>+ ?childMC} {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?childMC} } UNION { {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?MC} } } BINDINGS ?MC {@BINDINGS@}";
	
	//queries for relations to delete
	String KEYWORDS_QUERY = "SELECT DISTINCT ?Keyword WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine ?p ?Keyword}}";
	String API_QUERY = "SELECT DISTINCT ?API WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Contains/API> ?API}}";

	String PERSPECTIVES_QUERY = "SELECT DISTINCT ?Perspective WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>}{?Engine ?p ?Perspective}}";
	String INSIGHTS_QUERY = "SELECT DISTINCT ?Insight WHERE { BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>}{?Engine ?p ?Insight}}";

	//queries to clean up the keywords after engines deleted
	String KEYWORDS_WITHOUT_ENGINES_QUERY = "SELECT DISTINCT ?Keyword WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} OPTIONAL{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}FILTER(!BOUND(?Engine))}";
	String MC_KEYWORDS_QUERY = "SELECT DISTINCT ?MasterConcept ?Keyword WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?MasterConcept}} BINDINGS ?Keyword {@BINDINGS@}";
	String KEYWORDS_TYPE_QUERY = "SELECT DISTINCT ?Keyword ?Type WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Keyword <http://semoss.org/ontologies/Relation/Has> ?Type}} BINDINGS ?Keyword {@BINDINGS@}";

	//queries to clean up MCs on a deep clean
	String MCS_WITHOUT_KEYWORDS_QUERY = "SELECT DISTINCT ?MC WHERE { {?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}MINUS{SELECT DISTINCT ?MC WHERE{{?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {{?childMC  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MC <http://semoss.org/ontologies/Relation/ParentOf>+ ?childMC}{?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?childMC}}UNION{{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?Keyword <http://semoss.org/ontologies/Relation/ComposedOf> ?MC}}}}}";
	String PARENT_CHILD_MC_QUERY = "SELECT DISTINCT ?ParentMC ?ChildMC WHERE {{?ParentMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?ChildMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?ParentMC <http://semoss.org/ontologies/Relation/ParentOf> ?ChildMC}} BINDINGS ?ChildMC {@BINDINGS@}";
	String MC_TOP_HYPERNYM_MC_QUERY = "SELECT DISTINCT ?MC ?TopHypernymMC WHERE {{?MC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?TopHypernymMC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MC <http://semoss.org/ontologies/Relation/HasTopHypernym> ?TopHypernymMC}} BINDINGS ?MC {@BINDINGS@}";

}
