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
package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;

public final class DHMSMTransitionUtility {


	private DHMSMTransitionUtility() {

	}

	public static final String HEADER_KEY = "headers";
	public static final String DATA_KEY = "data";
	public static final String TOTAL_DIRECT_COST_KEY = "directCost";
	public static final String TOTAL_INDIRECT_COST_KEY = "indirectCost";
	public static final String SYS_URI_PREFIX = "http://health.mil/ontologies/Concept/System/";

	public static final String LOE_SYS_GLITEM_QUERY = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?sys ?data ?ser ?gltag1";
	public static final String LOE_GENERIC_GLITEM_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?Loe) WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } GROUP BY ?data ?ser";
	public static final String AVG_LOE_SYS_GLITEM_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe)/COUNT(DISTINCT ?sys) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1";
	public static final String SERVICE_TO_DATA_LIST_QUERY = "SELECT DISTINCT ?data (GROUP_CONCAT(?Ser; SEPARATOR = '; ') AS ?service) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data } BIND(SUBSTR(STR(?ser),46) AS ?Ser) } GROUP BY ?data";
	public static final String DATA_TO_SERVICE_QUERY = "SELECT DISTINCT ?data  ?Ser WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data } BIND(SUBSTR(STR(?ser),46) AS ?Ser) }";

	public static final String SYS_SPECIFIC_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?sys ?data ?ser ?loe ?gltag ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } ORDER BY ?sys ?ser ?gltag1";
	public static final String GENERIC_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?data ?ser ?loe ?phase WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} }";
	public static final String AVERAGE_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe)/COUNT(DISTINCT ?sys) AS ?Loe) ?gltag ?phase WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe ?gltag ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag ?phase";

	public static final String SYS_SPECIFIC_LOE_AND_PHASE_AVG_SERVICE_QUERY = "SELECT DISTINCT ?sys ?data (AVG(?loe) AS ?loe) ?gltag ?phase WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe ?gltag ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } ORDER BY ?sys ?ser ?gltag1 } GROUP BY ?sys ?data ?gltag ?phase";
	public static final String AVERAGE_LOE_AND_PHASE_AVG_SERVICE_QUERY = "SELECT DISTINCT ?data (AVG(?Loe) AS ?Loe) ?gltag ?phase WHERE { SELECT DISTINCT ?data ?ser (SUM(?loe)/COUNT(DISTINCT ?sys) AS ?Loe) ?gltag ?phase WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe ?gltag ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag ?phase } GROUP BY ?data ?gltag ?phase";
	public static final String LOE_GENERIC_AND_PHASE_AVG_SERVICE_QUERY = "SELECT DISTINCT ?data (AVG(?loe) as ?avgLoe) ?phase WHERE { SELECT DISTINCT ?data ?ser ?loe ?phase WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?phase";

	public static final String DHMSM_SOR_QUERY = "SELECT DISTINCT ?Data WHERE { BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} }";

	public static final String LPI_SYS_QUERY = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?entity <http://semoss.org/ontologies/Relation/Contains/Device> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'}{?entity <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) }";
	public static final String SYS_TYPE_QUERY = "SELECT DISTINCT ?entity ?Disposition WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Device> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Disposition> ?Disposition}{?entity <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) }";

	//	public static final String SYS_SOR_DATA_CONCAT_QUERY = "SELECT DISTINCT (CONCAT(STR(?system), STR(?data)) AS ?sysDataKey) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?data ?system";
	public static final String SYS_SOR_DATA_CONCAT_QUERY = "SELECT DISTINCT (CONCAT(STR(?System), STR(?Data)) AS ?sysDataKey) WHERE { SELECT DISTINCT (SUBSTR(STR(?system),45) AS ?System) (SUBSTR(STR(?data),49) AS ?Data) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?Data ?System }";
	public static final String ALL_SELF_REPORTED_ICD_QUERY = "SELECT DISTINCT ?ICD WHERE { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Carries ?Data} {?Carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> 'Self_Reported'} }";
	public static final String ALL_SELF_REPORTED_SYSTEMS = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?System <http://semoss.org/ontologies/Relation/Provide> ?ICD} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Carries ?Data} {?Carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> 'Self_Reported'} } UNION { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Carries ?Data} {?Carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> 'Self_Reported'} } }";

//	public static final String DETERMINE_PROVIDER_FUTURE_ICD_PROPERTIES = "SELECT DISTINCT ?System ?Data (GROUP_CONCAT(DISTINCT ?format; SEPARATOR = ';') AS ?Format) (GROUP_CONCAT(DISTINCT ?frequency; SEPARATOR = ';') AS ?Frequency) (GROUP_CONCAT(DISTINCT ?protocol; SEPARATOR = ';') AS ?Protocol) WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?System <http://semoss.org/ontologies/Relation/Provide> ?ICD} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ICD ?carries ?Data} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol} } GROUP BY ?System ?Data ORDER BY ?System ?Data";
//	public static final String DETERMINE_CONSUMER_FUTURE_ICD_PROPERTIES = "SELECT DISTINCT ?System ?Data (GROUP_CONCAT(DISTINCT ?format; SEPARATOR = ';') AS ?Format) (GROUP_CONCAT(DISTINCT ?frequency; SEPARATOR = ';') AS ?Frequency) (GROUP_CONCAT(DISTINCT ?protocol; SEPARATOR = ';') AS ?Protocol) WHERE{ {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ICD ?carries ?Data} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol} } GROUP BY ?System ?Data ORDER BY ?System ?Data";

	public static final String DETERMINE_CONSUMER_FUTURE_ICD_FREQUENCY = "SELECT DISTINCT ?System ?Data ?DFreq WHERE { {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?Data} {?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?System} OPTIONAL { {?DFreq <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq>} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?DFreq} } }";	
	public static final String DETERMINE_PROVIDER_FUTURE_ICD_FREQUENCY = "SELECT DISTINCT ?System ?Data ?DFreq WHERE { {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?SystemInterface <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface} OPTIONAL { {?DFreq <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq>} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?DFreq} } }";

	public static final String GET_EXISTING_ICD_RELS = "SELECT DISTINCT ?SystemInterface ?DForm ?DProt ?DFreq WHERE { BIND(<@SYSTEM_INTERFACE@> AS ?SystemInterface) {?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?DProt <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt>} {?DForm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm>} {?DFreq <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq>} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?DProt} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?DForm} {?SystemInterface <http://semoss.org/ontologies/Relation/Has> ?DFreq} }";
	
	public static final String SELF_REPORTED_SYSTEM_P2P_INTERFACE_COST = "SELECT DISTINCT ?System ?Data ?GLTag (SUM(?loe) AS ?Cost) WHERE { {?System a <http://semoss.org/ontologies/Concept/System>} {?GLItem a <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?System <http://semoss.org/ontologies/Relation/Influences> ?GLItem} {?GLItem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?Data a <http://semoss.org/ontologies/Concept/DataObject>} {?Data <http://semoss.org/ontologies/Relation/Input> ?GLItem} {?GLTag a <http://semoss.org/ontologies/Concept/GLTag>} {?GLItem <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} } GROUP BY ?System ?Data ?GLTag";
	public static final String SELF_REPORTED_SYSTEM_P2P_INTERFACE_COST_BY_TAG_AND_PHASE = "SELECT DISTINCT ?System ?Data ?GLTag ?Phase (SUM(?loe) AS ?Cost) WHERE { {?System a <http://semoss.org/ontologies/Concept/System>} {?GLItem a <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?System <http://semoss.org/ontologies/Relation/Influences> ?GLItem} {?GLItem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?Phase a <http://semoss.org/ontologies/Concept/SDLCPhase>} {?GLItem <http://semoss.org/ontologies/Relation/BelongsTo> ?Phase} {?GLTag a <http://semoss.org/ontologies/Concept/GLTag>} {?GLItem <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?Data a <http://semoss.org/ontologies/Concept/DataObject>} {?Data <http://semoss.org/ontologies/Relation/Input> ?GLItem} } GROUP BY ?System ?GLTag ?Data ?Phase";
	
	
	public static HashMap<String, ArrayList<String>> getDataToServiceHash(IDatabase engine) {
		HashMap<String, ArrayList<String>> retHash = new HashMap<String, ArrayList<String>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, DATA_TO_SERVICE_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(varName[0]).toString();
			String ser = sjss.getVar(varName[1]).toString();

			ArrayList<String> innerList;
			if(retHash.containsKey(data)) {
				innerList = retHash.get(data);
				innerList.add(ser);
			} else {
				innerList = new ArrayList<String>();
				innerList.add(ser);
				retHash.put(data, innerList);
			}
		}

		return retHash;
	}

	public static Map<String, Map<String, Map<String, Double>>> getSystemSelfReportedP2PCostByTagAndPhase(IDatabase futureCostEngine, IDatabase costEngine) {
		Map<String, Map<String, Double>> genericCosts = getGenericGLItemAndPhaseByAvgServ(costEngine);
		Map<String, Map<String, Map<String, Double>>> retHash = new HashMap<String, Map<String, Map<String, Double>>>();
		
		Set<String> addedDeployCost = new HashSet<String>();
		
		ISelectWrapper sjsw = Utility.processQuery(futureCostEngine, SELF_REPORTED_SYSTEM_P2P_INTERFACE_COST_BY_TAG_AND_PHASE);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getVar(varName[0]).toString();
			String data = sjss.getVar(varName[1]).toString();
			String glTag = sjss.getVar(varName[2]).toString();
			String phase = sjss.getVar(varName[3]).toString();
			Double p2pCost = (Double) sjss.getVar(varName[4]);

			Map<String, Map<String, Double>> innerMap1 = new HashMap<String, Map<String, Double>>();
			Map<String, Double> innerMap2 = new HashMap<String, Double>();
			if(retHash.containsKey(system)) {
				innerMap1 = retHash.get(system);
				if(innerMap1.containsKey(glTag)) {
					innerMap2 = innerMap1.get(glTag);
					if(innerMap2.containsKey(phase)) {
						double currCost = innerMap2.get(phase);
						currCost += p2pCost;
						innerMap2.put(phase, currCost);
					} else {
						innerMap2.put(phase, p2pCost);
					}
				} else {
					innerMap2.put(phase, p2pCost);
					innerMap1.put(glTag, innerMap2);
				}
			} else {
				innerMap2.put(phase, p2pCost);
				innerMap1.put(glTag, innerMap2);
				retHash.put(system, innerMap1);
			}
			
			if(glTag.equals("Provider")) {
				Map<String, Double> genericHash = genericCosts.get(data);
				double newLOE = genericHash.get(phase);
				newLOE += innerMap2.get(phase);
				innerMap2.put(phase, newLOE);
				
				if(!addedDeployCost.contains(system)) {
					String deployPhase = "Deploy";
					addedDeployCost.add(system);
					double deployLOE = genericHash.get(deployPhase);
					innerMap2.put(deployPhase, deployLOE);
				}
			}
		}

		return retHash;
	}

	public static Map<String, Double> getSystemSelfReportedP2PCost(IDatabase futureCostEngine, IDatabase costEngine) {
		Map<String, Map<String, Double>> genericCosts = getGenericGLItemAndPhaseByAvgServ(costEngine);
		
		Map<String, Double> retHash = new HashMap<String, Double>();
		ISelectWrapper sjsw = Utility.processQuery(futureCostEngine, SELF_REPORTED_SYSTEM_P2P_INTERFACE_COST);

		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getVar(varName[0]).toString();
			
			if(system.equals("AERO")) {
				System.out.println("");
			}
			String data = sjss.getVar(varName[1]).toString();
			String glTag = sjss.getVar(varName[2]).toString();
			Double p2pCost = (Double) sjss.getVar(varName[3]);
			
			if(glTag.equals("Provider")) {
				Map<String, Double> genericHash = genericCosts.get(data);
				for(String genPhase : genericHash.keySet()) {
					double genLOE = genericHash.get(genPhase);
					p2pCost += genLOE;
				}
			}
			
			if(retHash.containsKey(system)) {
				double currLOE = retHash.get(system);
				currLOE += p2pCost;
				retHash.put(system, currLOE);
			} else {
				retHash.put(system, p2pCost);
			}
		}

		return retHash;
	}

//	public static Map<String, Map<String, String[]>> getProviderFutureICDProperties(IDatabase engine) {
//		return runFutureICDProp(engine, DETERMINE_PROVIDER_FUTURE_ICD_PROPERTIES);
//	}
//
//	public static Map<String, Map<String, String[]>> getConsumerFutureICDProperties(IDatabase engine) {
//		return runFutureICDProp(engine, DETERMINE_CONSUMER_FUTURE_ICD_PROPERTIES);
//	}
//	
//	public static Map<String, Map<String, String[]>> runFutureICDProp(IDatabase engine, String query) {
//		Map<String, Map<String, String[]>> retHash = new HashMap<String, Map<String, String[]>>();
//
//		ISelectWrapper sjsw = Utility.processQuery(engine, query);
//		String[] varName = sjsw.getVariables();
//		while(sjsw.hasNext()) {
//			ISelectStatement sjss = sjsw.next();
//			String system = sjss.getVar(varName[0]).toString();
//			String data = sjss.getVar(varName[1]).toString();
//			String format = sjss.getVar(varName[2]).toString();
//			String frequency = sjss.getVar(varName[3]).toString();
//			String protocol = sjss.getVar(varName[4]).toString();
//
//			frequency = determineHighestFrequency(frequency);
//
//			String[] valArr = new String[]{format, frequency, protocol};
//			Map<String, String[]> innerHash = new HashMap<String, String[]>();
//			if(retHash.containsKey(system)) {
//				innerHash = retHash.get(system);
//				innerHash.put(data, valArr);
//			} else {
//				innerHash.put(data, valArr);
//				retHash.put(system, innerHash);
//			}
//		}
//
//		return retHash;
//	}
//	
//	private static String determineHighestFrequency(String frequencyConcat) {
//		if(frequencyConcat.contains("Real-time (user-initiated)")) return "Real-time (user-initiated)";
//		else if(frequencyConcat.contains("Real-time")) return "Real-time";
//		else if(frequencyConcat.contains("Real-time ")) return "Real-time ";
//		else if(frequencyConcat.contains("Real Time")) return "Real Time";
//		else if(frequencyConcat.contains("Transactional")) return "Transactional";
//		else if(frequencyConcat.contains("On Demand")) return "On Demand";
//		else if(frequencyConcat.contains("TheaterFramework")) return "TheaterFramework";
//		else if(frequencyConcat.contains("Event Driven (Seconds)")) return "Event Driven (Seconds)";
//		else if(frequencyConcat.contains("Web services")) return "Web services";
//		else if(frequencyConcat.contains("TF")) return "TF";
//		else if(frequencyConcat.contains("SFTP")) return "SFTP";
//		else if(frequencyConcat.contains("Near Real-time")) return "Near Real-time";
//		else if(frequencyConcat.contains("Batch")) return "Batch";
//		else if(frequencyConcat.contains("TCP")) return "TCP";
//		else if(frequencyConcat.contains("Interactive")) return "Interactive";
//		else if(frequencyConcat.contains("NFS, Oracle connection")) return "NFS, Oracle connection";
//		else if(frequencyConcat.contains("Near Real-time (transaction initiated)")) return "Near Real-time (transaction initiated)";
//		else if(frequencyConcat.contains("Periodic")) return "Periodic";
//		else if(frequencyConcat.contains("interactive")) return "interactive";
//		else if(frequencyConcat.contains("On demad")) return "On demad";
//		else if(frequencyConcat.contains("On-demand")) return "On-demand";
//		else if(frequencyConcat.contains("user upload")) return "user upload";
//		else if(frequencyConcat.contains("Each user login instance")) return "Each user login instance";
//		else if(frequencyConcat.contains("DVD")) return "DVD";
//
//		else if(frequencyConcat.contains("1/hour (KML)/On demand (HTML)")) return "1/hour (KML)/On demand (HTML)";
//		else if(frequencyConcat.contains("event-driven (Minutes-hours)")) return "event-driven (Minutes-hours)";
//		else if(frequencyConcat.contains("Event Driven (Minutes-hours)")) return "Event Driven (Minutes-hours)";
//		else if(frequencyConcat.contains("Hourly")) return "Hourly";
//
//		else if(frequencyConcat.contains("Batch (12/day)")) return "Batch (12/day)";
//
//		else if(frequencyConcat.contains("Batch (4/day)")) return "Batch (4/day)";
//
//		else if(frequencyConcat.contains("Every 8 hours (KML)/On demand (HTML)")) return "Every 8 hours (KML)/On demand (HTML)";
//
//		else if(frequencyConcat.contains("daily")) return "daily";
//		else if(frequencyConcat.contains("Daily")) return "Daily";
//		else if(frequencyConcat.contains("Batch (daily)")) return "Batch (daily)";
//		else if(frequencyConcat.contains("Batch(Daily)")) return "Batch(Daily)";
//		else if(frequencyConcat.contains("Daily at end of day")) return "Daily at end of day";
//		else if(frequencyConcat.contains("Daily Interactive")) return "Daily Interactive";
//
//		else if(frequencyConcat.contains("Batch (three times a week)")) return "Batch (three times a week)";
//
//		else if(frequencyConcat.contains("Event Driven (seconds-minutes)")) return "Event Driven (seconds-minutes)";
//
//		else if(frequencyConcat.contains("Batch (weekly)")) return "Batch (weekly)";
//		else if(frequencyConcat.contains("Batch(Weekly)")) return "Batch(Weekly)";
//		else if(frequencyConcat.contains("Weekly")) return "Weekly";
//		else if(frequencyConcat.contains("Weekly ")) return "Weekly ";
//		else if(frequencyConcat.contains("Weekly Daily")) return "Weekly Daily";
//		else if(frequencyConcat.contains("Weekly; Interactive; Interactive")) return "Weekly; Interactive; Interactive";
//		else if(frequencyConcat.contains("Weekly Daily Weekly Weekly Weekly Weekly Daily Daily Daily")) return "Weekly Daily Weekly Weekly Weekly Weekly Daily Daily Daily";
//
//		else if(frequencyConcat.contains("Bi-Weekly")) return "Bi-Weekly";
//
//		else if(frequencyConcat.contains("Batch (twice monthly)")) return "Batch (twice monthly)";
//
//		else if(frequencyConcat.contains("Monthly")) return "Monthly";
//		else if(frequencyConcat.contains("Batch (monthly)")) return "Batch (monthly)";
//		else if(frequencyConcat.contains("Batch(Monthly)")) return "Batch(Monthly)";
//		else if(frequencyConcat.contains("Batch(Daily/Monthly)")) return "Batch(Daily/Monthly)";
//		else if(frequencyConcat.contains("Monthly at beginning of month, or as user initiated")) return "Monthly at beginning of month, or as user initiated";
//		else if(frequencyConcat.contains("Monthly Bi-Monthly Weekly Weekly")) return "Monthly Bi-Monthly Weekly Weekly";
//
//		else if(frequencyConcat.contains("Batch (bi-monthly)")) return "Batch (bi-monthly)";
//
//		else if(frequencyConcat.contains("Quarterly")) return "Quarterly";
//		else if(frequencyConcat.contains("Batch (quarterly)")) return "Batch (quarterly)";
//		else if(frequencyConcat.contains("Batch(Quarterly)")) return "Batch(Quarterly)";
//		else if(frequencyConcat.contains("Weekly Quarterly")) return "Weekly Quarterly";
//
//		else if(frequencyConcat.contains("Batch (semiannually)")) return "Batch (semiannually)";
//
//		else if(frequencyConcat.contains("Batch (yearly)")) return "Batch (yearly)";
//		else if(frequencyConcat.contains("Annually")) return "Annually";
//		else if(frequencyConcat.contains("Annual")) return "Annual";
//
//		return frequencyConcat;
//	}
	
	
	public static Map<String, Map<String, String>> getProviderFutureICDFrequency(IDatabase engine) {
		return runFutureICDFreq(engine, DETERMINE_PROVIDER_FUTURE_ICD_FREQUENCY);
	}

	public static Map<String, Map<String, String>> getConsumerFutureICDFrequency(IDatabase engine) {
		return runFutureICDFreq(engine, DETERMINE_CONSUMER_FUTURE_ICD_FREQUENCY);
	}
	
	public static Map<String, Map<String, String>> runFutureICDFreq(IDatabase engine, String query) {
		/*
		 * This method is used to get the highest frequency for a given system with a data object
		 * 
		 * This is using the assumption that if DHMSM/MHS_Genesis is the SOR for these data objects
		 * Then whatever the highest frequency is required for the system and the given data object
		 * across all of it system interfaces should also be the rate of synchronization 
		 * with DHMSM/MHS_Genesis
		 */
		
		Map<String, Map<String, String>> retHash = new Hashtable<String, Map<String, String>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getVar(varName[0]).toString();
			String data = sjss.getVar(varName[1]).toString();
			String frequency = sjss.getVar(varName[2]).toString();

			Map<String, String> dataHash = new HashMap<String, String>();
			if(retHash.containsKey(system)) {
				dataHash = retHash.get(system);
				
				if(dataHash.containsKey(data)) {
					String existingFreq = dataHash.get(data);
					// need to compare the current frequency and the new one they want
					if(frequency.equalsIgnoreCase("Real-Time") || existingFreq.equalsIgnoreCase("Real-Time")) {
						dataHash.put(data, "Real-Time");
					} else if(frequency.equalsIgnoreCase("On_Demand") || existingFreq.equalsIgnoreCase("On_Demand")) {
						dataHash.put(data, "On_Demand");
					} else if(frequency.equalsIgnoreCase("Daily") || existingFreq.equalsIgnoreCase("Daily")) {
						dataHash.put(data, "Daily");
					} else if(frequency.equalsIgnoreCase("Weekly") || existingFreq.equalsIgnoreCase("Weekly")) {
						dataHash.put(data, "Weekly");
					} else if(frequency.equalsIgnoreCase("Monthly") || existingFreq.equalsIgnoreCase("Monthly")) {
						dataHash.put(data, "Monthly");
					} else if(frequency.equalsIgnoreCase("Quarterly") || existingFreq.equalsIgnoreCase("Quarterly")) {
						dataHash.put(data, "Quarterly");
					} else if(frequency.equalsIgnoreCase("Yearly") || existingFreq.equalsIgnoreCase("Yearly")) {
						dataHash.put(data, "Yearly");
					} else if(frequency.equalsIgnoreCase("TBD") || existingFreq.equalsIgnoreCase("TBD")) {
						dataHash.put(data, "TBD");
					} else {
						// default is to use the TBD
						dataHash.put(data, "TBD");
					}
				} else {
					// nothing to compare, just put it in there
					// but if it does not have a frequency, put in TBD
					if(frequency == null || frequency.isEmpty()) {
						frequency = "TBD";
					}
					dataHash.put(data, frequency);
				}
			} else {
				// nothing to compare, just put it in there
				// but if it does not have a frequency, put in TBD
				if(frequency == null || frequency.isEmpty()) {
					frequency = "TBD";
				}
				dataHash.put(data, frequency);
				retHash.put(system, dataHash);
			}
		}

		return retHash;
	}
	
	/*
	 * Return the DForm, DProt, and DFreq for a given icd
	 */
	public static String[] getExistingIcdRelUris(IDatabase engine, String systemInterfaceUri) {
		String[] values = new String[3];
		
		ISelectWrapper sjsw = Utility.processQuery(engine, GET_EXISTING_ICD_RELS.replace("@SYSTEM_INTERFACE@", systemInterfaceUri));
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String icd = sjss.getRawVar(varName[0]).toString();
			String format = sjss.getRawVar(varName[1]).toString();
			String protocol = sjss.getRawVar(varName[2]).toString();
			String frequency = sjss.getRawVar(varName[3]).toString();
			
			values[0] = format;
			values[1] = protocol;
			values[2] = frequency;
		}
			
		return values;
	}

	public static Set<String> getAllSelfReportedSystemURIs(IDatabase engine) {
		Set<String> retList = new HashSet<String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, ALL_SELF_REPORTED_SYSTEMS);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getRawVar(varName[0]).toString();
			if(!system.endsWith("MHS_GENESIS")) // note this is a URI
				retList.add(system);
		}
		return retList;
	}

	public static Set<String> getAllSelfReportedSystemNames(IDatabase engine) {
		Set<String> retList = new HashSet<String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, ALL_SELF_REPORTED_SYSTEMS);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String system = sjss.getVar(varName[0]).toString();
			if(!system.equals("MHS_GENESIS"))
				retList.add(system);
		}
		return retList;
	}

	public static Set<String> getAllSelfReportedICDs(IDatabase engine) {
		Set<String> retList = new HashSet<String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, ALL_SELF_REPORTED_ICD_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String icd = sjss.getRawVar(varName[0]).toString();
			retList.add(icd);
		}
		return retList;
	}

	public static Set<String> getAllSelfReportedICDQuery(IDatabase engine) {
		Set<String> retList = new HashSet<String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, ALL_SELF_REPORTED_ICD_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String icd = sjss.getRawVar(varName[0]).toString();
			retList.add(icd);
		}
		return retList;
	}

	/** 
	 * Report type can be LPI, LPNI, High, or TBD
	 */
	public static Map<String,String> processReportTypeQuery(IDatabase engine) {
		Map<String,String> retMap = new HashMap<String,String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_TYPE_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String instance = sjss.getVar(varName[0]).toString();
			String reportType = sjss.getVar(varName[1]).toString();
			retMap.put(instance,reportType);
		}

		return retMap;
	}

	public static Set<String> processSysDataSOR(IDatabase engine){
		ISelectWrapper sorWrapper = Utility.processQuery(engine, SYS_SOR_DATA_CONCAT_QUERY);
		String[] names = sorWrapper.getVariables();
		Set<String> retSet = new HashSet<String>();
		while(sorWrapper.hasNext()) {
			ISelectStatement sjss = sorWrapper.next();
			String val = sjss.getVar(names[0]).toString();
			retSet.add(val);
		}
		return retSet;
	}

	public static HashMap<String, HashMap<String, Double>> getSysGLItem(IDatabase engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, LOE_SYS_GLITEM_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString().replace("\"", "");
			String data = sjss.getVar(names[1]).toString().replace("\"", "");
			String ser = sjss.getVar(names[2]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[3]);
			String glTag = sjss.getVar(names[4]).toString().replace("\"", "");

			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				dataHash.put(data, innerHash);
				innerHash.put(sys + "+++" + ser + "+++" + glTag, loe);
			} else {
				innerHash = dataHash.get(data);
				innerHash.put(sys + "+++" + ser + "+++" + glTag, loe);
			}
		}
		return dataHash;
	}

	public static HashMap<String, HashMap<String, Double>> getAvgSysGLItem(IDatabase engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, AVG_LOE_SYS_GLITEM_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String glTag = sjss.getVar(names[3]).toString().replace("\"", "");

			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				dataHash.put(data, innerHash);
				innerHash.put(ser + "+++" + glTag, loe);
			} else {
				innerHash = dataHash.get(data);
				innerHash.put(ser + "+++" + glTag, loe);
			}
		}
		return dataHash;
	}

	public static HashMap<String, HashMap<String, Double>> getGenericGLItem(IDatabase engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, LOE_GENERIC_GLITEM_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);

			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				dataHash.put(data, innerHash);
				innerHash.put(ser, loe);
			} else {
				innerHash = dataHash.get(data);
				innerHash.put(ser, loe);
			}
		}
		return dataHash;
	}

	public static Map<String, String> getServiceToData(IDatabase engine)
	{
		Map<String, String> dataHash = new HashMap<String, String>();

		ISelectWrapper sjsw = Utility.processQuery(engine, SERVICE_TO_DATA_LIST_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");

			dataHash.put(data, ser);
		}
		return dataHash;
	}

	public static Map<String, Map<String, Map<String, Map<String, Map<String, Double>>>>> getSysGLItemAndPhase(IDatabase engine)
	{
		Map<String, Map<String, Map<String, Map<String, Map<String, Double>>>>> dataHash = new HashMap<String, Map<String, Map<String, Map<String, Map<String, Double>>>>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_SPECIFIC_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString().replace("\"", "");
			String data = sjss.getVar(names[1]).toString().replace("\"", "");
			String ser = sjss.getVar(names[2]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[3]);
			String glTag = sjss.getVar(names[4]).toString().replace("\"", "");
			String phase = sjss.getVar(names[5]).toString().replace("\"", "");

			if(dataHash.containsKey(ser)) {
				Map<String, Map<String, Map<String, Map<String, Double>>>> innerHash1 = dataHash.get(ser);
				if(innerHash1.containsKey(data)) {
					Map<String, Map<String, Map<String, Double>>> innerHash2 = innerHash1.get(data);
					if(innerHash2.containsKey(sys)) {
						Map<String, Map<String, Double>> innerHash3 = innerHash2.get(sys);
						if(innerHash3.containsKey(glTag)) {
							Map<String, Double> innerHash4 = innerHash3.get(glTag);
							if(innerHash4.containsKey(phase)) {
								// this code block should never be entered as there should only be one loe value per sys-data-ser-glTag-phase combination
								System.err.println("Multiple LOEs found for a given system-data-service-gltag-phase combination");
								System.err.println("Combination is: " + sys + "-" + data + "-" + ser + "-" + glTag + "-" + phase);	
							} else {
								innerHash4.put(phase, loe);
							}
						} else {
							Map<String, Double> innerHash4 = new HashMap<String, Double>();
							innerHash4.put(phase, loe);
							innerHash3.put(glTag, innerHash4);
						}
					} else {
						Map<String, Double> innerHash4 = new HashMap<String, Double>();
						innerHash4.put(phase, loe);
						Map<String, Map<String, Double>> innerHash3 = new HashMap<String, Map<String, Double>>();
						innerHash3.put(glTag, innerHash4);
						innerHash2.put(sys, innerHash3);
					}
				} else {
					Map<String, Double> innerHash4 = new HashMap<String, Double>();
					innerHash4.put(phase, loe);
					Map<String, Map<String, Double>> innerHash3 = new HashMap<String, Map<String, Double>>();
					innerHash3.put(glTag, innerHash4);
					Map<String, Map<String, Map<String, Double>>> innerHash2 = new HashMap<String, Map<String, Map<String, Double>>>();
					innerHash2.put(sys, innerHash3);
					innerHash1.put(data, innerHash2);
				}
			} else {
				Map<String, Double> innerHash4 = new HashMap<String, Double>();
				innerHash4.put(phase, loe);
				Map<String, Map<String, Double>> innerHash3 = new HashMap<String, Map<String, Double>>();
				innerHash3.put(glTag, innerHash4);
				Map<String, Map<String, Map<String, Double>>> innerHash2 = new HashMap<String, Map<String, Map<String, Double>>>();
				innerHash2.put(sys, innerHash3);
				Map<String, Map<String, Map<String, Map<String, Double>>>> innerHash1 = new HashMap<String, Map<String, Map<String, Map<String, Double>>>>();
				innerHash1.put(data, innerHash2);
				dataHash.put(ser, innerHash1);
			}
		}
		return dataHash;
	}

	public static Map<String, Map<String, Map<String, Map<String, Double>>>> getAvgSysGLItemAndPhase(IDatabase engine)
	{
		Map<String, Map<String, Map<String, Map<String, Double>>>> dataHash = new HashMap<String, Map<String, Map<String, Map<String, Double>>>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, AVERAGE_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String glTag = sjss.getVar(names[3]).toString().replace("\"", "");
			String phase = sjss.getVar(names[4]).toString().replace("\"", "");

			if(dataHash.containsKey(ser)) {
				Map<String, Map<String, Map<String, Double>>> innerHash1 = dataHash.get(ser);
				if(innerHash1.containsKey(data)) {
					Map<String, Map<String, Double>> innerHash2 = innerHash1.get(data);
					if(innerHash2.containsKey(glTag)) {
						Map<String, Double> innerHash3 = innerHash2.get(glTag);
						if(innerHash3.containsKey(phase)) {
							// this code block should never be entered as there should only be one loe value per sys-data-ser-glTag-phase combination
							System.err.println("Multiple LOEs found for a given data-service-gltag-phase combination");
							System.err.println("Combination is: " + data + "-" + ser + "-" + glTag + "-" + phase);
						} else {
							innerHash3.put(phase, loe);
						}
					} else {
						Map<String, Double> innerHash3 = new HashMap<String, Double>();
						innerHash3.put(phase, loe);
						innerHash2.put(glTag, innerHash3);
					}
				} else {
					Map<String, Double> innerHash3 = new HashMap<String, Double>();
					innerHash3.put(phase, loe);
					Map<String, Map<String, Double>> innerHash2 = new HashMap<String, Map<String, Double>>();
					innerHash2.put(glTag, innerHash3);
					innerHash1.put(data, innerHash2);
				}
			} else {
				Map<String, Double> innerHash3 = new HashMap<String, Double>();
				innerHash3.put(phase, loe);
				Map<String, Map<String, Double>> innerHash2 = new HashMap<String, Map<String, Double>>();
				innerHash2.put(glTag, innerHash3);
				Map<String, Map<String, Map<String, Double>>> innerHash1 = new HashMap<String, Map<String, Map<String, Double>>>();
				innerHash1.put(data, innerHash2);
				dataHash.put(ser, innerHash1);
			}
		}
		return dataHash;
	}

	public static Map<String, Map<String, Map<String, Double>>> getGenericGLItemAndPhase(IDatabase engine)
	{
		Map<String, Map<String, Map<String, Double>>> dataHash = new HashMap<String, Map<String, Map<String, Double>>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, GENERIC_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String phase = sjss.getVar(names[3]).toString().replace("\"", "");

			if(dataHash.containsKey(ser)) {
				Map<String, Map<String, Double>> innerHash1 = dataHash.get(ser);
				if(innerHash1.containsKey(data)) {
					Map<String, Double> innerHash2 = innerHash1.get(data);
					if(innerHash2.containsKey(phase)) {
						// this code block should never be entered as there should only be one loe value per sys-data-ser-glTag-phase combination
						System.err.println("Multiple LOEs found for a given data-service-phase combination");
						System.err.println("Combination is: " + data + "-" + ser + "-" + phase);
					} else {
						innerHash2.put(phase, loe);
					}
				} else {
					Map<String, Double> innerHash2 = new HashMap<String, Double>();
					innerHash2.put(phase, loe);
					innerHash1.put(data, innerHash2);
				}
			} else {
				Map<String, Double> innerHash2 = new HashMap<String, Double>();
				innerHash2.put(phase, loe);
				Map<String, Map<String, Double>> innerHash1 = new HashMap<String, Map<String, Double>>();
				innerHash1.put(data, innerHash2);
				dataHash.put(ser, innerHash1);
			}
		}
		return dataHash;
	}

	public static Map<String, Map<String, Map<String, Map<String, Double>>>> getSysGLItemAndPhaseByAvgServ(IDatabase engine)
	{
		Map<String, Map<String, Map<String, Map<String, Double>>>> dataHash = new HashMap<String, Map<String, Map<String, Map<String, Double>>>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, SYS_SPECIFIC_LOE_AND_PHASE_AVG_SERVICE_QUERY);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString().replace("\"", "");
			String data = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String glTag = sjss.getVar(names[3]).toString().replace("\"", "");
			String phase = sjss.getVar(names[4]).toString().replace("\"", "");

			if(dataHash.containsKey(data)) {
				Map<String, Map<String, Map<String, Double>>> innerHash1 = dataHash.get(data);
				if(innerHash1.containsKey(sys)) {
					Map<String, Map<String, Double>> innerHash2 = innerHash1.get(sys);
					if(innerHash2.containsKey(glTag)) {
						Map<String, Double> innerHash3 = innerHash2.get(glTag);
						if(innerHash3.containsKey(phase)) {
							// this code block should never be entered as there should only be one loe value per sys-data-ser-glTag-phase combination
							System.err.println("Multiple LOEs found for a given system-data-service-gltag-phase combination");
							System.err.println("Combination is: " + sys + "-" + data + "-" + glTag + "-" + phase);
						} else {
							innerHash3.put(phase, loe);
						}
					} else {
						Map<String, Double> innerHash3 = new HashMap<String, Double>();
						innerHash3.put(phase, loe);
						innerHash2.put(glTag, innerHash3);
					}
				} else {
					Map<String, Double> innerHash3 = new HashMap<String, Double>();
					innerHash3.put(phase, loe);
					Map<String, Map<String, Double>> innerHash2 = new HashMap<String, Map<String, Double>>();
					innerHash2.put(glTag, innerHash3);
					innerHash1.put(sys, innerHash2);
				}
			} else {
				Map<String, Double> innerHash3 = new HashMap<String, Double>();
				innerHash3.put(phase, loe);
				Map<String, Map<String, Double>> innerHash2 = new HashMap<String, Map<String, Double>>();
				innerHash2.put(glTag, innerHash3);
				Map<String, Map<String, Map<String, Double>>> innerHash1 = new HashMap<String, Map<String, Map<String, Double>>>();
				innerHash1.put(sys, innerHash2);
				dataHash.put(data, innerHash1);
			}
		}
		return dataHash;
	}

	public static Map<String, Map<String, Map<String, Double>>> getAvgSysGLItemAndPhaseByAvgServ(IDatabase engine)
	{
		Map<String, Map<String, Map<String, Double>>> dataHash = new HashMap<String, Map<String, Map<String, Double>>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, AVERAGE_LOE_AND_PHASE_AVG_SERVICE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[1]);
			String glTag = sjss.getVar(names[2]).toString().replace("\"", "");
			String phase = sjss.getVar(names[3]).toString().replace("\"", "");

			if(dataHash.containsKey(data)) {
				Map<String, Map<String, Double>> innerHash1 = dataHash.get(data);
				if(innerHash1.containsKey(glTag)) {
					Map<String, Double> innerHash2 = innerHash1.get(glTag);
					if(innerHash2.containsKey(phase)) {
						// this code block should never be entered as there should only be one loe value per data-ser-glTag-phase combination
						System.err.println("Multiple LOEs found for a given data-service-gltag-phase combination");
						System.err.println("Combination is: " + data + "-" + glTag + "-" + phase);
					} else {
						innerHash2.put(phase, loe);
					}
				} else {
					Map<String, Double> innerHash2 = new HashMap<String, Double>();
					innerHash2.put(phase, loe);
					innerHash1.put(glTag, innerHash2);
				}
			} else {
				Map<String, Double> innerHash2 = new HashMap<String, Double>();
				innerHash2.put(phase, loe);
				Map<String, Map<String, Double>> innerHash1 = new HashMap<String, Map<String, Double>>();
				innerHash1.put(glTag, innerHash2);
				dataHash.put(data, innerHash1);
			}
		}
		return dataHash;
	}

	public static Map<String, Map<String, Double>> getGenericGLItemAndPhaseByAvgServ(IDatabase engine) {
		Map<String, Map<String, Double>> dataHash = new HashMap<String, Map<String, Double>>();

		ISelectWrapper sjsw = Utility.processQuery(engine, LOE_GENERIC_AND_PHASE_AVG_SERVICE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[1]);
			String phase = sjss.getVar(names[2]).toString().replace("\"", "");

			if(dataHash.containsKey(data)) {
				Map<String, Double> innerHash1 = dataHash.get(data);
				if(innerHash1.containsKey(phase)) {
					// this code block should never be entered as there should only be one loe value per data-phase combination
					System.err.println("Multiple LOEs found for a given data-service-gltag-phase combination");
					System.err.println("Combination is: " + data + "-" + phase);
				} else {
					innerHash1.put(phase, loe);
				}
			} else {
				Map<String, Double> innerHash1= new HashMap<String, Double>();
				innerHash1.put(phase, loe);
				dataHash.put(data, innerHash1);
			}
		}
		return dataHash;
	}

	public static HashSet<String> runVarListQuery(IDatabase engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String var = sjss.getVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
	}

	public static HashSet<String> runRawVarListQuery(IDatabase engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String var = sjss.getRawVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
	}

	public static String[] removeSystemFromStringArray(String[] names) {
		String[] retArray = new String[names.length-1];
		for(int j=0;j<retArray.length;j++)
			retArray[j] = names[j+1];
		return retArray;
	}

	public static List<Object[]> removeSystemFromArrayList(List<Object[]> newData)
	{
		List<Object[]> retList = new ArrayList<Object[]>();
		for(int i=0;i<newData.size();i++)
		{
			Object[] row = new Object [newData.get(i).length-1];
			for(int j=0;j<row.length;j++)
				row[j] = newData.get(i)[j+1];
			retList.add(row);
		}
		return retList;
	}

}
