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
package prerna.ui.components.specific.tap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class ServicesAggregationProcessor extends AggregationHelper {

	private static final Logger FILE_LOGGER = Logger.getLogger("reportsLogger");
	private static final Logger LOGGER = LogManager.getLogger(ServicesAggregationProcessor.class.getName());

	private IEngine servicesDB;
	private IEngine coreDB;

	private HashSet<String> allSoftwareModules = new HashSet<String>();
	private HashSet<String> allHardwareModules = new HashSet<String>();

	public String errorMessage = "";
	private boolean addedToOwl = false;
	private boolean aggregationSuccess;

	private String TAP_SERVICES_AGGREGATE_SYSTEM_USERS_QUERY = "SELECT DISTINCT ?system ?usedBy ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>} {?systemService ?usedBy ?user}}";

	private String TAP_SYSTEM_SERVICES_PROPERTY_AGGREGATION_QUERY = "SELECT DISTINCT ?system ?prop ?value ?user WHERE{{?system a <http://semoss.org/ontologies/Concept/System>} {?systemService a <http://semoss.org/ontologies/Concept/SystemService>} {?user a <http://semoss.org/ontologies/Concept/SystemUser>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?prop a <http://semoss.org/ontologies/Relation/Contains>} {?systemService ?prop ?value} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user}}";
	private String TAP_CORE_SYSTEM_PROPERTY_AGGREGATION_QUERY = "SELECT DISTINCT ?system ?prop ?value WHERE{{?system a <http://semoss.org/ontologies/Concept/System>} {?prop a <http://semoss.org/ontologies/Relation/Contains>} {?system ?prop ?value} }";

	private String TAP_SERVICES_AGGREGATE_PERSONNEL_QUERY = "SELECT DISTINCT ?system ?usedBy ?personnel WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>}{?usedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>} {?personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel>}  {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?usedBy ?personnel}}";

	private String TAP_SERVICES_AGGREGATE_USER_INTERFACE_QUERY = "SELECT DISTINCT ?system ?utilizes ?userInterface WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>} {?userInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?utilizes ?userInterface}}";

	private String TAP_SERVICES_AGGREGATE_BP_QUERY = "SELECT DISTINCT ?system ?supports ?bp ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?supports ?bp} OPTIONAL{{?supports <http://semoss.org/ontologies/Relation/Contains/Reported> ?prop1} }}";
	private String TAP_SERVICES_AGGREGATE_BP_COMMENTS_QUERY = "SELECT DISTINCT ?system ?bp ?supports ?pred ?prop1 ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService ?supports ?bp} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?supports ?pred ?prop1}}";
	private String TAP_CORE_AGGREGATE_BP_COMMENTS_QUERY = "SELECT DISTINCT ?system ?bp ?supports ?pred ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}  {?system ?supports ?bp} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?supports ?pred ?prop1}}";

	private String TAP_SERVICES_AGGREGATE_ACTIVITY_QUERY = "SELECT DISTINCT ?system ?supports ?activity ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?supports ?activity} OPTIONAL{{?supports <http://semoss.org/ontologies/Relation/Contains/Reported> ?prop1}}}";
	private String TAP_SERVICES_AGGREGATE_ACTIVITY_COMMENTS_QUERY = "SELECT DISTINCT ?system ?activity ?supports ?pred ?prop1 ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService ?supports ?activity} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?supports ?pred ?prop1}}";
	private String TAP_CORE_AGGREGATE_ACTIVITY_COMMENTS_QUERY = "SELECT DISTINCT ?system ?activity ?supports ?pred ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>} {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?system ?supports ?activity} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?supports ?pred ?prop1}}";

	private String TAP_SERVICES_AGGREGATE_BLU_QUERY = "SELECT DISTINCT ?system ?provide ?BLU ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?provide ?BLU} {?provide <http://semoss.org/ontologies/Relation/Contains/weight> ?prop1} }";
	private String TAP_SERVICES_AGGREGATE_BLU_COMMENTS_QUERY = "SELECT DISTINCT ?system ?BLU ?provide ?pred ?prop1 ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService ?provide ?BLU} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?provide ?pred ?prop1 } }";
	private String TAP_CORE_AGGREGATE_BLU_COMMENTS_QUERY = "SELECT DISTINCT ?system ?BLU ?provide ?pred ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?system ?provide ?BLU} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?provide ?pred ?prop1 } }";

	private String TAP_SERVICES_AGGREGATE_LIFECYCLE_QUERY = "SELECT DISTINCT ?system ?phase ?lifeCycle WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase>} {?lifeCycle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/LifeCycle>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?phase ?lifeCycle}}";

	private String TAP_SERVICES_SYSTEM_PROVIDE_ICD_QUERY = "SELECT DISTINCT ?sys ?pred ?icd WHERE{{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?pred <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?sys ?pred ?icd}}";

	private String TAP_SERVICES_ICD_CONSUME_SYS_QUERY = "SELECT DISTINCT ?icd ?pred ?sys WHERE{{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?pred <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>} {?icd ?pred ?sys}}";

	private String TAP_CORE_AGGREGATE_ICD_PROP_QUERY = "SELECT DISTINCT ?Payload ?prop ?value WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?ICD ?Payload ?Data} {?Payload ?prop ?value}}";
	private String TAP_SERVICES_AGGREGATE_ICD_PROP_QUERY = "SELECT DISTINCT ?Payload ?prop ?value ?user WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?user a <http://semoss.org/ontologies/Concept/SystemUser>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?Payload ?prop ?value} {?ICD ?Payload ?Data} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService <http://semoss.org/ontologies/Relation/Implemented_At> ?ICD}}";

	private String TAP_SERVICES_AGGREGATE_ICD_DATAOBJECT_QUERY = "SELECT DISTINCT ?ICD ?Payload ?data WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Payload ?data} }";
	//	private String TAP_CORE_AND_SERVICES_AGGREGATE_ICD_DATAOBJECT_COMMENT_QUERY = "SELECT DISTINCT ?Payload ?pred ?prop1 WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Payload ?data} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?Payload ?pred ?prop1} }";

	private String TAP_SERVICES_AGGREGATE_ICD_DFORM_QUERY = "SELECT DISTINCT ?ICD ?has ?dForm WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?dForm <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?ICD ?has ?dForm}}";
	private String TAP_SERVICER_AGGREGATE_ICD_DFREQ_QUERY = "SELECT DISTINCT ?ICD ?has ?dFreq WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?dFreq <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?ICD ?has ?dFreq}}";
	private String TAP_SERVICER_AGGREGATE_ICD_DPROT_QUERY = "SELECT DISTINCT ?ICD ?has ?dProt WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?dProt <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?ICD ?has ?dProt}}";
	private String TAP_SERVICER_AGGREGATE_ICD_LIFECYCLE_QUERY = "SELECT DISTINCT ?ICD ?phase ?lifeCycle WHERE{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?lifeCycle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/LifeCycle>} {?phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase>} {?ICD ?phase ?lifeCycle} }";

	private String TAP_SERVICES_AGGREGATE_TERROR_WEIGHT_QUERY = "SELECT DISTINCT ?system ?has ?TError ?weight WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?has ?TError} OPTIONAL{{?has <http://semoss.org/ontologies/Relation/Contains/weight> ?weight}}}";
	private String TAP_CORE_AGGREGATE_TERROR_WEIGHT_QUERY = "SELECT DISTINCT ?system ?has ?TError ?weight WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?system ?has ?TError} {?has <http://semoss.org/ontologies/Relation/Contains/weight> ?weight}}";
	private String TAP_SERVICES_AGGREGATE_TERROR_COMMENTS_QUERY = "SELECT DISTINCT ?system ?TError ?has ?pred ?prop1 ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>}  {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService ?has ?TError} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?has ?pred ?prop1} }";
	private String TAP_CORE_AGGREGATE_TERROR_COMMENTS_QUERY = "SELECT DISTINCT ?system ?TError ?has ?pred ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?TError <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError>} {?system ?has ?TError} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?has ?pred ?prop1} }";

	private String TAP_SERVICES_AGGREGATE_DATAOBJECT_CRM_QUERY = "SELECT DISTINCT ?system ?provide ?data ?crm WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?provide ?data} OPTIONAL{{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}}}";
	private String TAP_CORE_AGGREGATE_DATAOBJECT_CRM_QUERY = "SELECT DISTINCT ?system ?provide ?data ?crm WHERE{ {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system ?provide ?data} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}}";
	private String TAP_SERVICER_AGGREGATE_DATAOBJECT_COMMENTS_QUERY = "SELECT DISTINCT ?system ?data ?provide ?pred ?prop1 ?user WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} {?systemService ?provide ?data} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?provide ?pred ?prop1} }";
	private String TAP_CORE_AGGREGATE_DATAOBJECT_COMMENTS_QUERY = "SELECT DISTINCT ?system ?data ?provide ?pred ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system ?provide ?data} BIND(<http://semoss.org/ontologies/Relation/Contains/Comments> AS ?pred) {?provide ?pred ?prop1} }";
	
	private String TAP_SERVICES_AGGRATE_DATAOBJECT_REPORTED_QUERY = "SELECT DISTINCT ?system ?provide ?data ?prop1 WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?systemService ?provide ?data} {?provide <http://semoss.org/ontologies/Relation/Contains/Reported> ?prop1}}";
	
	private String TAP_CORE_SOFTWARE_MODULE_LIST_QUERY = "SELECT DISTINCT ?softwareModule WHERE{{?softwareModule a <http://semoss.org/ontologies/Concept/SoftwareModule>} }";

	private String TAP_CORE_HARDWARE_MODULE_LIST_QUERY = "SELECT DISTINCT ?hardwareModule WHERE{{?hardwareModule a <http://semoss.org/ontologies/Concept/HardwareModule>} }";

	private String TAP_SERVICES_AGGREGATION_SOFTWARE_QUERY = "SELECT DISTINCT ?softwareModule ?prop ?value ?system ?softwareVersion ?software ?user WHERE{{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?softwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?software <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Software>} {?serviceSoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ServiceSoftwareModule>} {?serviceSoftwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?softwareModule} {?systemService <http://semoss.org/ontologies/Relation/Consists> ?serviceSoftwareModule} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?serviceSoftwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?softwareModule} {?softwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?softwareVersion} {?software <http://semoss.org/ontologies/Relation/Has> ?softwareVersion} {?systemService <http://semoss.org/ontologies/Relation/UsedBy> ?user} OPTIONAL{{?serviceSoftwareModule ?prop ?value}} }";

	private String TAP_CORE_AGGREGATION_SOFTWARE_QUERY = "SELECT DISTINCT ?softwareModule ?prop ?value WHERE{{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?softwareModule ?prop ?value}}";

	private String TAP_SERVICES_AGGREGATE_HARDWARE_QUERY = "SELECT DISTINCT ?hardwareModule ?prop ?value ?system ?hardwareVersion ?hardware ?user WHERE{{?hardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?serviceHardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ServiceHardwareModule>} {?systemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?hardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemUser>} {?hardware <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Hardware>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?hardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?serviceHardwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?hardwareModule} {?systemService <http://semoss.org/ontologies/Relation/Has> ?serviceHardwareModule} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?system <http://semoss.org/ontologies/Relation/ConsistsOf> ?systemService} {?serviceHardwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?hardwareModule} {?hardwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?hardwareVersion} {?hardware <http://semoss.org/ontologies/Relation/Has> ?hardwareVersion} {?systemService ?usedBy ?user} OPTIONAL{{?serviceHardwareModule ?prop ?value}} }";

	private String TAP_CORE_AGGREGATION_HARDWARE_QUERY = "SELECT DISTINCT ?hardwareModule ?prop ?value WHERE{{?hardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?hardwareModule ?prop ?value}}";

	private String TAP_CORE_RELATIONS_LIST_QUERY = "SELECT ?relations WHERE{{?relations <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} filter(regex(str(?relations),\"^http://semoss\"))}";

	private String TAP_CORE_CONCEPTS_LIST_QUERY = "SELECT ?concepts WHERE{{?concepts <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}}";

	public ServicesAggregationProcessor(IEngine servicesDB, IEngine coreDB){
		this.servicesDB = servicesDB;
		this.coreDB = coreDB;
	}

	public boolean runFullAggregation()
	{		
		this.aggregationSuccess = true;

		FILE_LOGGER.info("TAP_SERVICES_DB TO TAP_CORE_DB AGGREGATION!");
		FILE_LOGGER.info("DB NAME >>> SUBJECT >>>>> PREDICATE >>>>> OBJECT >>>>> ERROR MESSAGE");

		runGetListOfModules(TAP_CORE_SOFTWARE_MODULE_LIST_QUERY, true);
		runGetListOfModules(TAP_CORE_HARDWARE_MODULE_LIST_QUERY, false);

		LOGGER.info("PROCESSING SYSTEM USERS FOR SERVICE SYSTEMS AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_SYSTEM_USERS_QUERY);

		LOGGER.info("PROCESSING PERSONNEL FOR SERVICE SYSTEMS AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_PERSONNEL_QUERY);

		LOGGER.info("PROCESSING USER INTERFACE FOR SERVICE SYSTEMS AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_USER_INTERFACE_QUERY);

		LOGGER.info("PROCESSING BP FOR SERVICE SYSTEMS WITH WEIGHT AND COMMENTS PROPERTY AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregationWithProperties(TAP_SERVICES_AGGREGATE_BP_QUERY, new String[]{"Reported"});
		runCommentAggregationForRelationships(TAP_SERVICES_AGGREGATE_BP_COMMENTS_QUERY,TAP_CORE_AGGREGATE_BP_COMMENTS_QUERY );

		LOGGER.info("PROCESSING ACTIVITY FOR SERVICE SYSTEMS WITH WEIGHT AND COMMENTS PROPERTY AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregationWithProperties(TAP_SERVICES_AGGREGATE_ACTIVITY_QUERY, new String[]{"Reported"});
		runCommentAggregationForRelationships(TAP_SERVICES_AGGREGATE_ACTIVITY_COMMENTS_QUERY, TAP_CORE_AGGREGATE_ACTIVITY_COMMENTS_QUERY);

		LOGGER.info("PROCESSING BLU FOR SERVICE SYSTEMS WITH WEIGHT AND COMMENTS PROPERTY AND AGGREGATING TO SYSTEMS IN TAP CORE");
		runRelationshipAggregationWithProperties(TAP_SERVICES_AGGREGATE_BLU_QUERY, new String[]{"weight"});
		runCommentAggregationForRelationships(TAP_SERVICES_AGGREGATE_BLU_COMMENTS_QUERY, TAP_CORE_AGGREGATE_BLU_COMMENTS_QUERY);

		LOGGER.info("PROCESSING SYSTEM PROVIDE ICDS AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_SYSTEM_PROVIDE_ICD_QUERY);

		LOGGER.info("PROCESSING ICD CONSUME SYSTEM AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_ICD_CONSUME_SYS_QUERY);

		LOGGER.info("PROCESSING ICD PAYLOAD DATA_OBJECT AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_ICD_DATAOBJECT_QUERY);
		//		runCommentAggregationForRelationships(TAP_CORE_AND_SERVICES_AGGREGATE_ICD_DATAOBJECT_COMMENT_QUERY, TAP_CORE_AND_SERVICES_AGGREGATE_ICD_DATAOBJECT_COMMENT_QUERY);

		LOGGER.info("PROCESSING ICD HAS DFORM AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICES_AGGREGATE_ICD_DFORM_QUERY);
		LOGGER.info("PROCESSING ICD HAS DFREQ AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICER_AGGREGATE_ICD_DFREQ_QUERY);
		LOGGER.info("PROCESSING ICD HAS DPROT_OBJECT AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICER_AGGREGATE_ICD_DPROT_QUERY);
		LOGGER.info("PROCESSING ICD PHASE LIFECYCLE AND PUSHING INTO TAP CORE");
		runRelationshipAggregation(TAP_SERVICER_AGGREGATE_ICD_LIFECYCLE_QUERY);

		LOGGER.info("PROCESSING SYSTEM SERVICE LIFE AND AGGREGATING INTO TAP CORE");
		runSystemServiceLifeCylceAggregation(TAP_SERVICES_AGGREGATE_LIFECYCLE_QUERY);

		LOGGER.info("PROCESSING SYSTEM SERVICE PROPERTIES AND AGGREGATING TO SYSTEMS IN TAP CORE");
		LOGGER.info("QUERIES BOTH TAP SERVICES AND TAP CORE SO NO PROPERTIES ARE LOST");
		runSystemServicePropertyAggregation(TAP_SYSTEM_SERVICES_PROPERTY_AGGREGATION_QUERY, TAP_CORE_SYSTEM_PROPERTY_AGGREGATION_QUERY);

		LOGGER.info("PROCESSING ICD PAYLOAD DATA RELATIONSHIP PROPERTIES AND AGGREGATING TO PAYLOAD RELATIONSHIP IN TAP CORE");
		LOGGER.info("QUERIES BOTH TAP SERVICES AND TAP CORE SO NO PROPERTIES ARE LOST");
		runICDPropAggregation(TAP_SERVICES_AGGREGATE_ICD_PROP_QUERY, TAP_CORE_AGGREGATE_ICD_PROP_QUERY);

		LOGGER.info("PROCESSING SYSTEM TERROR RELATIONSHIP AND WEIGHT PROPERTY AND AGGREGATING INTO TAP CORE");
		LOGGER.info("QUERIES BOTH TAP SERVICES AND TAP CORE TO DETERMINE NEW WEIGHT OF TERROR PROPERTY VALUE");
		runTErrorAggregation(TAP_SERVICES_AGGREGATE_TERROR_WEIGHT_QUERY, TAP_CORE_AGGREGATE_TERROR_WEIGHT_QUERY);
		LOGGER.info("PROCESSING SYSTEM TERROR RELATIONSHIP AND COMMENTS PROPERTY AND AGGREGATING INTO TAP CORE");
		runCommentAggregationForRelationships(TAP_SERVICES_AGGREGATE_TERROR_COMMENTS_QUERY, TAP_CORE_AGGREGATE_TERROR_COMMENTS_QUERY);

		LOGGER.info("PROCESSING SYSTEM DATAOBJECT RELATIONSHIP AND CRM PROPERTY AND AGGREGATING INTO TAP CORE");
		LOGGER.info("QUERIES BOTH TAP SERVICES AND TAP CORE TO DETERMINE CRM");
		runDataObjectAggregation(TAP_SERVICES_AGGREGATE_DATAOBJECT_CRM_QUERY, TAP_CORE_AGGREGATE_DATAOBJECT_CRM_QUERY);
		LOGGER.info("PROCESSING SYSTEM DATAOBJECT RELATIONSHIP AND REPORTED COMMENT INTO TAP CORE");
		runRelationshipAggregationWithProperties(TAP_SERVICES_AGGRATE_DATAOBJECT_REPORTED_QUERY, new String[]{"Reported"});
		LOGGER.info("PROCESSING SYSTEM DATAOBJECT RELATIONSHIP AND COMMENTS PROPERTY AND AGGREGATING INTO TAP CORE");
		runCommentAggregationForRelationships(TAP_SERVICER_AGGREGATE_DATAOBJECT_COMMENTS_QUERY, TAP_CORE_AGGREGATE_DATAOBJECT_COMMENTS_QUERY);

		LOGGER.info("PROCESSING SERVICE SYSTEM MODULE AND DOING LOTS OF STUFF");
		runHardwareSoftwareAggregation(TAP_SERVICES_AGGREGATION_SOFTWARE_QUERY, TAP_CORE_AGGREGATION_SOFTWARE_QUERY, true);

		LOGGER.info("PROCESSING SERVICE HARDWARE MODULE AND DOING LOTS OF STUFF");
		runHardwareSoftwareAggregation(TAP_SERVICES_AGGREGATE_HARDWARE_QUERY, TAP_CORE_AGGREGATION_HARDWARE_QUERY, false);
		
		processNewConcepts();
		processNewRelationships();

		((BigDataEngine) coreDB).commit();
		((BigDataEngine) coreDB).infer();

		if(addedToOwl) {
			writeToOWL(coreDB);
		}

		return this.aggregationSuccess;
	}

	private void runRelationshipAggregation(String query)
	{
		dataHash.clear();
		allLabels.clear();
		ISelectWrapper sjsw = Utility.processQuery(servicesDB, query);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String subject = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String object = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(subject, "/") +":" + getTextAfterFinalDelimeter(object, "/");
			LOGGER.debug("ADDING RELATIONSHIP:     " + subject + " -----> {" + pred + " --- " + object + "}");
			addToDataHash(new String[]{subject, pred, object});
			// add instances to master list
			addToAllConcepts(subject);
			addToAllConcepts(object);
			addToAllRelationships(pred);
			
			addToAllLabel(subject);
			addToAllLabel(object);
			addToAllLabel(pred);
		}
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private void runRelationshipAggregationWithProperties(String query, String[] varNames)
	{
		dataHash.clear();
		allLabels.clear();
		ISelectWrapper sjsw = Utility.processQuery(servicesDB, query);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String subject = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String object = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(subject, "/") +":" + getTextAfterFinalDelimeter(object, "/");
			LOGGER.debug("ADDING RELATIONSHIP:     " + subject + " -----> {" + pred + " --- " + object + "}");
			addToDataHash(new String[]{subject, pred, object});
			
			// add instances to master list
			addToAllConcepts(subject);
			addToAllConcepts(object);
			addToAllRelationships(pred);

			addToAllLabel(subject);
			addToAllLabel(object);
			addToAllLabel(pred);
			for(int i = 3; i < vars.length; i++)
			{
				String propPrep = semossPropertyBaseURI + varNames[i-3];
				Object propValue = sjss.getVar(vars[i]);
				LOGGER.debug("ADDING RELATIONSHIP PROPERTY:     " + pred + " -----> {" + propPrep + " --- " + propValue + "}");
				addToDataHash(new Object[]{pred, propPrep, propValue});
			}
		}
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process system life cycle properties
	private void runSystemServiceLifeCylceAggregation(String query) 
	{
		dataHash.clear();
		allLabels.clear();
		ISelectWrapper sjsw = Utility.processQuery(servicesDB, query);
		HashMap<String, LinkedList<String>> lifeCycleHash = aggregateLifeCycle(sjsw);
		String lifeCycle = "";
		for( String sys : lifeCycleHash.keySet())
		{
			LinkedList<String> lifeCycleList = lifeCycleHash.get(sys);
			String pred = lifeCycleList.get(0);
			lifeCycleList.remove(0);
			if(lifeCycleList.toString().contains("LifeCycle/Supported"))
			{
				lifeCycle = getBaseURI(sys) + "/Concept/LifeCycle/Supported";
			}
			else if(lifeCycleList.toString().contains("LifeCycle/TBD"))
			{
				lifeCycle = getBaseURI(sys) + "/Concept/LifeCycle/TBD";
			}
			else
			{
				lifeCycle = getBaseURI(sys) + "/Concept/LifeCycle/Retired_(Not_Supported)";
			}
			addToAllConcepts(sys);
			addToAllConcepts(lifeCycle);
			addToAllRelationships(pred);
			
			addToAllLabel(sys);
			addToAllLabel(lifeCycle);
			addToAllLabel(pred);
			
			LOGGER.debug("ADDING SYSTEM LIFECYCLE:     " + sys + " -----> {" + pred + " --- " + lifeCycle + "}");
			addToDataHash(new String[]{sys, pred, lifeCycle});
		}
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private HashMap<String, LinkedList<String>> aggregateLifeCycle(ISelectWrapper sjsw)	{
		HashMap<String, LinkedList<String>> lifeCycleHash = new HashMap<String, LinkedList<String>>();
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String sys = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String lifeCycle = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(sys, "/") +":" + getTextAfterFinalDelimeter(lifeCycle, "/");

			if(!lifeCycle.equals("\"NA\"") && !lifeCycle.equals("\"N-A\"") && !lifeCycle.equals("\"TBD\""))
			{
				if(!lifeCycleHash.containsKey(sys))
				{
					LinkedList<String> lifeCycleList = new LinkedList<String>();
					lifeCycleList.add(pred);
					lifeCycleList.add(lifeCycle);
					lifeCycleHash.put(sys, lifeCycleList);
					LOGGER.debug("ADDING NEW LIFECYCLE LIST:     " + sys + " -----> {" + pred + " --- " + lifeCycleList.toString() + "}");
				}
				else
				{
					LinkedList<String> lifeCycleList = lifeCycleHash.get(sys);
					lifeCycleList.add(lifeCycle);
					LOGGER.debug("ADJUSTING LIFECYCLE LIST:     " + sys + " -----> {" + pred + " --- " + lifeCycleList.toString() + "}");
				}
			}
		}
		return lifeCycleHash;
	}
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process comments

	private void runCommentAggregationForRelationships(String propSystemServiceQuery, String propTAPCoreQuery)
	{
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();
		ISelectWrapper sjswServices = Utility.processQuery(servicesDB, propSystemServiceQuery);
		processCommentPropertiesForRelationships(sjswServices,  false);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, propTAPCoreQuery);
		processCommentPropertiesForRelationships(sjswCore, true);

		// processing modifies class variable dataHash directly
		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private void processCommentPropertiesForRelationships(ISelectWrapper sjsw, boolean TAP_Core)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			this.errorMessage = "";

			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String obj = sjss.getRawVar(vars[1]).toString();
			String pred = sjss.getRawVar(vars[2]).toString();
			pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(sub, "/") +":" + getTextAfterFinalDelimeter(obj, "/");

			String prop = sjss.getRawVar(vars[3]).toString();
			Object value = sjss.getRawVar(vars[4]);
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[5]).toString();
			}

			if(dataHash.containsKey(pred) || !TAP_Core)
			{
				Object[] returnTriple = new Object[3];
				if(prop.equals(semossPropertyBaseURI + "Comments"))
				{
					returnTriple = processConcatCommentString(pred, prop, value, user);
				}

				// if error occurs
				if(Arrays.equals(returnTriple, new String[]{""}))
				{
					String outuptToLog = "";
					if(!TAP_Core)
					{
						outuptToLog += "TAP_Services_DB ";
					}
					else
					{
						outuptToLog += "TAP_Core_DB ";
					}
					outuptToLog += pred + " >>>>> " + prop + " >>>>> " + value.toString() + " >>>>> " + this.errorMessage;
					FILE_LOGGER.info(outuptToLog);
					aggregationSuccess = false;
				}
				// returnTriple never gets a value when the property being passed in isn't in the defined list above
				else if(returnTriple[0] != null)
				{
					LOGGER.debug("ADDING SYSTEM COMMENT PROPERTY:     " + returnTriple[0] + " -----> {" + returnTriple[1] + " --- " + returnTriple[2].toString() + "}");
					addToDataHash(returnTriple);
					
					addToAllLabel(returnTriple[0].toString());
					addToAllLabel(returnTriple[1].toString());
					addToAllLabel(returnTriple[2].toString());
				}

				// sub already exists when going through TAP Core db
				if(!TAP_Core)
				{
					addToAllRelationships(pred);
				}
				// must remove existing triple in TAP Core prior to adding
				if(TAP_Core)
				{
					if(value.toString().startsWith("\"") && value.toString().endsWith("\""))
					{
						value = value.toString().substring(1, value.toString().length()-1); // remove annoying quotes
					}
					addToDeleteHash(new String[]{pred, prop, value.toString()});
				}
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process system service properties

	private void runSystemServicePropertyAggregation(String propSystemServiceQuery, String propTAPCoreQuery)
	{
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();
		ISelectWrapper sjswServices = Utility.processQuery(servicesDB, propSystemServiceQuery);
		processServiceSystemProperties(sjswServices,  false);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, propTAPCoreQuery);
		processServiceSystemProperties(sjswCore, true);

		// processing modifies class variable dataHash directly
		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private void processServiceSystemProperties(ISelectWrapper sjsw, boolean TAP_Core)
	{	
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			this.errorMessage = "";
			boolean aggregatedProp = false;
			
			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			Object value = sjss.getRawVar(vars[2]);
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[3]).toString();
			}

			if(dataHash.containsKey(sub) || !TAP_Core)
			{
				Object[] returnTriple = new Object[3];

				if(prop.equals(semossPropertyBaseURI + "ATO_Date"))
				{
					boolean earliest = false;
					returnTriple = processMinMaxDate(sub, prop, value, earliest);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "End_of_Support_Date"))
				{
					boolean latest = true;
					returnTriple = processMinMaxDate(sub, prop, value, latest);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Availability-Actual"))
				{
					boolean min = false;
					returnTriple = processMaxMinDouble(sub, prop, value, min);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Availability-Required"))
				{
					boolean max = true;
					returnTriple = processMaxMinDouble(sub, prop, value, max);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Description"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "POC"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Full_System_Name"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Number_of_Users"))
				{
					returnTriple = processSumValues(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Transaction_Count"))
				{
					returnTriple = processSumValues(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "User_Consoles"))
				{
					returnTriple = processSumValues(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "GarrisonTheater"))
				{
					returnTriple = processGarrisonTheater(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Transactional"))
				{
					returnTriple = processTransactional(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Comments"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}

				// if error occurs
				if(Arrays.equals(returnTriple, new String[]{""}))
				{
					String outuptToLog = "";
					if(!TAP_Core)
					{
						outuptToLog += "TAP_Services_DB ";
					}
					else
					{
						outuptToLog += "TAP_Core_DB ";
					}
					outuptToLog += sub + " >>>>> " + prop + " >>>>> " + value.toString() + " >>>>> " + this.errorMessage;
					FILE_LOGGER.info(outuptToLog);
					aggregationSuccess = false;
				}
				// returnTriple never gets a value when the property being passed in isn't in the defined list above
				else if(returnTriple[0] != null)
				{
					LOGGER.debug("ADDING SYSTEM PROPERTY:     " + returnTriple[0] + " -----> {" + returnTriple[1] + " --- " + returnTriple[2].toString() + "}");
					addToDataHash(returnTriple);
					
					addToAllLabel(returnTriple[0].toString());
				}

				// sub already exists when going through TAP Core db
				if(!TAP_Core)
				{
					addToAllConcepts(sub);
				}

				// must remove existing triple in TAP Core prior to adding
				if(TAP_Core && aggregatedProp)
				{
					if(value.toString().startsWith("\"") && value.toString().endsWith("\""))
					{
						value = value.toString().substring(1, value.toString().length()-1); // remove annoying quotes
					}
					addToDeleteHash(new String[]{sub, prop, value.toString()});
				}

			}
		}
	}

	private void runICDPropAggregation(String servicesQuery, String coreQuery)
	{
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();

		ISelectWrapper sjswService = Utility.processQuery(servicesDB, servicesQuery);
		processICDPropAggregation(sjswService, false);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, coreQuery);
		processICDPropAggregation(sjswCore , true);

		// processing modifies class variable dataHash directly
		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process icds

	private void processICDPropAggregation(ISelectWrapper sjsw, boolean TAP_Core) 
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			this.errorMessage = "";
			boolean aggregatedProp = false;

			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			Object value = sjss.getVar(vars[2]);
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[3]).toString();
			}

			if(dataHash.containsKey(sub) || !TAP_Core)
			{
				Object[] returnTriple = new Object[3];

				if(prop.equals(semossPropertyBaseURI + "Data"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Format"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Frequency"))
				{
					returnTriple = processDFreq(sub, prop, value);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Interface_Name"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Protocol"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Source"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Type"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}
				else if(prop.equals(semossPropertyBaseURI + "Comments"))
				{
					returnTriple = processConcatString(sub, prop, value, user);
					aggregatedProp = true;
				}

				// if error occurs
				if(Arrays.equals(returnTriple, new String[]{""}))
				{
					String outuptToLog = "";
					if(!TAP_Core)
					{
						outuptToLog += "TAP_Services_DB ";
					}
					else
					{
						outuptToLog += "TAP_Core_DB ";
					}
					outuptToLog += sub + " >>>>> " + prop + " >>>>> " + value.toString() + " >>>>> " + this.errorMessage;
					FILE_LOGGER.info(outuptToLog);
					aggregationSuccess = false;
				}
				// returnTriple never gets a value when the property being passed in isn't in the defined list above
				else if(returnTriple[0] != null)
				{
					LOGGER.debug("ADDING ICD PROPERTY:     " + returnTriple[0] + " -----> {" + returnTriple[1] + " --- " + returnTriple[2].toString() + "}");
					addToDataHash(returnTriple);
					
					addToAllLabel(returnTriple[0].toString());
				}
				// sub already exists when going through TAP Core db
				if(!TAP_Core)
				{
					addToAllRelationships(sub);
				}
				// must remove existing triple in TAP Core prior to adding
				if(TAP_Core & aggregatedProp)
				{
					if(value.toString().startsWith("\"") && value.toString().endsWith("\""))
					{
						value = value.toString().substring(1, value.toString().length()-1); // remove annoying quotes
					}
					addToDeleteHash(new String[]{sub, prop, value.toString()});
				}
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process terror

	private void runTErrorAggregation(String servicesQuery, String coreQuery) 
	{
		HashMap<String, HashMap<String, LinkedList<Object>>> aggregatedTError = new HashMap<String, HashMap<String, LinkedList<Object>>>();
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();

		ISelectWrapper sjswService = Utility.processQuery(servicesDB, servicesQuery);
		aggregatedTError = runAggregateAllData(sjswService, aggregatedTError, "weight", false);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, coreQuery);
		aggregatedTError = runAggregateAllData(sjswCore, aggregatedTError, "weight", true);

		// processing modifies class variable dataHash directly
		processTError(aggregatedTError, "weight");

		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private void processTError(HashMap<String, HashMap<String, LinkedList<Object>>> aggregatedTError, String propType) 
	{
		this.errorMessage = "";
		String propertyURI = semossPropertyBaseURI + propType;
		for( String sub : aggregatedTError.keySet() )
		{
			HashMap<String, LinkedList<Object>> innerHash = aggregatedTError.get(sub);
			for ( String obj : innerHash.keySet() )
			{
				LinkedList<Object> tErrList = innerHash.get(obj);
				Iterator<Object> tErrIt = tErrList.listIterator();
				int counter = 0;
				double totalTErr = 0;
				String pred = "";
				while(tErrIt.hasNext())
				{
					if(counter == 0)
					{
						pred  = tErrIt.next().toString();
						counter++;
					}
					else 
					{
						Literal valueAsObject = (Literal) tErrIt.next();
						try
						{
							valueAsObject.doubleValue();
							Double value = valueAsObject.doubleValue();
							totalTErr += value;
							counter++;
						}
						catch(NumberFormatException e)
						{
							this.errorMessage = this.errorMessage + "Error Processing TError! " 
									+ "Error occured processing: " + pred + " >>>> " + propertyURI + " >>>> " + valueAsObject + " " 	
									+ "Check that value is parsable as a double";	
							String outputToLog = "Unsure About DB" + " >>>>> " + pred + " >>>>> " + propertyURI + " >>>>> " + valueAsObject.toString() + " >>>>> " + this.errorMessage;
							FILE_LOGGER.info(outputToLog);

							aggregationSuccess = false;
						}
					}
				}

				Double TError = totalTErr/(counter-1);
				LOGGER.debug("ADDING SYSTEM TO TERROR RELATIONSHIP:     " + sub + " -----> {" + pred + " --- " + obj + "}");
				addToDataHash(new Object[]{sub, pred, obj});
				
				addToAllLabel(sub);
				addToAllLabel(pred);
				addToAllLabel(obj);
				
				LOGGER.debug("ADDING TERROR WEIGHT RELATIONSHIP PROPERTY:     " + pred + " -----> {" + propertyURI + " --- " +  TError +  "}");
				addToDataHash(new Object[]{pred, propertyURI, TError});
				addToAllConcepts(obj);
				addToAllRelationships(pred);
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process data objects

	private void runDataObjectAggregation(String servicesQuery, String coreQuery)
	{
		HashMap<String, HashMap<String, LinkedList<Object>>> aggregatedDataObjects = new HashMap<String, HashMap<String, LinkedList<Object>>>();
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();

		ISelectWrapper sjswService = Utility.processQuery(servicesDB, servicesQuery);
		aggregatedDataObjects = runAggregateAllData(sjswService , aggregatedDataObjects, "CRM", false);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, coreQuery);
		aggregatedDataObjects = runAggregateAllData(sjswCore, aggregatedDataObjects, "CRM", true);

		// processing modifies class variable dataHash directly
		processDataObjects(aggregatedDataObjects, "CRM");

		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}

	private void processDataObjects(HashMap<String, HashMap<String, LinkedList<Object>>> aggregatedDataObjects, String propType) 
	{
		String propertyURI = semossPropertyBaseURI + propType;
		for( String sub : aggregatedDataObjects.keySet() )
		{
			HashMap<String, LinkedList<Object>> innerHash = aggregatedDataObjects.get(sub);
			for ( String obj : innerHash.keySet() )
			{
				LinkedList<Object> crmList = innerHash.get(obj);
				String pred  = crmList.get(0).toString();
				crmList.remove(0);
				String CRM = "";
				if(crmList.toString().contains("C"))
				{
					CRM = "C";
				}
				else if(crmList.toString().contains("M"))
				{
					CRM = "M";
				}
				else if(crmList.toString().contains("R"))
				{
					CRM = "R";
				}

				LOGGER.debug("ADDING SYSTEM TO DATAOBJECT RELATIONSHIP:     " + sub + " -----> {" + pred + " --- " + obj + "}");
				addToDataHash(new Object[]{sub, pred, obj});
				
				addToAllLabel(sub);
				addToAllLabel(pred);
				addToAllLabel(obj);

				LOGGER.debug("ADDING DATAOBJECT CRM RELATIONSHIP PROPERTY:     " + pred + " -----> {" + propertyURI + " --- " +  CRM + "}");
				addToDataHash(new Object[]{pred, propertyURI, CRM});
				addToAllRelationships(pred);
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// process software and hardware modules

	private void runGetListOfModules(String query, boolean softwareModule) 
	{
		ISelectWrapper sjsw = Utility.processQuery(coreDB, query);
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(vars[0]).toString();
			if(softwareModule)
			{
				allSoftwareModules.add(sub);
			}
			else
			{
				allHardwareModules.add(sub);
			}
		}
	}

	private void runHardwareSoftwareAggregation(String servicesQuery, String coreQuery, boolean softwareModule) 
	{
		dataHash.clear();
		removeDataHash.clear();
		allLabels.clear();

		ISelectWrapper sjswServices = Utility.processQuery(servicesDB, servicesQuery);
		processHardwareSoftwareProperties(sjswServices,  false, softwareModule);

		ISelectWrapper sjswCore = Utility.processQuery(coreDB, coreQuery);
		processHardwareSoftwareProperties(sjswCore, true, softwareModule);

		// processing modifies class variable dataHash directly
		deleteData(coreDB, removeDataHash);
		processData(coreDB, dataHash);
		processLabel(coreDB);
	}


	private void processHardwareSoftwareProperties(ISelectWrapper sjsw, boolean TAP_Core, boolean softwareModule)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			this.errorMessage = "";
			boolean aggregatedProp = false;
			
			ISelectStatement sjss = sjsw.next();
			String module = sjss.getRawVar(vars[0]).toString();
			String prop = sjss.getRawVar(vars[1]).toString();
			Object value = sjss.getRawVar(vars[2]);
			String user = "";
			if(!TAP_Core)
			{
				user = sjss.getRawVar(vars[6]).toString();
			}

			if(value == null)
			{
				addToAllConcepts(module);
			}
			else
			{
				if(dataHash.containsKey(module) || !TAP_Core)
				{
					Object[] returnTriple = new Object[3];

					if(prop.equals(semossPropertyBaseURI + "Quantity"))
					{
						returnTriple = processSumValues(module, prop, value);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Comments"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "EOL"))
					{
						boolean max = true;
						returnTriple = processMinMaxDate(module, prop, value, max);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Manufacturer"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Model"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Product_Type"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Master_Version"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Major_Version"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}
					else if(prop.equals(semossPropertyBaseURI + "Vendor"))
					{
						returnTriple = processConcatString(module, prop, value, user);
						aggregatedProp = true;
					}

					// if error occurs
					if(Arrays.equals(returnTriple, new String[]{""}))
					{
						String outuptToLog = "";
						if(!TAP_Core)
						{
							outuptToLog += "TAP_Services_DB ";
						}
						else
						{
							outuptToLog += "TAP_Core_DB ";
						}
						outuptToLog += module + " >>>>> " + prop + " >>>>> " + value.toString() + " >>>>> " + this.errorMessage;
						FILE_LOGGER.info(outuptToLog);
						aggregationSuccess = false;
					}
					// returnTriple never gets a value when the property being passed in isn't in the defined list above
					else if(returnTriple[0] != null)
					{
						LOGGER.debug("ADDING HARDWARE/SOFTWARE MODULE PROPERTY:     " + returnTriple[0] + " -----> {" + returnTriple[1] + " --- " + returnTriple[2].toString() + "}");
						addToDataHash(returnTriple);
						
						addToAllLabel(returnTriple[0].toString());
					}

					// must remove existing triple in TAP Core prior to adding
					if(TAP_Core & aggregatedProp)
					{
						if(value.toString().startsWith("\"") && value.toString().endsWith("\""))
						{
							value = value.toString().substring(1, value.toString().length()-1); // remove annoying quotes
						}
						addToDeleteHash(new String[]{module, prop, value.toString()});
					}

				}
			}
			// perform check to see if must add software/hardware version is software/hardware Module does not exist in TAP Core
			if(softwareModule)
			{
				if(!allSoftwareModules.contains(module))
				{
					String system = sjss.getRawVar(vars[3]).toString();
					String softwareV = sjss.getRawVar(vars[4]).toString();
					String software = sjss.getRawVar(vars[5]).toString();
					addToAllConcepts(system);
					addToAllConcepts(softwareV);
					addToAllConcepts(software);
					addToAllConcepts(module);

					addToAllLabel(system);
					addToAllLabel(softwareV);
					addToAllLabel(software);
					addToAllLabel(module);
					
					String baseUri = getBaseURI(system);
					//relationship from system to softwareModule
					String predSysToMod = baseUri + "/Relation/Consists/" + getTextAfterFinalDelimeter(system, "/") + ":" + getTextAfterFinalDelimeter(module, "/");
					addToAllRelationships(predSysToMod);
					LOGGER.debug("SYSTEM TO SOFTWARE MODULE RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + system + " -----> {" + predSysToMod + " --- " + module + "}");
					addToDataHash(new String[]{system, predSysToMod, module});
					addToAllLabel(predSysToMod);

					
					//relationship from softwareModule to softwareVersion
					String predModToVer = baseUri + "/Relation/TypeOf/" + getTextAfterFinalDelimeter(module, "/") + ":" + getTextAfterFinalDelimeter(softwareV, "/");
					addToAllRelationships(predModToVer);
					LOGGER.debug("SOFTWARE MODULE TO SOFTWARE VERSION RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + module + " -----> {" + predModToVer + " --- " + softwareV + "}");
					addToDataHash(new String[]{module, predModToVer, softwareV});
					addToAllLabel(predModToVer);
					
					//relationship from software to softwareVersion
					String predSoffToVer = baseUri + "/Relation/Has/" + getTextAfterFinalDelimeter(software, "/") + ":" + getTextAfterFinalDelimeter(softwareV, "/");
					addToAllRelationships(predSoffToVer);
					LOGGER.debug("SOFTWARE TO SOFTWARE VERSION RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + software + " -----> {" + predSoffToVer + " --- " + softwareV + "}");
					addToDataHash(new String[]{software, predSoffToVer, softwareV});
					addToAllLabel(predSoffToVer);
				}
			}
			else
			{
				if(!allHardwareModules.contains(module))
				{
					String system = sjss.getRawVar(vars[3]).toString();
					String hardwareV = sjss.getRawVar(vars[4]).toString();
					String hardware = sjss.getRawVar(vars[5]).toString();
					addToAllConcepts(system);
					addToAllConcepts(hardwareV);
					addToAllConcepts(hardware);
					addToAllConcepts(module);

					addToAllLabel(system);
					addToAllLabel(hardwareV);
					addToAllLabel(hardware);
					addToAllLabel(module);
					
					String baseUri = getBaseURI(system);
					//relationship from system to hardwareModule
					String predSysToMod = baseUri + "/Relation/Has/" + getTextAfterFinalDelimeter(system, "/") + ":" + getTextAfterFinalDelimeter(module, "/");
					addToAllRelationships(predSysToMod);
					LOGGER.debug("SYSTEM TO HARDWARE MODULE RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + system + " -----> {" + predSysToMod + " --- " + module + "}");
					addToDataHash(new String[]{system, predSysToMod, module});
					addToAllLabel(predSysToMod);
					
					//relationship from hardwareModule to hardwareVersion
					String predModToVer = baseUri + "/Relation/TypeOf/" + getTextAfterFinalDelimeter(module, "/") + ":" + getTextAfterFinalDelimeter(hardwareV, "/");
					addToAllRelationships(predModToVer);
					LOGGER.debug("HARDWARE MODULE TO HARDWARE VERSION RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + module + " -----> {" + predModToVer + " --- " + hardwareV + "}");
					addToDataHash(new String[]{module, predModToVer, hardwareV});
					addToAllLabel(predModToVer);

					//relationship from software to softwareVersion
					String predhARDToVer = baseUri + "/Relation/Has/" + getTextAfterFinalDelimeter(hardware, "/") + ":" + getTextAfterFinalDelimeter(hardwareV, "/");
					addToAllRelationships(predhARDToVer);
					LOGGER.debug("HARDWARE TO HARDWARE VERSION RELATIONSHIP DOES NOT EXIST IN TAP CORE");
					LOGGER.debug("ADDING:     " + hardware + " -----> {" + predhARDToVer + " --- " + hardwareV + "}");
					addToDataHash(new String[]{hardware, predhARDToVer, hardwareV});
					addToAllLabel(predhARDToVer);
				}
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// methods override from aggregation helper

	public void processNewConcepts()
	{
		// get list of all concepts from tap core
		HashSet<String> conceptList = new HashSet<String>();
		LOGGER.info("PROCESSING QUERY: " + TAP_CORE_CONCEPTS_LIST_QUERY);
		ISelectWrapper sjsw = Utility.processQuery(coreDB, TAP_CORE_CONCEPTS_LIST_QUERY);
		String[] var = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			conceptList.add(sjss.getRawVar(var[0]) + "");
		}

		for ( String obj : allConcepts.keySet())
		{
			for (String sub : allConcepts.get(obj) )
			{
				super.processNewConceptsAtInstanceLevel(coreDB, sub, obj);
			}
			// add concepts that are not already in db
			if(!conceptList.contains(obj))
			{
				addedToOwl = true;
				super.processNewConcepts(coreDB, obj);
			}
		}
	}

	private void processNewRelationships()
	{
		// get list of all relationships from tap core
		HashSet<String> relationshipList = new HashSet<String>();
		LOGGER.info("PROCESSING QUERY: " + TAP_CORE_RELATIONS_LIST_QUERY);
		ISelectWrapper sjsw = Utility.processQuery(coreDB, TAP_CORE_RELATIONS_LIST_QUERY);
		String[] var = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			relationshipList.add(sjss.getRawVar(var[0]) + "");
		}

		for ( String obj : allRelations.keySet())
		{
			for (String sub : allRelations.get(obj) )
			{
				super.processNewRelationshipsAtInstanceLevel(coreDB, sub, obj);	
			}
			// add relationships that are not already in db
			if(!relationshipList.contains(obj))
			{
				addedToOwl = true;
				super.processNewRelationships(coreDB, obj);
			}
		}	
	}	

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Unique methods for properties

	public Object[] processConcatString(String sub, String prop, Object value, String user) 
	{
		// replace any tags for properties that are loaded as other data types but should be strings
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#double","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#decimal","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#integer","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#float","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#boolean","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#dateTime","");
		if(value.toString().startsWith("\"") || value.toString().endsWith("\""))
		{
			value = value.toString().substring(1, value.toString().length()-1); // remove annoying quotes
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			if(!user.equals(""))
			{
				value = getTextAfterFinalDelimeter(user, "/") + ":" + value.toString();
			}
			LOGGER.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			if(!user.equals(""))
			{
				value = currentString.toString() + ";" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString();
			}
			else
			{
				value = currentString.toString() + ";" + value.toString();
			}
			LOGGER.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}

	public Object[] processConcatCommentString(String sub, String prop, Object value, String user) 
	{
		// replace any tags for properties that are loaded as other data types but should be strings
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#double","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#decimal","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#integer","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#float","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#boolean","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#dateTime","");

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			if(!user.equals(""))
			{
				value = getTextAfterFinalDelimeter(user, "/") + ":" + value.toString();
			}
			LOGGER.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			if(!user.equals(""))
			{
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString();
			}
			else
			{
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + value.toString();
			}
			LOGGER.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}

	private Object[] processGarrisonTheater(String sub, String prop, Object value)
	{
		value = value.toString().replaceAll("\"", "");
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			LOGGER.debug("ADDING GARRISONTHEATER:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object oldGT = innerHash.get(prop);
			if(!oldGT.toString().toString().equalsIgnoreCase(value.toString()))
			{
				value = "Both";
				LOGGER.debug("ADJUSTING GARRISONTHEATER:     " + sub + " -----> {" + prop + " --- " + "Both" + "}");
			}
		}
		return new Object[]{sub, prop, value};
	}

	private Object[] processTransactional(String sub, String prop, Object value)
	{
		value = value.toString().replaceAll("\"", "");
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			LOGGER.debug("ADDING TRANSACTIONAL:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		//Different SystemServices should not be sending different transactional value
		//perform check to make sure data is correct
		else
		{
			innerHash = dataHash.get(sub);
			Object currentTransactional = innerHash.get(prop);
			if(!currentTransactional.toString().toString().equalsIgnoreCase(value.toString()))
			{
				this.errorMessage = "Error Processing Transactional!  Conflicting report from systems. " 
						+ "Error occured processing: " + sub + " >>>> " + prop + " >>>> " + value;				
				return new Object[]{""};
			}
		}
		return new Object[]{sub, prop, value};
	}

	private Object[] processDFreq(String sub, String prop, Object value) 
	{
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			LOGGER.debug("ADDING DFREQ:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			String[] frequencies = new String[]{value.toString().replaceAll("\"", ""), innerHash.get(prop).toString().replaceAll("\"", "")};
			Integer[] currentFreqValue = new Integer[2];

			for(int i = 0; i < frequencies.length; i++)
			{
				if(frequencies[i].equalsIgnoreCase("Real-time (user-initiated)")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (monthly)")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("Weekly")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Monthly")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("Batch (daily)")) currentFreqValue[i] = 24;
				else if(frequencies[i].equalsIgnoreCase("Batch(Daily)")) currentFreqValue[i] = 24;
				else if(frequencies[i].equalsIgnoreCase("Real-time")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Transactional")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("On Demand")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Event Driven (seconds-minutes)")) currentFreqValue[i] = 60;
				else if(frequencies[i].equalsIgnoreCase("TheaterFramework")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Event Driven (Seconds)")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Web services")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("TF")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (12/day)")) currentFreqValue[i] = 2;
				else if(frequencies[i].equalsIgnoreCase("SFTP")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (twice monthly)")) currentFreqValue[i] = 360;
				else if(frequencies[i].equalsIgnoreCase("Daily")) currentFreqValue[i] = 24;
				else if(frequencies[i].equalsIgnoreCase("Hourly")) currentFreqValue[i] = 1;
				else if(frequencies[i].equalsIgnoreCase("Near Real-time (transaction initiated)")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (three times a week)")) currentFreqValue[i] = 56;
				else if(frequencies[i].equalsIgnoreCase("Batch (weekly)")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Near Real-time")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Real Time")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (bi-monthly)")) currentFreqValue[i] = 1440;
				else if(frequencies[i].equalsIgnoreCase("Batch (semiannually)")) currentFreqValue[i] = 4392;
				else if(frequencies[i].equalsIgnoreCase("Event Driven (Minutes-hours)")) currentFreqValue[i] = 1;
				else if(frequencies[i].equalsIgnoreCase("Annually")) currentFreqValue[i] = 8760;
				else if(frequencies[i].equalsIgnoreCase("Batch(Monthly)")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("Bi-Weekly")) currentFreqValue[i] = 336;
				else if(frequencies[i].equalsIgnoreCase("Daily at end of day")) currentFreqValue[i] = 24;
				else if(frequencies[i].equalsIgnoreCase("TCP")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("event-driven (Minutes-hours)")) currentFreqValue[i] = 1;
				else if(frequencies[i].equalsIgnoreCase("Interactive")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Weekly Quarterly")) currentFreqValue[i] = 2184;
				else if(frequencies[i].equalsIgnoreCase("Weekly Daily Weekly Weekly Weekly Weekly Daily Daily Daily")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Weekly Daily")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Periodic")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (4/day)")) currentFreqValue[i] = 6;
				else if(frequencies[i].equalsIgnoreCase("Batch(Daily/Monthly)")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("Weekly; Interactive; Interactive")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("interactive")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch (quarterly)")) currentFreqValue[i] = 2184;
				else if(frequencies[i].equalsIgnoreCase("Every 8 hours (KML)/On demand (HTML)")) currentFreqValue[i] = 8;
				else if(frequencies[i].equalsIgnoreCase("Monthly at beginning of month, or as user initiated")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("On demad")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Monthly Bi-Monthly Weekly Weekly")) currentFreqValue[i] = 720;
				else if(frequencies[i].equalsIgnoreCase("Quarterly")) currentFreqValue[i] = 2184;
				else if(frequencies[i].equalsIgnoreCase("On-demand")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("user upload")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("1/hour (KML)/On demand (HTML)")) currentFreqValue[i] = 1;
				else if(frequencies[i].equalsIgnoreCase("DVD")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Real-time ")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Weekly ")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Annual")) currentFreqValue[i] = 8760;
				else if(frequencies[i].equalsIgnoreCase("Daily Interactive")) currentFreqValue[i] = 24;
				else if(frequencies[i].equalsIgnoreCase("NFS, Oracle connection")) currentFreqValue[i] = 0;
				else if(frequencies[i].equalsIgnoreCase("Batch(Weekly)")) currentFreqValue[i] = 168;
				else if(frequencies[i].equalsIgnoreCase("Batch(Quarterly)")) currentFreqValue[i] = 2184;
				else if(frequencies[i].equalsIgnoreCase("Batch (yearly)")) currentFreqValue[i] = 8760;
				else if(frequencies[i].equalsIgnoreCase("Each user login instance")) currentFreqValue[i] = 0;
			}

			if(currentFreqValue[0] == null || currentFreqValue[1] == null)
			{
				this.errorMessage = "Error Processing DFreq!  Check frequency is predefined in list. " 
						+ "Error occured processing: " + sub + " >>>> " + prop + " >>>> " + value;	
				return new String[]{""};
			}

			if(currentFreqValue[0] > currentFreqValue[1])
			{
				value = innerHash.get(prop);
			}
			// else, do not change the value to keep the one being inputed
			LOGGER.debug("ADJUSTING DFREQ:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}


	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Utility methods 

	private HashMap<String, HashMap<String, LinkedList<Object>>> runAggregateAllData(ISelectWrapper sjsw, HashMap<String, HashMap<String, LinkedList<Object>>> aggregatedData, String propType, boolean TAP_Core)
	{
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			// get the next row and see how it must be added to the insert query
			String sys = sjss.getRawVar(vars[0]).toString();
			String pred = sjss.getRawVar(vars[1]).toString();
			String obj = sjss.getRawVar(vars[2]).toString();
			Object prop = sjss.getRawVar(vars[3]);

			if(!TAP_Core)
			{
				pred = pred.substring(0, pred.lastIndexOf("/")) + "/" + getTextAfterFinalDelimeter(sys, "/") +":" + getTextAfterFinalDelimeter(obj, "/");
			}

			if(prop == null && !TAP_Core)
			{
				addToAllConcepts(sys);
				addToAllConcepts(obj);
				addToAllRelationships(pred);
				addToDataHash(new String[]{sys, pred, obj});
			}
			else
			{
				if(aggregatedData.containsKey(sys) || !TAP_Core)
				{
					LinkedList<Object> dataList = new LinkedList<Object>();
					HashMap<String, LinkedList<Object>> innerHash = new HashMap<String, LinkedList<Object>>();
					if(!aggregatedData.containsKey(sys))
					{
						dataList.add(pred);
						dataList.add(prop);
						innerHash.put(obj, dataList);
						aggregatedData.put(sys, innerHash);
						LOGGER.debug("ADDING NEW DATA LIST:     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
					}
					else
					{
						if(!aggregatedData.get(sys).containsKey(obj))
						{
							innerHash = aggregatedData.get(sys);
							dataList.add(pred);
							dataList.add(prop);
							innerHash.put(obj, dataList);
							LOGGER.debug("ADDING NEW DATA LIST:     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
						}
						else
						{
							innerHash = aggregatedData.get(sys);
							dataList = innerHash.get(obj);
							dataList.add(prop);
							LOGGER.debug("ADJUSTING DATA LIST:     " + sys + " -----> {" + obj + " --- " + dataList.toString() + "}");
						}
					}

					// add instances to master list
					addToAllConcepts(sys);
					addToAllConcepts(obj);

					if(TAP_Core)
					{
						if(prop.toString().startsWith("\"") && prop.toString().endsWith("\""))
						{
							prop = prop.toString().substring(1, prop.toString().length()-1); // remove annoying quotes
						}
						addToDeleteHash(new Object[]{pred, semossPropertyBaseURI + propType, prop});
					}
					addToAllRelationships(pred);
				}
			}
		}
		return aggregatedData;
	}
}
