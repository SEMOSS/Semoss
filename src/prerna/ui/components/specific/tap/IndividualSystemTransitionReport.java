/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.specific.IndividualSystemTransitionReportWriter;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.specific.tap.AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK;
import prerna.util.Constants;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;


//<@System-http://semoss.org/ontologies/Concept/System@>$Non-Contract
//prerna.ui.components.specific.tap.IndividualSystemTransitionReport

@SuppressWarnings("serial")
public class IndividualSystemTransitionReport extends TablePlaySheet{
	
	private static final Logger classLogger = LogManager.getLogger(IndividualSystemTransitionReport.class);

	// list of queries to run
	private String sysInfoQuery = "SELECT DISTINCT ?Description (COALESCE(?GT, 'Garrison') AS ?GarrisonTheater) (IF(BOUND(?MU),?MU,'TBD') AS ?MHS_Specific) ?Transaction_Count (COALESCE(SUBSTR(STR(?ATO),0,10),'TBD') AS ?ATO_Date) (COALESCE(SUBSTR(STR(?ES),0,10),'TBD') AS ?End_Of_Support) ?Num_Users ?POC WHERE   {   BIND(@SYSTEM@ AS ?System)  {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> ?MU}}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}}  OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?Transaction_Count}}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?ES}}   OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?Num_Users}}  OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/POC> ?POC}}   }";
	private String sysSORDataWithDHMSMQuery = "SELECT DISTINCT ?Data ?DHMSM_SOR WHERE { {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}  {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}  {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}  {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability}{?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task}{?Task ?Needs ?Data}  } } } UNION {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE {BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>}  {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM)  {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}  {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}  {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task}{?Task ?Needs ?Data} } }} } ORDER BY ?System";
	private String sysSORDataWithDHMSMCapQuery = "SELECT DISTINCT ?Capability ?Data (GROUP_CONCAT(DISTINCT ?Comment ; Separator = ' and ') AS ?Comments) WHERE { { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> }{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?SystemProvideData_CRM} FILTER( !regex(str(?SystemProvideData_CRM),'R')) {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} {?Task ?Needs ?Data} BIND('ICD validated through downstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM ) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } UNION { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM){?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?Task ?Needs ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } {?System <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data } FILTER(!BOUND(?icd2)) BIND('Interface implied through downstream and no upstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?DHMSM ) {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } } GROUP BY ?Capability ?Data ?System ORDER BY ?Capability ?Data ?System";
	private String softwareLifeCycleQuery = "SELECT DISTINCT ?System ?SoftwareVersion (IF( bound(?SoftMEOL) && datatype(?SoftMEOL) = xsd:dateTime, ?SoftMEOL, IF( bound(?SoftVEOL) && datatype(?SoftVEOL) = xsd:dateTime, ?SoftVEOL, 'TBD')) AS ?SupportDate) (IF( bound(?SoftMEOL) && datatype(?SoftMEOL) = xsd:dateTime, ?SoftMEOL, IF( bound(?SoftVEOL) && datatype(?SoftVEOL) = xsd:dateTime, ?SoftVEOL, 'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemSWBudget) WHERE { BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?System <http://semoss.org/ontologies/Relation/Consists> ?SoftwareModule} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?quantity} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?SoftwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?SoftwareVersion} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVEOL} OPTIONAL {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMEOL} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?SoftwareVersion";
	private String hardwareLifeCycleQuery = "SELECT DISTINCT ?System ?HardwareVersion (IF( bound(?HardMEOL) && datatype(?HardMEOL) = xsd:dateTime, ?HardMEOL, IF( bound(?HardVEOL) && datatype(?HardVEOL) = xsd:dateTime, ?HardVEOL, 'TBD')) AS ?SupportDate) (IF( bound(?HardMEOL) && datatype(?HardMEOL) = xsd:dateTime, ?HardMEOL, IF( bound(?HardVEOL) && datatype(?HardVEOL) = xsd:dateTime, ?HardVEOL, 'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemHWBudget) WHERE { BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?System <http://semoss.org/ontologies/Relation/Has> ?HardwareModule} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?quantity} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?HardwareModule <http://semoss.org/ontologies/Relation/TypeOf> ?HardwareVersion } OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVEOL} OPTIONAL {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMEOL} OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?HardwareVersion";
	private String hwSWBudgetQuery = "SELECT DISTINCT ?System ?GLTag (max(coalesce(?FY15,0)) as ?fy15) WHERE {BIND(@SYSTEM@ AS ?System) { {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem}{?SystemBudgetGLItem ?TaggedBy ?GLTag}{?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15)} UNION {{?SystemServiceBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem> ;} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService> ;}{?ConsistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>;} {?System ?ConsistsOf ?SystemService}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?SystemService ?Has ?SystemServiceBudgetGLItem}{?SystemServiceBudgetGLItem ?TaggedBy ?GLTag}{?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemServiceBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) } }GROUP BY ?System ?GLTag BINDINGS ?GLTag {(<http://health.mil/ontologies/Concept/GLTag/SW_Licen>) (<http://health.mil/ontologies/Concept/GLTag/HW>)}";
	private String sysBLUQuery = "SELECT DISTINCT ?BLU ?Description WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}OPTIONAL{?BLU <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}{?System ?provide ?BLU} }";
	private String sysDataQuery = "SELECT DISTINCT ?DataObject ?CRM WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?DataObject}}";
	// making the System Data sheet
	private String sysSORDataQuery = "SELECT DISTINCT ?Data ?ProvideOrConsume (GROUP_CONCAT(?Ser; SEPARATOR = ', ') AS ?Services) ?otherSystem WHERE {SELECT DISTINCT ?Data ?ProvideOrConsume ?Ser ?otherSystem WHERE {{SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} OPTIONAL {{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data}   FILTER(!BOUND(?icd2)) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} BIND('Consume' AS ?ProvideOrConsume)}} }} GROUP BY ?Data ?ProvideOrConsume ?otherSystem ORDER BY DESC(?ProvideOrConsume) ?Data";
//	private String otherSysSORDataQuery = "SELECT DISTINCT ?System ?Data (COUNT(?icd) AS ?DownstreamInterfaces) WHERE { {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?downstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?System <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?downstreamSystem} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} }UNION {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}{?downstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?System <http://semoss.org/ontologies/Relation/Provide> ?icd }{?icd <http://semoss.org/ontologies/Relation/Consume> ?downstreamSystem}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) }FILTER (?System != @SYSTEM@)} GROUP BY ?System ?Data ORDER BY ?Data ?System";
	private String otherSysSORDataQuery = "SELECT DISTINCT ?System ?Data (COUNT(DISTINCT(?icd)) AS ?DownstreamInterfaces) WHERE { {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> } {?downstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?System <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?downstreamSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}}} GROUP BY ?System ?Data ORDER BY ?Data ?System";
	//hpi requires site/region information
	private String siteQuery = "SELECT DISTINCT (COUNT(DISTINCT ?DCSite) AS ?SiteCount) (SAMPLE(?DCSite) AS ?ExampleSite) (GROUP_CONCAT(DISTINCT ?Reg ; SEPARATOR = ', ') AS ?Regions) WHERE { SELECT DISTINCT ?System ?DCSite (CONCAT(SUBSTR(STR(?Region),58)) AS ?Reg) WHERE { BIND(@SYSTEM@ AS ?System) {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} OPTIONAL{ {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} {?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF} {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HealthServiceRegion>} {?MTF <http://semoss.org/ontologies/Relation/Located> ?Region} }} ORDER BY ?Region} GROUP BY ?System";
	private String deploymentQuery = "SELECT DISTINCT ?System ?DCSite ?IOCSite { BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} OPTIONAL { {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>;} BIND(<http://health.mil/ontologies/Concept/Wave/IOC> AS ?Wave) {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} } BIND(IF(BOUND(?Wave),\"Yes\",\"\") AS ?IOCSite) }";
	// future db queries
	private String proposedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedSystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ";
	private String decommissionedFutureICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data  (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?Recommendation WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ProposedDecommissionedSystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> ?Recommendation;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } }";
	private String allPresentICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;}  OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ORDER BY ?Interface";	//store interface query results
	private String minimalICDQuery = "SELECT DISTINCT (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Interface WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} BIND(@SYSTEM@ AS ?System) { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } UNION { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} } } ORDER BY ?Interface";	//store interface query results
	private String minimalDownstreamQuery = "SELECT DISTINCT ?DownstreamSys ?Data WHERE { {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} BIND(@SYSTEM@ AS ?System) {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } ORDER BY ?Interface";
	private String allICDQuery =  "SELECT DISTINCT (COALESCE(?UpstreamSys, ?DownstreamSys) AS ?System) ?Type ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) (COALESCE(?UpstreamSys, ?Sys) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?Sys) AS ?DownstreamSystem)  WHERE {   	{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}   	{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}   	{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}  	{?Interface ?carries ?Data;} 	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}   	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;}   	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;}   	BIND(@SYSTEM@ AS ?Sys)   	{   		{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}   		{?Sys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   		{?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;}   		BIND('Downstream' As ?Type)  	}   	UNION   	{   		{?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}   		{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   		{?Interface <http://semoss.org/ontologies/Relation/Consume> ?Sys ;}   		BIND('Upstream' As ?Type)  	}   } ORDER BY ?SYSTEM ?Interface";
	private String allICDQueryHIGH =  "SELECT DISTINCT (COALESCE(?UpstreamSys, ?DownstreamSys) AS ?System) ?Type ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol)  WHERE {   	{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}   	{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}   	{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}  	{?Interface ?carries ?Data;} 	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}   	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;}   	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;}   	BIND(@SYSTEM@ AS ?Sys)   	{   		{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}   		{?Sys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   		{?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;}   		BIND('Downstream' As ?Type)  	}   	UNION   	{   		{?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}   		{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   		{?Interface <http://semoss.org/ontologies/Relation/Consume> ?Sys ;}   		BIND('Upstream' As ?Type)  	}   } ORDER BY ?SYSTEM ?Interface";
	
	private String activityQuery = "SELECT DISTINCT ?System ?Activity WHERE { BIND(@SYSTEM@ AS ?System) {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System <http://semoss.org/ontologies/Relation/Supports> ?Activity} }";
	private String dataElementQuery = "SELECT DISTINCT ?System ?DataObject ?DataElement WHERE { BIND(@SYSTEM@ AS ?System) {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?System <http://semoss.org/ontologies/Relation/Provide> ?DataObject } OPTIONAL { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Carries>} {?DataElement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataElement>} {?ICD ?Carries ?DataElement} {?BelongsTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>} {?DataElement ?BelongsTo ?DataObject} { {?System <http://semoss.org/ontologies/Relation/Provide> ?ICD} } UNION { {?ICD <http://semoss.org/ontologies/Relation/Consume> ?System } } } BIND(IF(!BOUND(?DataElement), \"None Captured\", ?DataElement) as ?DataElement) }";
	private String dataSMESORQuery = "SELECT DISTINCT ?System ?DataObject WHERE { BIND(@SYSTEM@ AS ?System) {?MigrationReference <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SourceType>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?DataObjectSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource>;} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?System <http://semoss.org/ontologies/Relation/Designated> ?DataObjectSource} {?DataObjectSource <http://semoss.org/ontologies/Relation/LabeledAs> ?MigrationReference} {?DataObjectSource <http://semoss.org/ontologies/Relation/Delivers> ?DataObject} }";
	private String dataSMESORSystemQuery = "SELECT DISTINCT ?UpstreamSystem ?DataObject WHERE { BIND(@SYSTEM@ AS ?System) {?MigrationReference <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SourceType>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?DataObjectSource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource>;} {?DataObject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?System <http://semoss.org/ontologies/Relation/Designated> ?DataObjectSource} {?DataObjectSource <http://semoss.org/ontologies/Relation/LabeledAs> ?MigrationReference} {?DataObjectSource <http://semoss.org/ontologies/Relation/Delivers> ?DataObject} {?UpstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?UpstreamSystem <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} {?Interface <http://semoss.org/ontologies/Relation/Payload> ?DataObject } }";
		
	private final String SYS_URI_PREFIX = "http://health.mil/ontologies/Concept/System/";

	private IDatabaseEngine TAP_Core_Data;
	private IDatabaseEngine TAP_Cost_Data;
	private IDatabaseEngine TAP_Site_Data;
	private IDatabaseEngine FutureDB;

	private final String[] dates = new String[]{"2016&June","2017&April","2022&July"};

	private String systemURI = "";
	private String systemName = "";
	private String reportType = "";

	private boolean showMessages = true;
	
	public void setSystemName(String systemName) {
		this.systemName = systemName;
	}
	
	private void processQueryParts(String query) {
		String[] systemAndType = this.query.split("\\$");
		systemURI = systemAndType[0];
		reportType = systemAndType[1];
		systemName = systemURI.substring(systemURI.lastIndexOf("/")+1,systemURI.lastIndexOf(">"));
	}
	
	private void specifySysInQueriesForReport() 
	{		
		if(systemURI.equals("")){
			processQueryParts(this.query);
		}
		sysInfoQuery = sysInfoQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMQuery = sysSORDataWithDHMSMQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMCapQuery = sysSORDataWithDHMSMCapQuery.replace("@SYSTEM@", systemURI);
		softwareLifeCycleQuery = softwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		hardwareLifeCycleQuery = hardwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		sysSORDataQuery = sysSORDataQuery.replace("@SYSTEM@", systemURI);
		otherSysSORDataQuery = otherSysSORDataQuery.replace("@SYSTEM@", systemURI);
		sysBLUQuery = sysBLUQuery.replace("@SYSTEM@",systemURI);
		sysDataQuery = sysDataQuery.replace("@SYSTEM@",systemURI);
		proposedFutureICDQuery = proposedFutureICDQuery.replace("@SYSTEM@",systemURI);
		decommissionedFutureICDQuery = decommissionedFutureICDQuery.replace("@SYSTEM@",systemURI);
		allPresentICDQuery = allPresentICDQuery.replace("@SYSTEM@",systemURI);
		allICDQuery = allICDQuery.replace("@SYSTEM@",systemURI);
		allICDQueryHIGH = allICDQueryHIGH.replace("@SYSTEM@",systemURI);
		minimalICDQuery = minimalICDQuery.replace("@SYSTEM@",systemURI);
		dataElementQuery = dataElementQuery.replace("@SYSTEM@",systemURI);
		minimalDownstreamQuery = minimalDownstreamQuery.replace("@SYSTEM@",systemURI);
		deploymentQuery = deploymentQuery.replace("@SYSTEM@",systemURI);
		activityQuery = activityQuery.replace("@SYSTEM@",systemURI);
		
		dataSMESORQuery=dataSMESORQuery.replace("@SYSTEM@",systemURI);
		dataSMESORSystemQuery=dataSMESORSystemQuery.replace("@SYSTEM@",systemURI);
		
	}

	@Override
	public void createData() {

		try{
			TAP_Core_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(TAP_Core_Data==null)
				throw new IllegalArgumentException("Database not found");
		} catch(IllegalArgumentException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report.");
			return;
		}
		
		try{
			FutureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
			if(FutureDB==null)
				throw new IllegalArgumentException("Database not found");
		} catch(IllegalArgumentException e) {
			Utility.showError("Could not find necessary database: FutureDB. Cannot generate report.");
			return;
		}

		processQueryParts(query);
		specifySysInQueriesForReport();
		
		boolean includeCosts = true;
		String exceptionError = "Could not find database:";
		try {
			TAP_Cost_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
			TAP_Site_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
			if(TAP_Cost_Data==null) {
				exceptionError = exceptionError.concat("\nTAP_Cost_Data: Report will not include cost data"); 
				throw new IllegalArgumentException(exceptionError);
			}
			if(TAP_Site_Data==null) {
				exceptionError = exceptionError.concat("\nTAP_Site_Data: Report will not include site data"); 
				throw new IllegalArgumentException(exceptionError);
			}
		} catch(IllegalArgumentException e) {
			Utility.showError(exceptionError);
			includeCosts=false;
		}

		Map<String, Object> hwSWBudgetHash = new HashMap<String, Object>();
		if(includeCosts) {
			hwSWBudgetHash = getHWSWCostInfo();
		}

		Map<String, Object> sysInfoHash = getSysInfo();
		Map<String, Object> sysSiteHash = new HashMap<String,Object>();
		if(reportType.equals("High")) {
			siteQuery = siteQuery.replace("@SYSTEM@", systemURI);
			sysSiteHash = getQueryDataWithHeaders(TAP_Site_Data, siteQuery);
		}

		Map<String, Object> sysSORDataWithDHMSMHash = getQueryDataWithHeaders(TAP_Core_Data, sysSORDataWithDHMSMQuery);
		Map<String, Object> sysSORDataWithDHMSMCapHash = getQueryDataWithHeaders(TAP_Core_Data, sysSORDataWithDHMSMCapQuery);
		Map<String, Object> sysSORTableHash = getSysSORTableWithHeaders(TAP_Core_Data,sysSORDataQuery,otherSysSORDataQuery);
		Map<String, Object> sysBLUHash = getQueryDataWithHeaders(TAP_Core_Data,sysBLUQuery);
		Map<String, Object> sysDataHash = getQueryDataWithHeaders(TAP_Core_Data,sysDataQuery);
		
		Map<String, Object> sysProposedFutureICD = getQueryDataWithHeaders(FutureDB, proposedFutureICDQuery);
		Map<String, Object> sysDecommissionedFutureICD = getQueryDataWithHeaders(FutureDB, decommissionedFutureICDQuery);
		Map<String, Object> sysPersistentICD = determinePersistentICDs(sysDecommissionedFutureICD);
		
		Map<Integer, Map<String, Object>> storeSoftwareData = processHWSWData(softwareLifeCycleQuery);
		Map<Integer, Map<String, Object>> storeHardwareData = processHWSWData(hardwareLifeCycleQuery);
		Map<String, Object> softwareBarHash = createHWSWBarHash(storeSoftwareData.get(0));
		Map<String, Object> hardwareBarHash = createHWSWBarHash(storeHardwareData.get(0));
		Map<String, Object> allICDs = getQueryDataWithHeaders(TAP_Core_Data, allICDQuery);
		
		Map<String, Object> sysLPInterfaceWithCostHash = new HashMap<String, Object>();
		Map<String, Object> interfaceBarHash = new HashMap<String, Object>();
		Map<String, Object> minimalICDHash = getQueryDataWithHeaders(TAP_Core_Data, minimalICDQuery);
		Map<String, Object> dataElementHash = getQueryDataWithHeaders(TAP_Core_Data, dataElementQuery);
		Map<String, Object> minimalDownstreamHash = getQueryDataWithHeaders(TAP_Core_Data, minimalDownstreamQuery);
		Map<String, Object> deploymentHash = getQueryDataWithHeaders(TAP_Site_Data, deploymentQuery);
		Map<String, Object> activityHash  = getQueryDataWithHeaders(TAP_Core_Data, activityQuery);
		
		Map<String, Object> dataSMESORSystemHash  = getQueryDataWithHeaders(TAP_Core_Data, dataSMESORSystemQuery);
		Map<String, Object> dataSMESORHash  = getQueryDataWithHeaders(TAP_Core_Data, dataSMESORQuery);
		
		try {
			sysLPInterfaceWithCostHash = calculateInterfaceModernizationCost();
			// perform after the above to improve performance
			interfaceBarHash = createInterfaceBarChart(sysLPInterfaceWithCostHash);
		} catch (IOException e) {
			Utility.showError(e.getMessage());
		}
		
		boolean success = writeReport(sysInfoHash,
				sysSiteHash,
				storeSoftwareData,
				storeHardwareData,
				hwSWBudgetHash,
				sysSORDataWithDHMSMHash,
				sysSORDataWithDHMSMCapHash,
				sysBLUHash,
				sysSORTableHash, 
				softwareBarHash, 
				hardwareBarHash, 
				interfaceBarHash, 
				sysDataHash, 
				sysProposedFutureICD, 
				sysDecommissionedFutureICD, 
				sysPersistentICD,
				allICDs,
				minimalICDHash,
				dataElementHash,
				minimalDownstreamHash,
				deploymentHash,
				activityHash,
				dataSMESORSystemHash,
				dataSMESORHash);
		
		if(showMessages)
		{
			if(success){
				Utility.showMessage("System Export Finished! File located in:\n" + IndividualSystemTransitionReportWriter.getFileLoc() );
			} else {
				Utility.showError("Error Creating Report!");
			}
		}
	}

	private boolean writeReport(Map<String, Object> sysInfoHash,
			Map<String, Object> sysSiteHash,
			Map<Integer, Map<String, Object>> storeSoftwareData,
			Map<Integer, Map<String, Object>> storeHardwareData,
			Map<String, Object> hwSWBudgetHash,
			Map<String, Object> sysSORDataWithDHMSMHash,
			Map<String, Object> sysSORDataWithDHMSMCapHash,
			Map<String, Object> sysBLUHash,
			Map<String, Object> sysSORTableHash,
			Map<String, Object> softwareBarHash,
			Map<String, Object> hardwareBarHash,
			Map<String, Object> interfaceBarHash,
			Map<String, Object> sysDataHash,
			Map<String, Object> sysProposedFutureICD,
			Map<String, Object> sysDecommissionedFutureICD,
			Map<String, Object> sysPersistentICD,
			Map<String, Object> allICDs,
			Map<String, Object> minimalICDHash,
			Map<String, Object> dataElementHash,
			Map<String, Object> minimalDownstreamHash,
			Map<String, Object> deploymentHash,
			Map<String, Object> activityHash,
			Map<String, Object> dataSMESORSystemHash,
			Map<String, Object> dataSMESORHash) {
		IndividualSystemTransitionReportWriter writer = new IndividualSystemTransitionReportWriter();
		String templateFileName = "";
		
		//Grab workbook from Template
		if(reportType.equals("LPI"))
			templateFileName = "Individual_System_LPI_Transition_Report_Template.xlsx";
		else if(reportType.equals("LPNI"))
			templateFileName = "Individual_System_LPNI_Transition_Report_Template.xlsx";
		else if(reportType.equals("High"))
			templateFileName = "Individual_System_High_Transition_Report_Template.xlsx";
		else if(reportType.equals("DeepDive"))
			templateFileName = "Individual_System_DeepDive_Transition_Report_Template.xlsx";
		else if(reportType.equals("Non-Contract"))
			templateFileName = "NON-contract_workbook_Template.xlsx";
		
		
		
		if( reportType.equals("LPI") || reportType.equals("LPNI") || reportType.equals("High"))
		{
			writer.makeWorkbook(Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")),templateFileName);
			writer.writeListSheet("Future Interface Development", sysProposedFutureICD);
			writer.writeListSheet("Future Interface Decommission", sysDecommissionedFutureICD);
			writer.writeListSheet("Future Interface Sustainment", sysPersistentICD);
			writer.writeSystemInfoSheet("System Overview",sysInfoHash);
			if(reportType.equals("High"))
				writer.writeSystemSiteDetails("System Overview",sysSiteHash);
			writer.writeHWSWSheet("Software Lifecycles", storeSoftwareData.get(0), storeSoftwareData.get(1), storeSoftwareData.get(2));
			writer.writeHWSWSheet("Hardware Lifecycles", storeHardwareData.get(0), storeHardwareData.get(1), storeHardwareData.get(2));
	//		writer.writeModernizationTimelineSheet("Modernization Timeline", storeSoftwareData.get(0), storeHardwareData.get(0), hwSWBudgetHash);
			writer.writeListSheet("SOR Overlap With MHS GENESIS", sysSORDataWithDHMSMHash);
	//		writer.writeListSheet("DHMSM Data Requirements", sysSORDataWithDHMSMCapHash);
	//		writer.writeListSheet("System Interfaces", sysLPInterfaceWithCostHash);
			writer.writeListSheet("System BLU", sysBLUHash);
			
			writer.writeSORSheet("System Data",sysDataHash);
			writer.writeBarChartData("Summary Charts",3,softwareBarHash);
			writer.writeBarChartData("Summary Charts",11,hardwareBarHash);
			writer.writeBarChartData("Summary Charts",19,interfaceBarHash);
		}
		else if(reportType.equals("DeepDive"))
		{
			writer.makeWorkbook(Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")),templateFileName);
			writer.writeListSheet("Future Interface Development", sysProposedFutureICD);
			writer.writeListSheet("Future Interface Decommission", sysDecommissionedFutureICD);
			writer.writeListSheet("Future Interface Sustainment", sysPersistentICD);
			writer.writeSystemInfoSheetShort("System Overview",sysInfoHash);
			writer.writeListSheet("Current State Interfaces", allICDs);
		}
		else if(reportType.equals("Non-Contract"))
		{
			DHMSMIntegrationTransitionCostWriter CostWriter;
			XSSFWorkbook wb;
			try {
				//Utilize DHMSMIntegrationTransitionCostWriter to fulfill it's piece
				CostWriter = new DHMSMIntegrationTransitionCostWriter();
				CostWriter.setSysURI(systemURI.replace(">", "").replace("<", ""));
				CostWriter.calculateValuesForReport();
				wb = CostWriter.generateWB(templateFileName);
				
				//Move to IndividualTransitionReportWriter
				writer.makeWorkbook(Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")),wb, "Non-Contract");
				printout(deploymentHash);
				writer.writeListSheet("Transition Cost Estimates",removeColumn(removeColumn(sysProposedFutureICD,3),6), 35, 0);
				writer.writeListSheet("Current Interfaces", minimalICDHash,3,0);
				writer.writeListSheet("Subset DE of System", dataElementHash,1,0,4);
				writer.writeListSheet("Subset DE of System", minimalDownstreamHash,2,7);
				writer.writeListSheet("Deployment", deploymentHash,1,0);
				writer.writeListSheet("Activities", activityHash, 6,0);
				
				writer.writeListSheet("SOR for Data Objects", dataSMESORHash,2,0,3);
				writer.writeListSheet("SOR for Data Objects", dataSMESORSystemHash,4,6);
				
				writer.replaceAll("@SYSTEM@",Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				Utility.showError(e.getMessage());
			}
			
			
		}
		
		
		return writer.writeWorkbook();
	}

	private Map<String, Object> determinePersistentICDs(Map<String, Object> decommissionedFutureICDs) {
		Map<String, Object> retHash = new HashMap<String, Object>();
		
		Map<String, Object> allICDs = getQueryDataWithHeaders(TAP_Core_Data, allPresentICDQuery);
		ArrayList<Object[]> allData = (ArrayList<Object[]>) allICDs.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> removedData = (ArrayList<Object[]>) decommissionedFutureICDs.get(DHMSMTransitionUtility.DATA_KEY);
		
		Iterator<Object[]> itOverArray = allData.iterator();
		while(itOverArray.hasNext()) {
			Object[] icdRow = itOverArray.next();
			INNER: for(Object[] removeICD : removedData) {
				if(icdRow[2].toString().equals(removeICD[2].toString())) {
					itOverArray.remove();
					break INNER;
				}
			}
		}
		
		retHash.put(DHMSMTransitionUtility.HEADER_KEY, allICDs.get(DHMSMTransitionUtility.HEADER_KEY));
		retHash.put(DHMSMTransitionUtility.DATA_KEY, allData);
		
		return retHash;
	}

	private Map<Integer, Map<String, Object>> processHWSWData(String query) 
	{
		Map<Integer, Map<String, Object>> storeData = new HashMap<Integer, Map<String, Object>>();

		LifeCycleGridPlaySheet getSoftwareHardwareData = new LifeCycleGridPlaySheet();
		for(int i = 0; i < dates.length; i++)
		{
			getSoftwareHardwareData.engine = TAP_Core_Data;
			getSoftwareHardwareData.setQuery(dates[i] + "&" + query);
			ArrayList<Object[]> dataRow = getSoftwareHardwareData.processQuery(getSoftwareHardwareData.getQuery());
			String[] names = getSoftwareHardwareData.getNames();
			dataRow  = removeSystemFromArrayList(dataRow);
			names = removeSystemFromStringArray(names);
			HashMap<String, Object> innerMap = new HashMap<String, Object>();
			innerMap.put(DHMSMTransitionUtility.HEADER_KEY, names);
			innerMap.put(DHMSMTransitionUtility.DATA_KEY, dataRow);
			storeData.put(i, innerMap);
		}

		return storeData;
	}

	public Map<String, Object> calculateInterfaceModernizationCost() throws IOException 
	{
		LPInterfaceCostProcessor sysInterfaceData = new LPInterfaceCostProcessor();
		sysInterfaceData.setEngine(TAP_Core_Data);
		return sysInterfaceData.generateSystemTransitionReport(systemName, COST_FRAMEWORK.P2P);
	}

	public HashMap<String, Object> getSysInfo()
	{
		return getQueryDataWithHeaders(TAP_Core_Data, sysInfoQuery);
	}

	public HashMap<String, Object> getHWSWCostInfo() 
	{
		hwSWBudgetQuery = hwSWBudgetQuery.replace("@SYSTEM@",systemURI);
		return getQueryDataWithHeaders(TAP_Cost_Data, hwSWBudgetQuery);
	}

	public void enableMessages(boolean showMessages)
	{
		this.showMessages = showMessages;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> createInterfaceBarChart(Map<String,Object> sysLPIInterfaceHash) throws IOException
	{
		// run interface report if object passed in is empty
		if(sysLPIInterfaceHash == null || sysLPIInterfaceHash.isEmpty()) {
			LPInterfaceCostProcessor sysInterfaceData;
			sysInterfaceData = new LPInterfaceCostProcessor();
			sysLPIInterfaceHash = sysInterfaceData.generateSystemTransitionReport(systemName, COST_FRAMEWORK.P2P);
		}

		HashMap<String, Object> barHash = new HashMap<String, Object>();
		String[] headers = new String[]{"Required Direct Interfaces with MHS GENESIS - Legacy Provider","Required Direct Interfaces with MHS GENESIS - Legacy Consumer","Existing Legacy Interfaces Required to Endure","Existing Legacy Interfaces Recommended for Removal","Proposed Future Interfaces with MHS GENESIS - Legacy Provider","Proposed Temporary Interfaces with MHS GENESIS - Legacy Consumer"};
		int[] barChartVals = new int[]{0,0,0,0,0,0};
		ArrayList<Object[]> interfaceRowList = (ArrayList<Object[]>) sysLPIInterfaceHash.get(DHMSMTransitionUtility.DATA_KEY);
		HashSet<String> dataObjectListForConsumers = new HashSet<String>();
		HashSet<String> dataObjectListForProviders = new HashSet<String>();
		
		for(int i=0;i<interfaceRowList.size();i++)
		{
			Object[] interfaceRow = interfaceRowList.get(i);
			String comment = ((String)interfaceRow[interfaceRow.length - 3]).toLowerCase();
			String otherSystem = ((String)interfaceRow[1]).toLowerCase();
			boolean addedInterface = false;		
			boolean removedInterface = false;
			//if it says need to add interface from dhmsm to our system, then legacy - consumer
			//if it says need to add interface from our system to dhmsm, then legacy - provider
			if(comment.contains("need to add interface "+systemName.toLowerCase()+"->MHS GENESIS"))
			{
				if(!dataObjectListForProviders.contains(interfaceRow[4].toString())) {
					dataObjectListForProviders.add(interfaceRow[4].toString());
					addedInterface = true;
					barChartVals[0] = barChartVals[0]+1;
				}
			}
			else if(comment.contains("need to add interface MHS GENESIS->"+systemName.toLowerCase()))
			{
				if(!dataObjectListForConsumers.contains(interfaceRow[4].toString())) {
					dataObjectListForConsumers.add(interfaceRow[4].toString());
					addedInterface = true;
					barChartVals[1] = barChartVals[1]+1;
				}
			}
			if(comment.contains("provide temporary integration between MHS GENESIS->"+systemName.toLowerCase()))
			{
				//addedInterface = true;
				barChartVals[5] = barChartVals[5]+1;
			}

			if(comment.contains("recommend review of removing interface "+systemName.toLowerCase()+"->"+otherSystem)||comment.contains("recommend review of removing interface "+otherSystem+"->"+systemName.toLowerCase()))
			{
				removedInterface = true;
				barChartVals[3] = barChartVals[3]+1;
			}

			if(comment.contains("developing"))
			{
				if(comment.contains(systemName.toLowerCase()))
					barChartVals[4] = barChartVals[4]+1;
			}

			//stayAsIs if neither need to add interface or remove interface involve the system we're looking at
			if((!addedInterface && !removedInterface) || comment.contains("stay"))
				barChartVals[2] = barChartVals[2]+1;

		}
		// interfaces staying are total list being returned minus those being removed
		barChartVals[2] = interfaceRowList.size() - barChartVals[3];
		
		barHash.put(DHMSMTransitionUtility.HEADER_KEY, headers);
		barHash.put(DHMSMTransitionUtility.DATA_KEY, barChartVals);
		return barHash;
	}


	@SuppressWarnings("unchecked")
	private Map<String, Object> createHWSWBarHash(Map<String, Object> storeData)
	{
		Map<String, Object> barHash = new HashMap<String, Object>();
		String[] headers = new String[]{"Retired (Not Supported)","Supported","GA (Generally Available)","TBD"};
		int[] barChartVals = new int[]{0,0,0,0};

		ArrayList<Object[]> data = (ArrayList<Object[]>) storeData.get(DHMSMTransitionUtility.DATA_KEY);
		for(int i=0;i<data.size();i++)
		{
			Object[] row = data.get(i);
			String lifecycle = ((String)row[2]).toLowerCase();
			if(lifecycle.contains("retired"))
				barChartVals[0] = barChartVals[0]+1;
			else if(lifecycle.contains("supported"))
				barChartVals[1] = barChartVals[1]+1;
			else if(lifecycle.contains("ga"))
				barChartVals[2] = barChartVals[2]+1;
			else if(lifecycle.contains("tbd"))
				barChartVals[3] = barChartVals[3]+1;
		}
		barHash.put(DHMSMTransitionUtility.HEADER_KEY, headers);
		barHash.put(DHMSMTransitionUtility.DATA_KEY, barChartVals);
		return barHash;		
	}
	
	private HashMap<String, Object> getSysSORTableWithHeaders(IDatabaseEngine engine,String sysSORDataQuery,String otherSysSORDataQuery)
	{
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		ArrayList<String> dataObjectsProvided = new ArrayList<String>();
		ArrayList<String> dataObjectsConsumed = new ArrayList<String>();
		ArrayList<String> systemsToAdd = new ArrayList<String>();
		HashMap<String,ArrayList<String>> dataConsumedFromHash = new HashMap<String,ArrayList<String>>();

		//making list of SOR systems
		ISelectWrapper sjsw = Utility.processQuery(engine,sysSORDataQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = (String)sjss.getVar(names[0]);
			String provideConsume = (String)sjss.getVar(names[1]);
			String service = (String)sjss.getVar(names[2]);
			String otherSystem = (String)sjss.getVar(names[3]);
			if(provideConsume.startsWith("\"") && provideConsume.endsWith("\""))
				provideConsume = provideConsume.substring(1, provideConsume.toString().length()-1); // remove annoying quotes
			Object[] dataRow;

			//check if it is a consume,
			if(provideConsume.toLowerCase().contains("consume")){
				//if we haven't already made a row for consuming this data object, then make a row.
				int dataIndex = dataObjectsConsumed.indexOf(data);
				if(dataIndex<0){
					dataRow = new Object[]{data,provideConsume,service,0};
					dataObjectsConsumed.add(data);
					dataToAddArr.add(dataRow);
				}
				//add the other system to the table because our system is consuming from it
				if(!systemsToAdd.contains(otherSystem))
					systemsToAdd.add(otherSystem);
				//add the other system to the data hash since we are consuming that data from the other system.
				if(!dataConsumedFromHash.containsKey(data)) {
					ArrayList<String> systemsConsumedFrom = new ArrayList<String>();
					systemsConsumedFrom.add(otherSystem);
					dataConsumedFromHash.put(data,systemsConsumedFrom);
				} else {
					ArrayList<String> systemsConsumedFrom = dataConsumedFromHash.get(data);
					if(!systemsConsumedFrom.contains(otherSystem))
						systemsConsumedFrom.add(otherSystem);
					dataConsumedFromHash.put(data,systemsConsumedFrom);
				}
				
			}
			//if it is a provide
			else{
				int dataIndex = dataObjectsProvided.indexOf(data);
				//check if there has been a row made for providing this data object, if not make a row with val 1
				if(dataIndex<0){
					dataRow = new Object[]{data,provideConsume,service,1};
					dataObjectsProvided.add(data);
					dataToAddArr.add(dataRow);
				}
				//if a row has been made, then add 1 because there is an additional downstream system
				else{
					dataRow = dataToAddArr.get(dataIndex);
					dataRow[3]= (Integer)dataRow[3]+1;
					dataToAddArr.set(dataIndex, dataRow);
				}
			}
		}

		if(!systemsToAdd.isEmpty()) {
		// sort selected systems
		Vector <String> systemsVector = new Vector<String>(systemsToAdd);
		Collections.sort(systemsVector);
		systemsToAdd = new ArrayList<String>(systemsVector);

		//update list to include spots for all systems
		for(int i=0;i<dataToAddArr.size();i++)
		{
			Object[] oldRow = dataToAddArr.get(i);
			Object[] newRow = new Object[oldRow.length + systemsToAdd.size()];
			for(int j=0;j<oldRow.length;j++)
				newRow[j] = oldRow[j];
			for(int j=oldRow.length;j<oldRow.length+systemsToAdd.size();j++)
				newRow[j] = 0;
			dataToAddArr.set(i,newRow);
		}
		
		//add a bindings list to only pull the systems we care about
		String systemsBindings = "";
		for(int i=0;i<systemsToAdd.size();i++) {
			systemsBindings+= "(<" + SYS_URI_PREFIX+systemsToAdd.get(i)+">)";
		}
		otherSysSORDataQuery += " BINDINGS ?System {"+systemsBindings+"}";

		ArrayList<Object[]> otherSysList = new ArrayList<Object[]>();
		ISelectWrapper sjsw3 = Utility.processQuery(engine,otherSysSORDataQuery);
		String[] names3 = sjsw3.getVariables();
		while(sjsw3.hasNext())
		{
			ISelectStatement sjss = sjsw3.next();
			Object[] dataRow = new Object[names3.length];
			for(int i=0;i<names3.length;i++)
			{
				Object dataElem = sjss.getVar(names3[i]);
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				dataRow[i] = dataElem;
			}
			otherSysList.add(dataRow);
		}

		for(Object[] row : otherSysList)
		{
			String sys = (String) row[0];
			String data = (String) row[1];
			if(dataConsumedFromHash.containsKey(data)&&dataConsumedFromHash.get(data).contains(sys)) {
				int sysInd = systemsToAdd.indexOf(sys);
				int dataInd = dataObjectsConsumed.indexOf(row[1]);
				Object[] rowToUpdate = dataToAddArr.get(dataObjectsProvided.size()+dataInd);
				rowToUpdate[sysInd+4] = row[2];
				dataToAddArr.set(dataObjectsProvided.size()+dataInd, rowToUpdate);
			}
		}
		}
		String[] headers = new String[4+systemsToAdd.size()];
		headers[0] = "Data Object";
		headers[1] = "Provides or Consumes";
		headers[2] = "Services";
		headers[3] = systemName;
		for(int i=0;i<systemsToAdd.size();i++)
			headers[i+4] = systemsToAdd.get(i);

		dataHash.put(DHMSMTransitionUtility.HEADER_KEY,headers);
		dataHash.put(DHMSMTransitionUtility.DATA_KEY, dataToAddArr);
		return dataHash;
	}


	private HashMap<String, Object> getQueryDataWithHeaders(IDatabaseEngine engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(DHMSMTransitionUtility.HEADER_KEY, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++)
			{
				Object dataElem = sjss.getVar(names[i]);
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
				{
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				}
				dataRow[i] = dataElem;
			}
			dataToAddArr.add(dataRow);
		}

		dataHash.put(DHMSMTransitionUtility.DATA_KEY, dataToAddArr);

		return dataHash;
	}
	
	private void printout( Map<String, Object> sysProposedFutureICD )
	{
		String[] headers = (String[]) sysProposedFutureICD.get(DHMSMTransitionUtility.HEADER_KEY);
		
		for( int i = 0 ; i < headers.length-1; i++)
		{
			System.out.print(headers[i] +"\t");
			
		}
		System.out.println();
		ArrayList<Object[]> data = (ArrayList<Object[]>) sysProposedFutureICD.get(DHMSMTransitionUtility.DATA_KEY);

		for( int i = 0 ; i < data.size(); i++)
		{
			Object[] row = data.get(i);

			for( int j = 0 ; j < row.length-1; j++)
			{
				System.out.print(row[j] +"\t");
			}
			System.out.println();

		}
	}
	
	private HashMap<String, Object> removeColumn( Map<String, Object> input, int column )
	{
		
		String[] headers = (String[]) input.get(DHMSMTransitionUtility.HEADER_KEY);
		String[] newHeaders = new String[headers.length-1];
		for( int i = 0 ; i < headers.length-1; i++)
		{
			if( i < column )
				newHeaders[i] = headers[i];
			else
				newHeaders[i] = headers[i+1];
		}
		
		ArrayList<Object[]> data = (ArrayList<Object[]>) input.get(DHMSMTransitionUtility.DATA_KEY);
		ArrayList<Object[]> newData = new ArrayList<Object[]>();
		for( int i = 0 ; i < data.size(); i++)
		{
			Object[] row = data.get(i);
			Object[] newRow = new Object[row.length - 1];
			for( int j = 0 ; j < row.length-1; j++)
			{
				if( j < column )
					newRow[j] = row[j];
				else
					newRow[j] = row[j+1];
			}
			newData.add(newRow);
		}
		
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put(DHMSMTransitionUtility.HEADER_KEY, newHeaders);
		output.put(DHMSMTransitionUtility.DATA_KEY, newData);
		
		return output;
	}
	
	private ArrayList<Object[]> removeSystemFromArrayList(ArrayList<Object[]> dataRow)
	{
		ArrayList<Object[]> retList = new ArrayList<Object[]>();
		for(int i=0;i<dataRow.size();i++)
		{
			Object[] row = new Object [dataRow.get(i).length-1];
			for(int j=0;j<row.length;j++)
				row[j] = dataRow.get(i)[j+1];
			retList.add(row);
		}
		return retList;
	}

	private String[] removeSystemFromStringArray(String[] names)
	{
		String[] retArray = new String[names.length-1];
		for(int j=0;j<retArray.length;j++)
			retArray[j] = names[j+1];
		return retArray;
	}

	
	
}
