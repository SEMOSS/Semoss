package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.poi.specific.IndividualSystemTransitionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IndividualSystemTransitionReport extends AbstractRDFPlaySheet{

	// list of queries to run
	private String sysInfoQuery = "SELECT DISTINCT ?Description (COALESCE(?GT, 'Garrison') AS ?GarrisonTheater) (IF(BOUND(?MU),'Yes','No') AS ?MHS_Specific) ?Transaction_Count (COALESCE(SUBSTR(STR(?ATO),0,10),'NA') AS ?ATO_Date) (COALESCE(SUBSTR(STR(?ES),0,10),'NA') AS ?End_Of_Support) ?Num_Users WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> ?MU}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}}OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?Transaction_Count}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?ES}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?Num_Users}} }";
	private String sysSORDataWithDHMSMQuery = "SELECT DISTINCT ?Data ?DHMSM_SOR WHERE { {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}  {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } } } UNION {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE {BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } }} } ORDER BY ?System";
	private String sysSORDataWithDHMSMCapQuery = "SELECT DISTINCT ?Capability ?Data (GROUP_CONCAT(DISTINCT ?Comment ; Separator = ' and ') AS ?Comments) WHERE { { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> }{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?SystemProvideData_CRM} FILTER( !regex(str(?SystemProvideData_CRM),'R')) {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} {?Task ?Needs ?Data} BIND('ICD validated through downstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } UNION { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?Task ?Needs ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } {?System <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data } FILTER(!BOUND(?icd2)) BIND('ICD implied through downstream and no upstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } } GROUP BY ?Capability ?Data ?System ORDER BY ?Capability ?Data ?System";
	private String softwareLifeCycleQuery = "SELECT DISTINCT ?System ?SoftwareVersion (COALESCE(xsd:dateTime(?SoftMEOL), COALESCE(xsd:dateTime(?SoftVEOL),'TBD')) AS ?SupportDate) (COALESCE(xsd:dateTime(?SoftMEOL), COALESCE(xsd:dateTime(?SoftVEOL),'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemSWBudget) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?System ?Has ?SoftwareModule} {?SoftwareModule ?TypeOf ?SoftwareVersion} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVEOL} OPTIONAL {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMEOL} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?SoftwareVersion";
	private String hardwareLifeCycleQuery = "SELECT DISTINCT ?System ?HardwareVersion (COALESCE(xsd:dateTime(?HardMEOL), COALESCE(xsd:dateTime(?HardVEOL),'TBD')) AS ?SupportDate) (COALESCE(xsd:dateTime(?HardMEOL), COALESCE(xsd:dateTime(?HardVEOL),'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemHWBudget) WHERE { BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion } OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVEOL} OPTIONAL {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMEOL} OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?HardwareVersion";
	private String hwSWBudgetQuery = "SELECT DISTINCT ?System ?GLTag (max(coalesce(?FY15,0)) as ?fy15) WHERE {BIND(@SYSTEM@ AS ?System) { {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem}{?SystemBudgetGLItem ?TaggedBy ?GLTag}{?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15)} UNION {{?SystemServiceBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem> ;} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService> ;}{?ConsistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>;} {?System ?ConsistsOf ?SystemService}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?SystemService ?Has ?SystemServiceBudgetGLItem}{?SystemServiceBudgetGLItem ?TaggedBy ?GLTag}{?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemServiceBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) } }GROUP BY ?System ?GLTag BINDINGS ?GLTag {(<http://health.mil/ontologies/Concept/GLTag/SW_Licen>) (<http://health.mil/ontologies/Concept/GLTag/HW>)}";

	// making the SOR sheet
	private String sysSORDataQuery = "SELECT DISTINCT ?Data ?ProvideOrConsume (GROUP_CONCAT(?Ser; SEPARATOR = ', ') AS ?Services) ?otherSystem WHERE {{SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} OPTIONAL {{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data}   FILTER(!BOUND(?icd2)) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) ?otherSystem WHERE {BIND(@SYSTEM@ AS ?System){?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data}{?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} BIND('Consume' AS ?ProvideOrConsume)}} } GROUP BY ?Data ?ProvideOrConsume ?otherSystem ORDER BY DESC(?ProvideOrConsume) ?Data";
	private String otherSysSORDataQuery = "SELECT DISTINCT ?System ?Data (COUNT(?icd) AS ?DownstreamInterfaces) WHERE { {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?downstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?System <http://semoss.org/ontologies/Relation/Provide> ?icd}{?icd <http://semoss.org/ontologies/Relation/Consume> ?downstreamSystem} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} }UNION {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?downstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> }{?System <http://semoss.org/ontologies/Relation/Provide> ?icd }{?icd <http://semoss.org/ontologies/Relation/Consume> ?downstreamSystem}{?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) }FILTER (?System != @SYSTEM@)} GROUP BY ?System ?Data ORDER BY ?Data ?System";

	// direct cost and indirect costs reqiures 
	private String loeForSysGlItemQuery = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?sys ?data ?ser ?gltag1";
	private HashMap<String, HashMap<String, Double>> loeForSysGlItemHash = new HashMap<String, HashMap<String, Double>>();

	private String loeForGenericGlItemQuery = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?Loe) WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } GROUP BY ?data ?ser";
	private HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash = new HashMap<String, HashMap<String, Double>>();
	private String avgLoeForSysGLItemQuery = "SELECT DISTINCT ?data ?ser (ROUND(SUM(?loe)/COUNT(DISTINCT ?sys)) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1";
	private HashMap<String, HashMap<String, Double>> avgLoeForSysGlItemHash = new HashMap<String, HashMap<String, Double>>();
	private String serviceToDataQuery = "SELECT DISTINCT ?data (GROUP_CONCAT(?Ser; SEPARATOR = '; ') AS ?service) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data } BIND(SUBSTR(STR(?ser),46) AS ?Ser) } GROUP BY ?data";
	private HashMap<String, String> serviceToDataHash = new HashMap<String, String>();


	// lpni indirect cost also requires
	private String dhmsmSORQuery = "SELECT DISTINCT ?Data WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} }";
	private HashSet<String> dhmsmSORList = new HashSet<String>();
	private String lpiSystemQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private HashSet<String> lpiSystemList = new HashSet<String>();


	private int costPerHr = 150;

	private String headerKey = "headers";
	private String dataKey = "data";
	private String totalDirectCostKey = "directCost";
	private String totalIndirectCostKey = "indirectCost";

	private IEngine hr_Core;
	private IEngine TAP_Cost_Data;

	private String[] dates = new String[]{"2016&June","2017&April","2022&July"};

	private String systemURI = "";
	private String systemName = "";
	private String reportType = "";

	private boolean showMessages = true;

	// list of services built for the systems
	private HashSet<String> servicesProvideList = new HashSet<String>();
	private HashSet<String> servicesConsumeList = new HashSet<String>();

	Logger logger = Logger.getLogger(getClass());

	@Override
	public void createData() {

		try{
			hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			TAP_Cost_Data = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		} catch(Exception e) {
			e.printStackTrace();
			Utility.showError("Could not find necessary databases:\nHR_Core, TAP_Cost_Data");
		}

		// get data for all systems
		if(loeForGenericGlItemHash.isEmpty()) {
			loeForGenericGlItemHash = getGenericGLItem(TAP_Cost_Data, loeForGenericGlItemQuery);
		}
		if(avgLoeForSysGlItemHash.isEmpty()) {
			avgLoeForSysGlItemHash = getAvgSysGLItem(TAP_Cost_Data, avgLoeForSysGLItemQuery);
		}
		if(serviceToDataHash.isEmpty()) {
			serviceToDataHash = getServiceToData(TAP_Cost_Data, serviceToDataQuery);
		}
		if(loeForSysGlItemHash.isEmpty()) {
			loeForSysGlItemHash = getSysGLItem(TAP_Cost_Data, loeForSysGlItemQuery);
		}

		String[] systemAndType = this.query.split("\\$");
		systemURI = systemAndType[0];
		reportType = systemAndType[1];

		if(reportType.equals("LPNI")){
			if(dhmsmSORList.isEmpty()) {
				dhmsmSORList = runListQuery(hr_Core, dhmsmSORQuery);
			}
			if(lpiSystemList.isEmpty()) {
				lpiSystemList = runListQuery(hr_Core, lpiSystemQuery);
			}
		}

		systemName = systemURI.substring(systemURI.lastIndexOf("/")+1,systemURI.lastIndexOf(">"));

		sysInfoQuery = sysInfoQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMQuery = sysSORDataWithDHMSMQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMCapQuery = sysSORDataWithDHMSMCapQuery.replace("@SYSTEM@", systemURI);
		softwareLifeCycleQuery = softwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		hardwareLifeCycleQuery = hardwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		hwSWBudgetQuery = hwSWBudgetQuery.replace("@SYSTEM@",systemURI);

		sysSORDataQuery = sysSORDataQuery.replace("@SYSTEM@", systemURI);
		otherSysSORDataQuery = otherSysSORDataQuery.replace("@SYSTEM@", systemURI);
		HashMap<String, Object> sysInfoHash = getQueryDataWithHeaders(hr_Core, sysInfoQuery);
		HashMap<String, Object> sysSORDataWithDHMSMHash = getQueryDataWithHeaders(hr_Core, sysSORDataWithDHMSMQuery);
		HashMap<String, Object> sysSORDataWithDHMSMCapHash = getQueryDataWithHeaders(hr_Core, sysSORDataWithDHMSMCapQuery);
		HashMap<String, Object> hwSWBudgetHash = getQueryDataWithHeaders(TAP_Cost_Data, hwSWBudgetQuery);
		HashMap<String, Object> sysSORTableHash = getSysSORTableWithHeaders(hr_Core,sysSORDataQuery,otherSysSORDataQuery);

		HashMap<Integer, HashMap<String, Object>> storeSoftwareData = new HashMap<Integer, HashMap<String, Object>>();
		HashMap<Integer, HashMap<String, Object>> storeHardwareData = new HashMap<Integer, HashMap<String, Object>>();

		LifeCycleGridPlaySheet getSoftwareHardwareData = new LifeCycleGridPlaySheet();
		for(int i = 0; i < dates.length; i++)
		{
			getSoftwareHardwareData.engine = hr_Core;
			getSoftwareHardwareData.setQuery(dates[i] + "&" + softwareLifeCycleQuery);
			ArrayList<Object[]> dataRow = getSoftwareHardwareData.processQuery(getSoftwareHardwareData.getQuery());
			String[] names = getSoftwareHardwareData.getNames();
			dataRow  = removeSystemFromArrayList(dataRow);
			names = removeSystemFromStringArray(names);
			HashMap<String, Object> innerMap = new HashMap<String, Object>();
			innerMap.put(headerKey, names);
			innerMap.put(dataKey, dataRow);
			storeSoftwareData.put(i, innerMap);
		}
		for(int i = 0; i < dates.length; i++)
		{
			getSoftwareHardwareData.setQuery(dates[i] + "&" + hardwareLifeCycleQuery);
			String[] names = getSoftwareHardwareData.getNames();
			ArrayList<Object[]> dataRow = getSoftwareHardwareData.processQuery(getSoftwareHardwareData.getQuery());
			dataRow  = removeSystemFromArrayList(dataRow);
			names = removeSystemFromStringArray(names);
			HashMap<String, Object> innerMap = new HashMap<String, Object>();
			innerMap.put(headerKey, names);
			innerMap.put(dataKey, dataRow);
			storeHardwareData.put(i, innerMap);
		}

		LPInterfaceReportGenerator sysLPInterfaceData = new LPInterfaceReportGenerator();
		HashMap<String, Object> sysLPIInterfaceHash = sysLPInterfaceData.getSysLPIInterfaceData(systemName);

 		HashMap<String, Object> sysLPInterfaceWithCostHash = new HashMap<String, Object>();
		if(reportType.equals("LPI")) {
			sysLPInterfaceWithCostHash = createLPIInterfaceWithCostHash(sysLPIInterfaceHash, loeForSysGlItemHash, loeForGenericGlItemHash);
		} else {
			sysLPInterfaceWithCostHash = createLPNIInterfaceWithCostHash(sysLPIInterfaceHash, loeForSysGlItemHash, loeForGenericGlItemHash, dhmsmSORList, lpiSystemList);
		}
		
		HashMap<String, Object> interfaceBarHash = createInterfaceBarChart(sysLPIInterfaceHash);
		HashMap<String, Object> softwareBarHash = createHWSWBarHash(storeSoftwareData.get(0));
		HashMap<String, Object> hardwareBarHash = createHWSWBarHash(storeHardwareData.get(0));

		IndividualSystemTransitionReportWriter writer = new IndividualSystemTransitionReportWriter();
		String templateFileName = "Individual_System_LPI_Transition_Report_Template.xlsx";
		if(!reportType.equals("LPI"))
			templateFileName = "Individual_System_LPNI_Transition_Report_Template.xlsx";
		writer.makeWorkbook(Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")),templateFileName);
		writer.writeSystemInfoSheet("System Overview",sysInfoHash);
		writer.writeHWSWSheet("Software Lifecycles", storeSoftwareData.get(0), storeSoftwareData.get(1), storeSoftwareData.get(2));
		writer.writeHWSWSheet("Hardware Lifecycles", storeHardwareData.get(0), storeHardwareData.get(1), storeHardwareData.get(2));
		writer.writeModernizationTimelineSheet("Modernization Timeline", storeSoftwareData.get(0), storeHardwareData.get(0), hwSWBudgetHash);
		writer.writeListSheet("SOR Overlap With DHMSM", sysSORDataWithDHMSMHash);
		writer.writeListSheet("DHMSM Data Requirements", sysSORDataWithDHMSMCapHash);
		writer.writeListSheet("System Interfaces", sysLPInterfaceWithCostHash);
		writer.writeSORSheet("System Data",sysSORTableHash);
		writer.writeBarChartData("Summary Charts",3,softwareBarHash);
		writer.writeBarChartData("Summary Charts",11,hardwareBarHash);
		writer.writeBarChartData("Summary Charts",19,interfaceBarHash);
		boolean success = writer.writeWorkbook();
		if(showMessages)
		{
			if(success){
				Utility.showMessage("System Export Finished! File located in:\n" + IndividualSystemTransitionReportWriter.getFileLoc() );
			} else {
				Utility.showError("Error Creating Report!");
			}
		}
	}

	public void enableMessages(boolean showMessages)
	{
		this.showMessages = showMessages;
	}
	
	private HashMap<String, Object> createInterfaceBarChart(HashMap<String,Object> sysLPIInterfaceHash)
	{
		HashMap<String, Object> barHash = new HashMap<String, Object>();
		String[] headers = new String[]{"Required Direct Interfaces with DHMSM - Legacy Provider","Required Direct Interfaces with DHMSM - Legacy Consumer","Existing Legacy Interfaces Required to Endure","Existing Legacy Interfaces Recommended for Removal","Proposed Future Interfaces with DHMSM - Legacy Provider"};
		int[] barChartVals = new int[]{0,0,0,0,0};
		ArrayList<Object[]> interfaceRowList = (ArrayList<Object[]>) sysLPIInterfaceHash.get(dataKey);
		
		for(int i=0;i<interfaceRowList.size();i++)
		{
			Object[] interfaceRow = interfaceRowList.get(i);
			String comment = ((String)interfaceRow[interfaceRow.length -1]).toLowerCase();
			boolean addedInterface = false;		
			boolean removedInterface = false;
			//if it says need to add interface from dhmsm to our system, then legacy - consumer
			//if it says need to add interface from our system to dhmsm, then legacy - provider
			if(comment.contains("need to add interface "+systemName.toLowerCase()+"->dhmsm"))
			{
				addedInterface = true;
				barChartVals[0] = barChartVals[0]+1;
			}
			else if(comment.contains("need to add interface dhmsm->"+systemName.toLowerCase()))
			{
				addedInterface = true;
				barChartVals[1] = barChartVals[1]+1;
			}
			
			if(comment.contains("recommend review of removing interface "+systemName.toLowerCase()))
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
			if((!addedInterface && !removedInterface)|| comment.contains("stay"))
				barChartVals[2] = barChartVals[2]+1;

		}
		barHash.put("headers", headers);
		barHash.put("data", barChartVals);
		return barHash;
	}
	private HashMap<String, Object> createHWSWBarHash(HashMap<String, Object> storeData)
	{
		HashMap<String, Object> barHash = new HashMap<String, Object>();
		String[] headers = new String[]{"Retired (Not Supported)","Supported","GA (Generally Available)","TBD"};
		int[] barChartVals = new int[]{0,0,0,0};

		ArrayList<Object[]> data = (ArrayList<Object[]>) storeData.get(dataKey);
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
		barHash.put("headers", headers);
		barHash.put("data", barChartVals);
		return barHash;		
	}

	private HashMap<String, Object> createLPIInterfaceWithCostHash(HashMap<String, Object> sysLPIInterfaceHash, HashMap<String, HashMap<String, Double>> loeForSysGlItemHash, HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash) 
	{
		HashMap<String, Object> dataHash = new HashMap<String, Object>();
		String[] oldHeaders = (String[]) sysLPIInterfaceHash.get(headerKey);
		ArrayList<Object[]> oldData = (ArrayList<Object[]>) sysLPIInterfaceHash.get(dataKey);

		String[] newHeaders = new String[oldHeaders.length + 3];
		for(int i = 0; i < oldHeaders.length; i++)
		{
			if(i < oldHeaders.length - 1) {
				newHeaders[i] = oldHeaders[i];
			} else {
				newHeaders[i] = "Services";
				newHeaders[i+1] = "Recommendation";
				newHeaders[i+2] = "Direct Cost";
				newHeaders[i+3] = "Indirect Cost";
			}
		}

		ArrayList<Object[]> newData = new ArrayList<Object[]>();
		// clear the list of services already built for each system report
		servicesProvideList.clear();
		servicesConsumeList.clear();
		double totalDirectCost = 0;
		double totalIndirectCost = 0;
		String dataObject = "";
		String interfacingSystem = "";
		
		// used to keep track of rows that have the same data object
		ArrayList<Integer> indexArr = new ArrayList<Integer>();
		int rowNum = -1;
		boolean deleteOtherInterfaces = false;

		for(Object[] row : oldData)
		{
			rowNum++;
			Object[] newRow = new Object[oldHeaders.length + 3];
			for(int i = 0; i < row.length; i++)
			{
				if(i == 1) 
				{
					interfacingSystem= row[i].toString(); 
					newRow[i] = interfacingSystem;
				}
				if(i == 4) 
				{
					// test if data object of last row is the same of this row
					if(!dataObject.equals(row[i].toString()))
					{
						dataObject = row[i].toString();
						indexArr = new ArrayList<Integer>();
						deleteOtherInterfaces = false;
					}
					indexArr.add(rowNum);
					newRow[i] = dataObject;
				} 
				else if(i == row.length - 1) 
				{
					String servicesList = serviceToDataHash.get(dataObject);
					if(servicesList != null) {
						newRow[i] = servicesList;
					} else {
						newRow[i] = "No Services.";
					}

					String comment = row[i].toString().replaceAll("\"", "");
					newRow[i+1] = comment;

					String[] commentSplit = comment.split("\\."); 
					String sysSpecificComment = commentSplit[0];
					String sysRecommendationComment = "";
					if(commentSplit.length > 1) {
						sysRecommendationComment = commentSplit[1];
						if(sysRecommendationComment.startsWith(" ")){
							sysRecommendationComment = sysRecommendationComment.substring(1);
						}
					}
					if(!sysSpecificComment.contains("->"))
					{
						newRow[i+2] = "";
						newRow[i+3] = "";
					}
					else
					{
						String[] sysSpecificCommentSplit = sysSpecificComment.split("->");
						//////////////////////////////////////////////////////////////////////////////////////////////
						if(sysSpecificCommentSplit[1].contains("DHMSM")) // this means DHMSM consumes data, direct provider cost for system
						{
							Double finalCost = null;
							boolean interfacingSysProvider = false;
							if(sysSpecificCommentSplit[0].contains(systemName)) { // if system providing is our system
								finalCost = calculateCost(dataObject, systemName, "Provider", true);
							} else { // if system providing is the interfacing system, no cost
								interfacingSysProvider = true;
								newRow[i+2] = "";
								newRow[i+3] = "";
							}
							if(!interfacingSysProvider)
							{
								if(finalCost == null) {
									newRow[i+2] = "Cost already taken into consideration.";
									newRow[i+3] = "";
								} else if(finalCost != (double) 0){
									newRow[i+2] = finalCost;
									totalDirectCost += finalCost;
									newRow[i+3] = "";
								} else {
									newRow[i+2] = "No data present to calculate loe.";
									newRow[i+3] = "";
								}
							}
						}
						//////////////////////////////////////////////////////////////////////////////////////////////
						else // this means DHMSM provides data
						{
							if(sysSpecificCommentSplit[1].contains(systemName) && !deleteOtherInterfaces) // direct consumer cost if our system is consuming
							{
								Double finalCost = calculateCost(dataObject, systemName, "Consume", false);
								if(finalCost == null) {
									newRow[i+2] = "Cost already taken into consideration.";
									newRow[i+3] = "";
								} else if(finalCost != (double) 0){
									newRow[i+2] = finalCost;
									totalDirectCost += finalCost;
									newRow[i+3] = "";
								} else {
									newRow[i+2] = "No data present to calculate loe.";
									newRow[i+3] = "";
								}

								deleteOtherInterfaces = true; 
								for(Integer index : indexArr)
								{
									if(index < newData.size() && newData.get(index) != null) // case when first row is the LPI system and hasn't been added to newData yet
									{
										Object[] modifyCost = newData.get(index);
										if(!sysRecommendationComment.equals("")) {
											newRow[i+1] = sysRecommendationComment + ".";
										} else if(sysRecommendationComment.contains("Stay as-is")) {
											newRow[i+1] = sysSpecificComment + ".";
										} else {
											newRow[i+1] = "Interface already taken into consideration.";
										}
										modifyCost[i+2] = "";
										modifyCost[i+3] = "";
									}
								}
							} else if(deleteOtherInterfaces) {
								if(!sysRecommendationComment.equals("")) {
									newRow[i+1] = sysRecommendationComment + ".";
								} else if(sysRecommendationComment.contains("Stay as-is")) {
									newRow[i+1] = sysSpecificComment + ".";
								} else {
									newRow[i+1] = "Interface already taken into consideration.";
								}
								newRow[i+2] = "";
								newRow[i+3] = "";
							} else { // indirect consumer cost if not our system
								Double finalCost = calculateCost(dataObject, interfacingSystem, "Consume", false);
								if(finalCost == null) {
									newRow[i+3] = "Cost already taken into consideration.";
									newRow[i+2] = "";
								} else if(finalCost != (double) 0){
									newRow[i+3] = finalCost;
									totalIndirectCost += finalCost;
									newRow[i+2] = "";
								} else {
									newRow[i+3] = "No data present to calculate loe.";
									newRow[i+2] = "";
								}
							}
						}
					}
				} else {
					newRow[i] = row[i];
				}
			}
			newData.add(newRow);
		}
		dataHash.put(dataKey, newData);
		dataHash.put(headerKey, newHeaders);
		dataHash.put(totalDirectCostKey, totalDirectCost);
		dataHash.put(totalIndirectCostKey, totalIndirectCost);

		return dataHash;
	}

	private HashMap<String, Object> createLPNIInterfaceWithCostHash(HashMap<String, Object> sysLPNIInterfaceHash, HashMap<String, HashMap<String, Double>> loeForSysGlItemHash, HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash, HashSet<String> dhmsmSORList, HashSet<String> lpiSystemList) 
	{
		HashMap<String, Object> dataHash = new HashMap<String, Object>();
		String[] oldHeaders = (String[]) sysLPNIInterfaceHash.get(headerKey);
		ArrayList<Object[]> oldData = (ArrayList<Object[]>) sysLPNIInterfaceHash.get(dataKey);

		String[] newHeaders = new String[oldHeaders.length + 3];
		for(int i = 0; i < oldHeaders.length; i++)
		{
			if(i < oldHeaders.length - 1) {
				newHeaders[i] = oldHeaders[i];
			} else {
				newHeaders[i] = "Services";
				newHeaders[i+1] = "Recommendation";
				newHeaders[i+2] = "Direct Cost";
				newHeaders[i+3] = "Indirect Cost";
			}
		}

		ArrayList<Object[]> newData = new ArrayList<Object[]>();
		double totalDirectCost = 0;
		double totalIndirectCost = 0;
		String dataObject = "";
		String interfacingSystem = "";

		for(Object[] row : oldData)
		{
			Object[] newRow = new Object[oldHeaders.length + 3];
			for(int i = 0; i < row.length; i++)
			{
				if(i == 1) 
				{
					interfacingSystem= row[i].toString(); 
					newRow[i] = interfacingSystem;
				}
				else if(i == 4) 
				{
					dataObject = row[i].toString(); 
					newRow[i] = dataObject;
				} 
				else if(i == row.length - 1) 
				{
					String servicesList = serviceToDataHash.get(dataObject);
					if(servicesList != null) {
						newRow[i] = servicesList;
					} else {
						newRow[i] = "No Services";
					}

					String comment = row[i].toString().replaceAll("\"", "");
					newRow[i+1] = comment;

					String[] commentSplit = comment.split("\\."); 
					String sysSpecificComment = commentSplit[0];
					if(!sysSpecificComment.contains("->"))
					{
						newRow[i+2] = "";
						newRow[i+3] = "";
					}
					else
					{
						String[] sysSpecificCommentSplit = sysSpecificComment.split("->");

						// DHMSM is providing information to an LPI system  
						// DHMSM is receiving information from LPNI which is a SOR of the data object
						if( (sysSpecificCommentSplit[0].contains("DHMSM") && lpiSystemList.contains(systemName)) || (sysSpecificCommentSplit[0].contains("review of developing interface between") && sysSpecificCommentSplit[1].contains("DHMSM")) )
						{
							Double finalCost = calculateCost(dataObject, systemName, "Provide", true);
							if(finalCost == null) {
								newRow[i+2] = "Cost already taken into consideration.";
								newRow[i+3] = "";
							} else if(finalCost != (double) 0){
								newRow[i+2] = finalCost;
								totalDirectCost += finalCost;
								newRow[i+3] = "";
							} else {
								newRow[i+2] = "No data present to calculate loe.";
								newRow[i+3] = "";
							}
						} 
						//////////////////////////////////////////////////////////////////////////////////////////////
						else 
						{
							if(lpiSystemList.contains(interfacingSystem) && dhmsmSORList.contains(dataObject))
							{
								Double finalCost = calculateCost(dataObject, interfacingSystem, "Consume", false);
								if(finalCost == null) {
									newRow[i+3] = "Cost already taken into consideration.";
									newRow[i+2] = "";
								} else if(finalCost != (double) 0){
									newRow[i+3] = finalCost;
									totalIndirectCost += finalCost;
									newRow[i+2] = "";
								} else {
									newRow[i+3] = "No data present to calculate loe.";
									newRow[i+2] = "";
								}
							} else {
								newRow[i+2] = "";
								newRow[i+3] = "";
							}
						}
					}
				} else {
					newRow[i] = row[i];
				}
			}
			newData.add(newRow);
		}

		dataHash.put(dataKey, newData);
		dataHash.put(headerKey, newHeaders);
		dataHash.put(totalDirectCostKey, totalDirectCost);
		dataHash.put(totalIndirectCostKey, totalIndirectCost);

		return dataHash;
	}

	private Double calculateCost(String dataObject, String system, String tag, boolean includeGenericCost)
	{
		double sysGLItemCost = 0;
		double genericCost = 0;

		ArrayList<String> sysGLItemServices = new ArrayList<String>();
		// get sysGlItem for provider lpi systems
		HashMap<String, Double> sysGLItem = loeForSysGlItemHash.get(dataObject);
		HashMap<String, Double> avgSysGLItem = avgLoeForSysGlItemHash.get(dataObject);

		boolean useAverage = true;
		boolean servicesAllUsed = false;
		if(sysGLItem != null)
		{
			for(String sysSerGLTag : sysGLItem.keySet())
			{
				String[] sysSerGLTagArr = sysSerGLTag.split("\\+\\+\\+");
				if(sysSerGLTagArr[0].equals(system))
				{
					if(sysSerGLTagArr[2].contains(tag))
					{
						useAverage = false;
						String ser = sysSerGLTagArr[1];
						if(!servicesProvideList.contains(ser)) {
							sysGLItemServices.add(ser);
							servicesProvideList.add(ser);
							sysGLItemCost += sysGLItem.get(sysSerGLTag);
						} else {
							servicesAllUsed = true;
						}
					} // else do nothing - do not care about consume loe
				}
			}
		}
		// else get the average system cost
		if(useAverage)
		{
			if(avgSysGLItem != null)
			{
				for(String serGLTag : avgSysGLItem.keySet())
				{
					String[] serGLTagArr = serGLTag.split("\\+\\+\\+");
					if(serGLTagArr[1].contains(tag))
					{
						String ser = serGLTagArr[0];
						if(!servicesProvideList.contains(ser)) {
							sysGLItemServices.add(ser);
							servicesProvideList.add(ser);
							sysGLItemCost += avgSysGLItem.get(serGLTag);
						} else {
							servicesAllUsed = true;
						}
					}
				}
			}
		}

		if(includeGenericCost)
		{
			HashMap<String, Double> genericGLItem = loeForGenericGlItemHash.get(dataObject);
			if(genericGLItem != null)
			{
				for(String ser : genericGLItem.keySet())
				{
					if(sysGLItemServices.contains(ser)) {
						genericCost += genericGLItem.get(ser);
					} 
				}
			}
		}

		Double finalCost = null;
		if(!servicesAllUsed) {
			finalCost = (sysGLItemCost + genericCost) * costPerHr;
		}

		return finalCost;
	}


	private HashMap<String, Object> getSysSORTableWithHeaders(IEngine engine,String sysSORDataQuery,String otherSysSORDataQuery)
	{
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		ArrayList<String> dataObjectsProvided = new ArrayList<String>();
		ArrayList<String> dataObjectsConsumed = new ArrayList<String>();
		ArrayList<String> systems = new ArrayList<String>();

		//making list of SOR systems
		SesameJenaSelectWrapper sjsw = processQuery(engine,sysSORDataQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String data = (String)sjss.getVar(names[0]);
			String provideConsume = (String)sjss.getVar(names[1]);
			String service = (String)sjss.getVar(names[2]);
			String otherSystem = (String)sjss.getVar(names[3]);
			if(provideConsume.startsWith("\"") && provideConsume.endsWith("\""))
				provideConsume = provideConsume.substring(1, provideConsume.toString().length()-1); // remove annoying quotes
			Object[] dataRow;

			if(provideConsume.toLowerCase().contains("consume")){
				int dataIndex = dataObjectsConsumed.indexOf(data);
				if(dataIndex<0){
					dataRow = new Object[]{data,provideConsume,service,0};
					dataObjectsConsumed.add(data);
					dataToAddArr.add(dataRow);
				}
			}
			else{
				int dataIndex = dataObjectsProvided.indexOf(data);
				if(dataIndex<0){
					dataRow = new Object[]{data,provideConsume,service,1};
					dataObjectsProvided.add(data);
					dataToAddArr.add(dataRow);
				}
				else{
					dataRow = dataToAddArr.get(dataIndex);
					dataRow[3]= (Integer)dataRow[3]+1;
					dataToAddArr.set(dataIndex, dataRow);
				}
			}
			if(!systems.contains(otherSystem))
				systems.add(otherSystem);

		}

		// sort selected systems
		Vector <String> systemsVector = new Vector<String>(systems);
		Collections.sort(systemsVector);
		systems = new ArrayList<String>(systemsVector);

		//update list to include spots for all systems
		for(int i=0;i<dataToAddArr.size();i++)
		{
			Object[] oldRow = dataToAddArr.get(i);
			Object[] newRow = new Object[oldRow.length + systems.size()];
			for(int j=0;j<oldRow.length;j++)
				newRow[j] = oldRow[j];
			for(int j=oldRow.length;j<oldRow.length+systems.size();j++)
				newRow[j] = 0;
			dataToAddArr.set(i,newRow);
		}

		ArrayList<Object[]> otherSysList = new ArrayList<Object[]>();
		SesameJenaSelectWrapper sjsw3 = processQuery(engine,otherSysSORDataQuery);
		String[] names3 = sjsw3.getVariables();
		while(sjsw3.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw3.next();
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
			int indSys = systems.indexOf(row[0]);
			int indData = dataObjectsConsumed.indexOf(row[1]);
			if(indSys>-1 && indData>-1)
			{
				Object[] rowToUpdate = dataToAddArr.get(dataObjectsProvided.size()+indData);
				rowToUpdate[indSys+4] = row[2];
				dataToAddArr.set(dataObjectsProvided.size()+indData, rowToUpdate);
			}
		}

		String[] headers = new String[4+systems.size()];
		headers[0] = "Data Object";
		headers[1] = "Provides or Consumes";
		headers[2] = "Services";
		headers[3] = systemName;
		for(int i=0;i<systems.size();i++)
			headers[i+4] = systems.get(i);

		dataHash.put(headerKey,headers);
		dataHash.put(dataKey, dataToAddArr);
		return dataHash;
	}


	private HashMap<String, Object> getQueryDataWithHeaders(IEngine engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(headerKey, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
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

		dataHash.put(dataKey, dataToAddArr);

		return dataHash;
	}

	private HashMap<String, HashMap<String, Double>> getSysGLItem(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
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

	private HashMap<String, HashMap<String, Double>> getAvgSysGLItem(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
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

	private HashMap<String, HashMap<String, Double>> getGenericGLItem(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
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

	private HashMap<String, String> getServiceToData(IEngine engine, String query)
	{
		HashMap<String, String> dataHash = new HashMap<String, String>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");

			dataHash.put(data, ser);
		}
		return dataHash;
	}

	private HashSet<String> runListQuery(IEngine engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String var = sjss.getVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
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

	private SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}

	@Override
	public void refineView() {
	}

	@Override
	public void overlayView() {
	}

	@Override
	public void runAnalytics() {
	}

}
