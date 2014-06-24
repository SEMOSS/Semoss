package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import prerna.poi.specific.IndividualSystemTransitionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IndividualLPISystemTransitionReport extends AbstractRDFPlaySheet{

	// list of queries to run
	private String sysInfoQuery = "SELECT DISTINCT ?Description (COALESCE(?GT, 'Garrison') AS ?GarrisonTheater) (IF(BOUND(?MU),'Yes','No') AS ?MHS_Specific) ?Transaction_Count (COALESCE(SUBSTR(STR(?ATO),0,10),'NA') AS ?ATO_Date) (COALESCE(SUBSTR(STR(?ES),0,10),'NA') AS ?End_Of_Support) ?Num_Users WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> ?MU}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}}OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?Transaction_Count}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?ES}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?Num_Users}} }";
	private String sysDataConsumeProvideQuery = "SELECT DISTINCT ?Data ?ProvideOrConsume (GROUP_CONCAT(?Ser; SEPARATOR = ', ') AS ?Services) WHERE { {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>}{?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} OPTIONAL {{?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data}} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?otherSystem} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data}  FILTER(!BOUND(?icd2)) BIND('Provide' AS ?ProvideOrConsume)}} UNION {SELECT DISTINCT ?System ?Data ?ProvideOrConsume (SUBSTR(STR(?Service), 46) AS ?Ser) WHERE {BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?otherSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>}   {?otherSystem <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?Service <http://semoss.org/ontologies/Relation/Exposes> ?Data} BIND('Consume' AS ?ProvideOrConsume)}} } GROUP BY ?System ?Data ?ProvideOrConsume ORDER BY ?System";
	private String sysSORDataWithDHMSMQuery = "SELECT DISTINCT ?Data ?DHMSM_SOR WHERE { {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}  {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} FILTER( !regex(str(?crm),'R')) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } } } UNION {SELECT DISTINCT ?System ?Data (IF(BOUND(?Needs),'Yes','No') AS ?DHMSM_SOR) WHERE {BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>}{?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } FILTER(!BOUND(?icd2)) OPTIONAL { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} } }} } ORDER BY ?System";
	private String sysSORDataWithDHMSMCapQuery = "SELECT DISTINCT ?Capability ?Data (GROUP_CONCAT(DISTINCT ?Comment ; Separator = ' and ') AS ?Comments) WHERE { { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> }{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?SystemProvideData_CRM} FILTER( !regex(str(?SystemProvideData_CRM),'R')) {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?System <http://semoss.org/ontologies/Relation/Provide> ?icd} {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data} {?System ?provideData ?Data} {?Task ?Needs ?Data} BIND('ICD validated through downstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } UNION { SELECT DISTINCT ?Capability ?Data ?System ?Comment ?DHMSMcrm WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'} {?Task ?Needs ?Data} OPTIONAL { {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?System} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?Data} } {?System <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?Data } FILTER(!BOUND(?icd2)) BIND('ICD implied through downstream and no upstream interfaces' AS ?Comment) { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability.} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } GROUP BY ?Data } BIND('R' AS ?DHMSMcrm) } } } GROUP BY ?Capability ?Data ?System ORDER BY ?Capability ?Data ?System";
	private String softwareLifeCycleQuery = "SELECT DISTINCT ?System ?SoftwareVersion (COALESCE(xsd:dateTime(?SoftMEOL), COALESCE(xsd:dateTime(?SoftVEOL),'TBD')) AS ?SupportDate) (COALESCE(xsd:dateTime(?SoftMEOL), COALESCE(xsd:dateTime(?SoftVEOL),'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemSWBudget) WHERE { BIND(@SYSTEM@ AS ?System){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?System ?Has ?SoftwareModule} {?SoftwareModule ?TypeOf ?SoftwareVersion} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVEOL} OPTIONAL {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMEOL} OPTIONAL {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?SoftwareVersion";
	private String hardwareLifeCycleQuery = "SELECT DISTINCT ?System ?HardwareVersion (COALESCE(xsd:dateTime(?HardMEOL), COALESCE(xsd:dateTime(?HardVEOL),'TBD')) AS ?SupportDate) (COALESCE(xsd:dateTime(?HardMEOL), COALESCE(xsd:dateTime(?HardVEOL),'TBD')) AS ?LifeCycle) (COALESCE(?unitcost,0) AS ?UnitCost) (COALESCE(?quantity,0) AS ?Quantity) (COALESCE(?UnitCost*?Quantity,0) AS ?TotalCost) (COALESCE(?budget,0) AS ?SystemHWBudget) WHERE { BIND(@SYSTEM@ AS ?System) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion } OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVEOL} OPTIONAL {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMEOL} OPTIONAL {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } ORDER BY ?System ?HardwareVersion";
	private String hwSWBudgetQuery = "SELECT DISTINCT ?System ?GLTag (max(coalesce(?FY15,0)) as ?fy15) WHERE {BIND(@SYSTEM@ AS ?System) { {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?System ?Has ?SystemBudgetGLItem}{?SystemBudgetGLItem ?TaggedBy ?GLTag}{?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15)} UNION {{?SystemServiceBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem> ;} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService> ;}{?ConsistsOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ConsistsOf>;} {?System ?ConsistsOf ?SystemService}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}  {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> ;} {?SystemService ?Has ?SystemServiceBudgetGLItem}{?SystemServiceBudgetGLItem ?TaggedBy ?GLTag}{?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget ;} {?SystemServiceBudgetGLItem ?OccursIn ?FYTag} {?OccursIn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OccursIn>;} BIND(if(?FYTag = <http://health.mil/ontologies/Concept/FYTag/FY15>, ?Budget,0) as ?FY15) } }GROUP BY ?System ?GLTag BINDINGS ?GLTag {(<http://health.mil/ontologies/Concept/GLTag/SW_Licen>) (<http://health.mil/ontologies/Concept/GLTag/HW>)}";
	private String sysInterfacesQuery = "HR_Core$HR_Core$HR_Core$HR_Core$ SELECT DISTINCT ?LPISystem ?InterfaceType ?InterfacingSystem ?Probability (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?Freq,'') AS ?Frequency) (COALESCE(?Prot,'') AS ?Protocol) ?DHMSM ?Comment WHERE { {SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?LPISystem) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem) (COALESCE(IF(BOUND(?y),IF(!REGEX(STR(?UpstreamSysProb1),'High'),'Low','High'),IF(BOUND(?x),IF(!REGEX(STR(?DownstreamSysProb1),'High'),'Low','High'),'')), '') AS ?Probability) ?interface ?Data ?format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provides','Consumes')) AS ?DHMSM) (COALESCE(?HIEsys, '') AS ?HIE) ?DHMSMcrm WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;}OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}}{?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} }} FILTER(REGEX(STR(?LPISystem), '@SYSTEMNAME@')) } ORDER BY ?Data $ SELECT DISTINCT (CONCAT(STR(?system), STR(?data)) AS ?sysDataKey) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?data ?system $ SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'} } BINDINGS ?Probability {('Medium')('Low')('Medium-High')} $ SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;} } BINDINGS ?HIEsys {('Y')}";
	
	private String loeForSysGlItemQuery = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) WHERE { BIND(@SYSTEM@ AS ?sys) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1";

	private String loeForGenericGlItemQuery = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?Loe) WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } GROUP BY ?data ?ser";
	private HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash = new HashMap<String, HashMap<String, Double>>();

	private String avgLoeForSysGLItemQuery = "SELECT DISTINCT ?data ?ser (ROUND(SUM(?loe)/COUNT(DISTINCT ?sys)) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1";
	private HashMap<String, HashMap<String, Double>> avgLoeForSysGlItemHash = new HashMap<String, HashMap<String, Double>>();

	private String serviceToDataQuery = "SELECT DISTINCT ?data (GROUP_CONCAT(?Ser; SEPARATOR = '; ') AS ?service) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data } BIND(SUBSTR(STR(?ser),46) AS ?Ser) } GROUP BY ?data";
	private HashMap<String, String> serviceToDataHash = new HashMap<String, String>();


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

	private boolean showMessages = true;

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

		if(loeForGenericGlItemHash.isEmpty()) {
			loeForGenericGlItemHash = getGenericGLItem(TAP_Cost_Data, loeForGenericGlItemQuery);
		}

		if(avgLoeForSysGlItemHash.isEmpty()) {
			avgLoeForSysGlItemHash = getSysGLItem(TAP_Cost_Data, avgLoeForSysGLItemQuery);
		}

		if(serviceToDataHash.isEmpty()) {
			serviceToDataHash = getServiceToData(TAP_Cost_Data, serviceToDataQuery);
		}

		systemURI = this.query;
		systemName = systemURI.substring(systemURI.lastIndexOf("/")+1,systemURI.lastIndexOf(">"));

		sysInfoQuery = sysInfoQuery.replace("@SYSTEM@", systemURI);
		sysDataConsumeProvideQuery = sysDataConsumeProvideQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMQuery = sysSORDataWithDHMSMQuery.replace("@SYSTEM@", systemURI);
		sysSORDataWithDHMSMCapQuery = sysSORDataWithDHMSMCapQuery.replace("@SYSTEM@", systemURI);
		softwareLifeCycleQuery = softwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		hardwareLifeCycleQuery = hardwareLifeCycleQuery.replace("@SYSTEM@", systemURI);
		hwSWBudgetQuery = hwSWBudgetQuery.replace("@SYSTEM@",systemURI);
		sysInterfacesQuery = sysInterfacesQuery.replaceAll("@SYSTEM@", systemURI);
		sysInterfacesQuery = sysInterfacesQuery.replaceAll("@SYSTEMNAME@", systemName);
		loeForSysGlItemQuery = loeForSysGlItemQuery.replace("@SYSTEM@", systemURI);

		HashMap<String, Object> sysInfoHash = getQueryDataWithHeaders(hr_Core, sysInfoQuery);
		HashMap<String, Object> sysDataConsumeProvideHash = getQueryDataWithHeaders(hr_Core, sysDataConsumeProvideQuery);
		HashMap<String, Object> sysSORDataWithDHMSMHash = getQueryDataWithHeaders(hr_Core, sysSORDataWithDHMSMQuery);
		HashMap<String, Object> sysSORDataWithDHMSMCapHash = getQueryDataWithHeaders(hr_Core, sysSORDataWithDHMSMCapQuery);
		HashMap<String, Object> hwSWBudgetHash = getQueryDataWithHeaders(TAP_Cost_Data, hwSWBudgetQuery);

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

		LPIInterfaceReportGenerator getSysLPIInterfaceData = new LPIInterfaceReportGenerator();
		getSysLPIInterfaceData.setQuery(sysInterfacesQuery);
		getSysLPIInterfaceData.createData();
		HashMap<String, Object> sysLPIInterfaceHash = new HashMap<String, Object>();
		sysLPIInterfaceHash.put(headerKey, removeSystemFromStringArray(getSysLPIInterfaceData.getNames()));
		sysLPIInterfaceHash.put(dataKey, removeSystemFromArrayList(getSysLPIInterfaceData.getList()));

		HashMap<String, HashMap<String, Double>> loeForSysGlItemHash = getSysGLItem(TAP_Cost_Data, loeForSysGlItemQuery);

		HashMap<String, Object> sysLPIInterfaceWithCostHash = createLPIInterfaceWithCostHash(sysLPIInterfaceHash, loeForSysGlItemHash, loeForGenericGlItemHash);

		IndividualSystemTransitionReportWriter writer = new IndividualSystemTransitionReportWriter();
		writer.makeWorkbook(Utility.getInstanceName(systemURI.replace(">", "").replace("<", "")));
		writer.writeSystemInfoSheet("System Overview",sysInfoHash);
		writer.writeHWSWSheet("Software Lifecycles", storeSoftwareData.get(0), storeSoftwareData.get(1), storeSoftwareData.get(2));
		writer.writeHWSWSheet("Hardware Lifecycles", storeHardwareData.get(0), storeHardwareData.get(1), storeHardwareData.get(2));
		writer.writeModernizationTimelineSheet("Modernization Timeline", storeSoftwareData.get(0), storeHardwareData.get(0), hwSWBudgetHash);
		writer.writeListSheet("System Data", sysDataConsumeProvideHash);
		writer.writeListSheet("Data Provenance", sysSORDataWithDHMSMHash);
		writer.writeListSheet("DHMSM Data Requirements", sysSORDataWithDHMSMCapHash);
		writer.writeListSheet("System Interfaces", sysLPIInterfaceWithCostHash);
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

	private HashMap<String, Object> createLPIInterfaceWithCostHash(HashMap<String, Object> sysLPIInterfaceHash, HashMap<String, HashMap<String, Double>> loeForSysGlItemHash, HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash) {

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
				newHeaders[i+1] = "Comments";
				newHeaders[i+2] = "Direct Cost";
				newHeaders[i+3] = "Indirect Cost";
			}
		}

		ArrayList<Object[]> newData = new ArrayList<Object[]>();
		HashSet<String> servicesProvideList = new HashSet<String>();
		HashSet<String> servicesConsumeList = new HashSet<String>();
		double totalDirectCost = 0;
		double totalIndirectCost = 0;
		String dataObject = "";

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
						newRow[i] = "No Services";
					}
					
					String comment = row[i].toString().replaceAll("\"", "");
					newRow[i+1] = comment;
					if(!comment.contains("->"))
					{
						newRow[i+2] = "";
						newRow[i+3] = "";
					}
					else
					{
						String[] commentSplit = comment.split("->");

						double sysGLItemProviderCost = 0;
						double sysGLItemConsumerCost = 0;
						double genericCost = 0;

						if(commentSplit[1].contains("DHMSM")) // this means LPI provide data to DHMSM 
						{
							ArrayList<String> sysGLItemServices = new ArrayList<String>();

							// get sysGlItem for provider lpi systems
							HashMap<String, Double> sysGLItem = loeForSysGlItemHash.get(dataObject);
							HashMap<String, Double> avgSysGLItem = avgLoeForSysGlItemHash.get(dataObject);

							boolean useAverage = true;
							boolean servicesAllUsed = false;
							if(sysGLItem != null)
							{
								for(String glTagSer : sysGLItem.keySet())
								{
									String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
									if(glTagSerArr[1].contains("Provider"))
									{
										useAverage = false;
										String ser = glTagSerArr[0];
										if(!servicesProvideList.contains(ser)) {
											sysGLItemServices.add(ser);
											servicesProvideList.add(ser);
											sysGLItemProviderCost += sysGLItem.get(glTagSer);
										} else {
											servicesAllUsed = true;
										}
									}
									// else do nothing - do not care about consume loe
								}
							}
							// else get the average system cost
							if(useAverage)
							{
								if(avgSysGLItem != null)
								{
									for(String glTagSer : avgSysGLItem.keySet())
									{
										String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
										if(glTagSerArr[1].contains("Provider"))
										{
											String ser = glTagSerArr[0];
											if(!servicesProvideList.contains(ser)) {
												sysGLItemServices.add(ser);
												servicesProvideList.add(ser);
												sysGLItemProviderCost += avgSysGLItem.get(glTagSer);
											} else {
												servicesAllUsed = true;
											}
										}
										// else do nothing - do not care about consume loe
									}
								}
							}

							// get genericGlItem cost for provider lpi systems
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

							Double finalCost = (sysGLItemProviderCost + genericCost) * costPerHr;
							if(finalCost != (double) 0){
								newRow[i+2] = finalCost;
								totalDirectCost += finalCost;
								newRow[i+3] = "";
							} else if(servicesAllUsed){
								newRow[i+2] = "Cost already taken into consideration";
								newRow[i+3] = "";
							} else {
								newRow[i+2] = "No data present to calculate loe";
								newRow[i+3] = "";
							}
						}
						
						//////////////////////////////////////////////////////////////////////////////////////////////
						//////////////////////////////////////////////////////////////////////////////////////////////
						else // this means LPI consume from DHMSM
						{
							if(commentSplit[1].contains(systemName) && !deleteOtherInterfaces)
							{
								HashMap<String, Double> sysGLItem = loeForSysGlItemHash.get(dataObject);
								HashMap<String, Double> avgSysGLItem = avgLoeForSysGlItemHash.get(dataObject);

								boolean useAverage = true;
								boolean servicesAllUsed = false;
								if(sysGLItem != null)
								{
									for(String glTagSer : sysGLItem.keySet())
									{
										String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
										if(glTagSerArr[1].contains("Consume"))
										{
											useAverage = false;
											String ser = glTagSerArr[0];
											if(!servicesConsumeList.contains(ser)) {
												servicesConsumeList.add(ser);
												sysGLItemConsumerCost += sysGLItem.get(glTagSer);
											} else {
												servicesAllUsed = true;
											}
										}
										// else do nothing - do not care about provide loe
									}
								}
								// else get the average system cost
								if(useAverage)
								{
									if(avgSysGLItem != null)
									{
										for(String glTagSer : avgSysGLItem.keySet())
										{
											String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
											if(glTagSerArr[1].contains("Consume"))
											{
												String ser = glTagSerArr[0];
												if(!servicesConsumeList.contains(ser)) {
													servicesConsumeList.add(ser);
													sysGLItemConsumerCost += avgSysGLItem.get(glTagSer);
												} else {
													servicesAllUsed = true;
												}
											}
											// else do nothing - do not care about provide loe
										}
									}
								}

								Double finalCost = (sysGLItemConsumerCost) * costPerHr;
								if(finalCost != (double) 0) {
									newRow[i+2] = finalCost;
									totalDirectCost += finalCost;
									newRow[i+3] = "";
								} else if(servicesAllUsed) {
									newRow[i+2] = "Cost already taken into consideration";
									newRow[i+3] = "";
								} else {
									newRow[i+2] = "No data present to calculate loe";
									newRow[i+3] = "";
								}

								deleteOtherInterfaces = true;
								for(Integer index : indexArr)
								{
									if(index < newData.size() && newData.get(index) != null) // case when first row is the LPI system and hasn't been added to newData yet
									{
										Object[] modifyCost = newData.get(index);
										modifyCost[i+1] = "Remove existing Interface";
										modifyCost[i+2] = "";
										modifyCost[i+3] = "";
									}
								}
							} else if(deleteOtherInterfaces) {
								newRow[i+1] = "Remove existing Interface";
								newRow[i+2] = "";
								newRow[i+3] = "";
							} else {
								HashMap<String, Double> sysGLItem = loeForSysGlItemHash.get(dataObject);
								HashMap<String, Double> avgSysGLItem = avgLoeForSysGlItemHash.get(dataObject);

								boolean useAverage = true;
								boolean servicesAllUsed = false;
								if(sysGLItem != null)
								{
									for(String glTagSer : sysGLItem.keySet())
									{
										String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
										if(glTagSerArr[1].contains("Consume"))
										{
											useAverage = false;
											sysGLItemConsumerCost += sysGLItem.get(glTagSer);
											}
										}
										// else do nothing - do not care about provide loe
									}
								// else get the average system cost
								if(useAverage)
								{
									if(avgSysGLItem != null)
									{
										for(String glTagSer : avgSysGLItem.keySet())
										{
											String[] glTagSerArr = glTagSer.split("\\+\\+\\+");
											if(glTagSerArr[1].contains("Consume"))
											{
												sysGLItemConsumerCost += avgSysGLItem.get(glTagSer);
											} 
											// else do nothing - do not care about provide loe
										}
									}
								}

								Double finalCost = (sysGLItemConsumerCost) * costPerHr;
								if(finalCost != (double) 0){
									newRow[i+3] = finalCost;
									totalIndirectCost += finalCost;
									newRow[i+2] = "";
								} else if(servicesAllUsed){
									newRow[i+3] = "Cost already taken into consideration";
									newRow[i+2] = "";
								} else {
									newRow[i+3] = "No data present to calculate loe";
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
		// TODO Auto-generated method stub
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}

}
