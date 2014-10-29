package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

public class DHMSMTransitionUtility {
	
	private DHMSMTransitionUtility() {
		
	}
	// string to modify for LP Interface processor
	// change @SYSTEMNAME@ 
	public static String lpSystemInterfacesQuery = "SELECT DISTINCT ?System ?InterfaceType ?InterfacingSystem ?Probability (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?Freq,'') AS ?Frequency) (COALESCE(?Prot,'') AS ?Protocol) ?DHMSM ?Recommendation WHERE { {SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?System) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem)  (COALESCE(IF(BOUND(?y),IF(?UpstreamSysProb1 != 'High' && ?UpstreamSysProb1 != 'Question','Low','High'),IF(BOUND(?x),IF(?DownstreamSysProb1 != 'High' &&?DownstreamSysProb1 != 'Question','Low','High'),'')), '') AS ?Probability) ?interface ?Data ?format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consume')) AS ?DHMSM) (COALESCE(?HIEsys, '') AS ?HIE) ?DHMSMcrm WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND('N' AS ?InterfaceYN) LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} }} FILTER(REGEX(STR(?System), '^http://health.mil/ontologies/Concept/System/@SYSTEMNAME@$')) } ORDER BY ?System ?InterfacingSystem ?Data";

	public static final String HEADER_KEY = "headers";
	public static final String DATA_KEY = "data";
	public static final String TOTAL_DIRECT_COST_KEY = "directCost";
	public static final String TOTAL_INDIRECT_COST_KEY = "indirectCost";
	public static final String SYS_URI_PREFIX = "http://health.mil/ontologies/Concept/System/";
	
	public static final String LOE_SYS_GLITEM_QUERY = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?sys ?data ?ser ?gltag1";
	public static final String LOE_GENERIC_GLITEM_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?Loe) WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } GROUP BY ?data ?ser";
	public static final String AVG_LOE_SYS_GLITEM_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe)/COUNT(DISTINCT ?sys) AS ?Loe) ?gltag1 WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) ?phase WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1";
	public static final String SERVICE_TO_DATA_QUERY = "SELECT DISTINCT ?data (GROUP_CONCAT(?Ser; SEPARATOR = '; ') AS ?service) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?ser <http://semoss.org/ontologies/Relation/Exposes> ?data } BIND(SUBSTR(STR(?ser),46) AS ?Ser) } GROUP BY ?data";

	public static final String SYS_SPECIFIC_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) (SUBSTR(STR(?phase), 48) AS ?Phase) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } ORDER BY ?sys ?ser ?gltag1";
	public static final String GENERIC_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?data ?ser ?loe (SUBSTR(STR(?phase), 48) AS ?Phase) WHERE { BIND(<http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} }";
	public static final String AVERAGE_LOE_AND_PHASE_QUERY = "SELECT DISTINCT ?data ?ser (SUM(?loe)/COUNT(DISTINCT ?sys) AS ?Loe) ?gltag1 ?Phase WHERE { SELECT DISTINCT ?sys ?data ?ser ?loe (SUBSTR(STR(?gltag), 44) AS ?gltag1) (SUBSTR(STR(?phase), 48) AS ?Phase) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?sys <http://semoss.org/ontologies/Relation/Provide> ?data} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TransitionGLItem>} {?gltag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser} {?data <http://semoss.org/ontologies/Relation/Input> ?GLitem} } } GROUP BY ?data ?ser ?gltag1 ?Phase";
	
	public static final String DHMSM_SOR_QUERY = "SELECT DISTINCT ?Data WHERE { BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM) {?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>} {?DHMSM <http://semoss.org/ontologies/Relation/TaggedBy> ?Capability} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability <http://semoss.org/ontologies/Relation/Consists> ?Task} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'} {?Task ?Needs ?Data} }";

	public static final String LPI_SYS_QUERY = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	public static final String SYS_TYPE_QUERY = "SELECT DISTINCT ?entity (IF((?Probability='Low'||?Probability='Medium'||?Probability='Medium-High'), IF(?interface='Y','LPI','LPNI'), IF(?interface='Y','HPI','HPNI')) AS ?ReportType) WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?interface}} BINDINGS ?Probability {('Low')('Medium')('Medium-High')('High')('Question')}";

	public static final String SYS_SOR_DATA_CONCAT_QUERY = "SELECT DISTINCT (CONCAT(STR(?system), STR(?data)) AS ?sysDataKey) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?data ?system";

	public static final String ALL_SELF_REPORTED_ICD_QUERY = "SELECT DISTINCT ?ICD WHERE { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?ICD ?Carries ?Data} {?Carries <http://semoss.org/ontologies/Relation/Contains/Recommendation> 'Self_Reported'} }";
	
	public static Set<String> getAllSelfReportedICDQuery(IEngine engine) {
		Set<String> retList = new HashSet<String>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, ALL_SELF_REPORTED_ICD_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String icd = sjss.getRawVar(varName[0]).toString();
			retList.add(icd);
		}
		return retList;
	}
	
	public static HashMap<String,String> processReportTypeQuery(IEngine engine) {
		HashMap<String,String> retList = new HashMap<String,String>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SYS_TYPE_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String instance = sjss.getVar(varName[0]).toString();
			String reportType = sjss.getVar(varName[1]).toString();
			retList.put(instance,reportType);
		}

		return retList;
	}
	
	public static HashSet<String> processSysDataSOR(IEngine engine){
		String[] names = new String[1];
		SesameJenaSelectWrapper sorWrapper = new SesameJenaSelectWrapper();
		sorWrapper.setQuery(SYS_SOR_DATA_CONCAT_QUERY);
		sorWrapper.setEngine(engine);
		sorWrapper.executeQuery();
		names = sorWrapper.getVariables();

		HashSet<String> retSet = new HashSet<String>();
		while(sorWrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = sorWrapper.next();
			int colIndex = 0;
			String val = sjss.getVar(names[colIndex]).toString();
			retSet.add(val);
		}
		return retSet;
	}
	
	public static HashMap<String, HashMap<String, Double>> getSysGLItem(IEngine engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, LOE_SYS_GLITEM_QUERY);
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

	public static HashMap<String, HashMap<String, Double>> getAvgSysGLItem(IEngine engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, AVG_LOE_SYS_GLITEM_QUERY);
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

	public static HashMap<String, HashMap<String, Double>> getGenericGLItem(IEngine engine)
	{
		HashMap<String, HashMap<String, Double>> dataHash = new HashMap<String, HashMap<String, Double>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, LOE_GENERIC_GLITEM_QUERY);
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

	public static HashMap<String, String> getServiceToData(IEngine engine)
	{
		HashMap<String, String> dataHash = new HashMap<String, String>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SERVICE_TO_DATA_QUERY);
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
	
	public static HashMap<String, HashMap<String, HashMap<String, Double>>> getSysGLItemAndPhase(IEngine engine)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, SYS_SPECIFIC_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString().replace("\"", "");
			String data = sjss.getVar(names[1]).toString().replace("\"", "");
			String ser = sjss.getVar(names[2]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[3]);
			String glTag = sjss.getVar(names[4]).toString().replace("\"", "");
			String phase = sjss.getVar(names[5]).toString().replace("\"", "");
			
			HashMap<String, HashMap<String, Double>> outerHash = new HashMap<String, HashMap<String, Double>>();
			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				innerHash.put(phase, loe);
				outerHash.put(sys + "+++" + ser + "+++" + glTag, innerHash);
				dataHash.put(data, outerHash);
			} else {
				outerHash = dataHash.get(data);
				if(outerHash.containsKey(sys + "+++" + ser + "+++" + glTag)) {
					innerHash = outerHash.get(sys + "+++" + ser + "+++" + glTag);
					innerHash.put(phase, loe);
				} else {
					innerHash.put(phase, loe);
					outerHash.put(sys + "+++" + ser + "+++" + glTag, innerHash);
				}
			}
		}
		return dataHash;
	}

	public static HashMap<String, HashMap<String, HashMap<String, Double>>> getAvgSysGLItemAndPhase(IEngine engine)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, AVERAGE_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String glTag = sjss.getVar(names[3]).toString().replace("\"", "");
			String phase = sjss.getVar(names[4]).toString().replace("\"", "");

			HashMap<String, HashMap<String, Double>> outerHash = new HashMap<String, HashMap<String, Double>>();
			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				innerHash.put(phase, loe);
				outerHash.put(ser + "+++" + glTag, innerHash);
				dataHash.put(data, outerHash);
			} else {
				outerHash = dataHash.get(data);
				if(outerHash.containsKey(ser + "+++" + glTag)) {
					innerHash = outerHash.get(ser + "+++" + glTag);
					innerHash.put(phase, loe);
				} else {
					innerHash.put(phase, loe);
					outerHash.put(ser + "+++" + glTag, innerHash);
				}
			}
		}
		return dataHash;
	}

	public static HashMap<String, HashMap<String, HashMap<String, Double>>> getGenericGLItemAndPhase(IEngine engine)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, GENERIC_LOE_AND_PHASE_QUERY);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String data = sjss.getVar(names[0]).toString().replace("\"", "");
			String ser = sjss.getVar(names[1]).toString().replace("\"", "");
			Double loe = (Double) sjss.getVar(names[2]);
			String phase = sjss.getVar(names[3]).toString().replace("\"", "");

			HashMap<String, HashMap<String, Double>> outerHash = new HashMap<String, HashMap<String, Double>>();
			HashMap<String, Double> innerHash = new HashMap<String, Double>();

			if(!dataHash.containsKey(data)) {
				innerHash.put(phase, loe);
				outerHash.put(ser, innerHash);
				dataHash.put(data, outerHash);
			} else {
				outerHash = dataHash.get(data);
				if(outerHash.containsKey(ser)) {
					innerHash = outerHash.get(ser);
					innerHash.put(phase, loe);
				} else {
					innerHash.put(phase, loe);
					outerHash.put(ser, innerHash);
				}
			}
		}
		return dataHash;
	}

	public static HashSet<String> runVarListQuery(IEngine engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String var = sjss.getVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
	}
	
	public static HashSet<String> runRawVarListQuery(IEngine engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String var = sjss.getRawVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
	}
	
	public static String[] removeSystemFromStringArray(String[] names)
	{
		String[] retArray = new String[names.length-1];
		for(int j=0;j<retArray.length;j++)
			retArray[j] = names[j+1];
		return retArray;
	}

	public static ArrayList<Object[]> removeSystemFromArrayList(ArrayList<Object[]> dataRow)
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

}
