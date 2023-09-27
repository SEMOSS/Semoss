package prerna.ui.components.specific.tap;

import java.util.Iterator;
import java.util.Map;

import prerna.engine.api.IDatabaseEngine;

public abstract class AbstractLPInterfaceProcessor {

	// string to modify for LP Interface processor, change @SYSTEMNAME@ 
	protected static final String LP_SYSTEM_DONWSTREAM_INTERFACE_QUERY = "SELECT DISTINCT ?System ?InterfaceType ?InterfacingSystem ?Disposition (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?freq,'') AS ?Freq) (COALESCE(?prot,'') AS ?Prot) ?MHS_GENESIS ?Recommendation WHERE { { SELECT DISTINCT (?UpstreamSys AS ?System) ('Downstream' AS ?InterfaceType) (?DownstreamSys AS ?InterfacingSystem) (COALESCE(?DownstreamSysDisp1,'') AS ?Disposition) ?interface ?Data ?format ?freq ?prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consumer')) AS ?MHS_GENESIS) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device> 'N';} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI';}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) OPTIONAL{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> ?DownstreamSysDisp1;} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}  OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?prot ;}{?interface ?carries ?Data;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?interface ;} {?interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?dhmsm ) {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?dhmsm ?TaggedBy ?Capability.} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } } GROUP BY ?Data} } } FILTER(REGEX(STR(?System), '^http://health.mil/ontologies/Concept/System/@SYSTEMNAME@$')) } ORDER BY ?System ?InterfacingSystem ?Data";
	protected static final String LP_SYSTEM_UPSTREAM_INTERFACE_QUERY = "SELECT DISTINCT ?System ?InterfaceType ?InterfacingSystem ?Disposition (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?freq,'') AS ?Freq) (COALESCE(?prot,'') AS ?Prot) ?MHS_GENESIS ?Recommendation WHERE { { SELECT DISTINCT (?DownstreamSys AS ?System) ('Upstream' AS ?InterfaceType) (?UpstreamSys AS ?InterfacingSystem) (COALESCE(?UpstreamSysDisp1,'') AS ?Disposition) ?interface ?Data ?format ?freq ?prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consumer')) AS ?MHS_GENESIS) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}OPTIONAL{ {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device> 'N';} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI';} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))OPTIONAL{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> ?UpstreamSysDisp1;} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?prot ;}{?interface ?carries ?Data;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?interface ;} {?interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?dhmsm ) {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?dhmsm ?TaggedBy ?Capability.} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } } GROUP BY ?Data} } } FILTER(REGEX(STR(?System), '^http://health.mil/ontologies/Concept/System/@SYSTEMNAME@$')) } ORDER BY ?System ?InterfacingSystem ?Data";
	
	protected static final String DEFAULT_UPSTREAM_QUERY = "SELECT DISTINCT (?DownstreamSys AS ?System) ('Upstream' AS ?InterfaceType) (?UpstreamSys AS ?InterfacingSystem) (COALESCE(?UpstreamSysDisp1,'') AS ?Disposition) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?freq,'') AS ?Freq) (COALESCE(?prot,'') AS ?Prot) (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consumer')) AS ?MHS_GENESIS) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device> 'N';} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI';}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))OPTIONAL{ {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> ?UpstreamSysDisp1;} } {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?prot ;}{?Interface ?carries ?Data;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?dhmsm ) {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?dhmsm ?TaggedBy ?Capability.} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } } GROUP BY ?Data} } ORDER BY ?System ?InterfacingSystem";
	protected static final String DEFAULT_DOWNSTREAMSTREAM_QUERY = "SELECT DISTINCT (?UpstreamSys AS ?System) ('Downstream' AS ?InterfaceType) (?DownstreamSys AS ?InterfacingSystem) (COALESCE(?DownstreamSysDisp1,'') AS ?Disposition) ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?freq,'') AS ?Freq) (COALESCE(?prot,'') AS ?Prot) (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consumer')) AS ?MHS_GENESIS) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device> 'N';} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI';} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))OPTIONAL{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Disposition> ?DownstreamSysDisp1;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?prot ;} {?Interface ?carries ?Data;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSys ;} } { SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{ BIND(<http://health.mil/ontologies/Concept/MHS_GENESIS/MHS_GENESIS> AS ?dhmsm ) {?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;} {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?dhmsm ?TaggedBy ?Capability.} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Capability ?Consists ?Task.} {?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;} {?Task ?Needs ?Data.} } } GROUP BY ?Data} } ORDER BY ?System ?InterfacingSystem";
	
	protected String downstreamQuery = "";
	protected String upstreamQuery = "";
	
	protected static final String SYS_KEY = "System";
	protected static final String INTERFACE_TYPE_KEY = "InterfaceType";
	protected static final String INTERFACING_SYS_KEY = "InterfacingSystem";
	protected static final String DISPOSITION_KEY = "Disposition";
	protected static final String ICD_KEY = "Interface";
	protected static final String DATA_KEY = "Data";
	protected static final String FORMAT_KEY = "Format";
	protected static final String FREQ_KEY = "Freq";
	protected static final String PROT_KEY = "Prot";
	protected static final String DHMSM = "MHS_GENESIS";
	
	protected static final String DOWNSTREAM_KEY = "Downstream";
	
	protected IDatabaseEngine engine;
	
	public AbstractLPInterfaceProcessor() {
		downstreamQuery = DEFAULT_DOWNSTREAMSTREAM_QUERY;
		upstreamQuery = DEFAULT_UPSTREAM_QUERY;
	}
	
	public void setEngine(IDatabaseEngine engine) {
		this.engine = engine;
	}
	
	public void setQueriesForSysName(String sysName) {
		sysName = sysName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
		downstreamQuery = LP_SYSTEM_DONWSTREAM_INTERFACE_QUERY.replace("@SYSTEMNAME@", sysName);
		upstreamQuery = LP_SYSTEM_UPSTREAM_INTERFACE_QUERY.replace("@SYSTEMNAME@", sysName);
	}
	
	protected double aggregateCost(Map<String, Double> costHash) {
		double sum = 0;
		Iterator<Double> it = costHash.values().iterator();
		while(it.hasNext()) {
			Double val = it.next();
			if(val != null) {
				sum += val;
			}
		}
		return sum;
	}
}