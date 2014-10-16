package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.Utility;

public class LPInterfaceProcessor {
	
	private static final Logger LOGGER = LogManager.getLogger(LPInterfaceProcessor.class.getName());
	
	// direct cost and indirect costs requires 
	private HashMap<String, HashMap<String, Double>> loeForSysGlItemHash = new HashMap<String, HashMap<String, Double>>();
	private HashMap<String, HashMap<String, Double>> loeForGenericGlItemHash = new HashMap<String, HashMap<String, Double>>();
	private HashMap<String, HashMap<String, Double>> avgLoeForSysGlItemHash = new HashMap<String, HashMap<String, Double>>();
	private HashMap<String, String> serviceToDataHash = new HashMap<String, String>();

	// direct cost and indirect cost at phase requires
	private HashMap<String, HashMap<String, HashMap<String, Double>>> loeForSysGlItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	private HashMap<String, HashMap<String, HashMap<String, Double>>> genericLoeForSysGLItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	private HashMap<String, HashMap<String, HashMap<String, Double>>> avgLoeForSysGLItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	private HashMap<Integer, HashMap<String, Double>> sysCostInfo = new HashMap<Integer, HashMap<String, Double>>();
	private HashMap<String, Double> consolidatedSysCostInfo = new HashMap<String, Double>();
	
	HashSet<String> sysDataSOR = new HashSet<String>();
	HashMap<String, String> sysTypeHash = new HashMap<String, String>();

	// lpni indirect cost also requires
	private HashSet<String> dhmsmSORList;
	private HashSet<String> lpiSystemList;
	
	private final double COST_PER_HOUR = 150.0;
	private double totalDirectCost = 0;
	private double totalIndirectCost = 0;
	private boolean generateCost = false;
	
	private String query = "SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?System) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem) (COALESCE(IF(BOUND(?y),IF((?UpstreamSysProb1 != 'High' && ?UpstreamSysProb1 != 'Question'),'Low','High'),IF(BOUND(?x),IF((?DownstreamSysProb1!='High' && ?DownstreamSysProb1!='Question'),'Low','High'),'')), '') AS ?Probability) ?Interface ?Data ?Format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provider','Consume')) AS ?DHMSM) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND('N' AS ?InterfaceYN) BIND('Y' AS ?ReceivedInformation) LET(?d := 'd') { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;}OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}} OPTIONAL{ {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Received_Information> ?ReceivedInformation;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION { {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Received_Information> ?ReceivedInformation;} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} } ORDER BY ?System ?InterfacingSystem ?Data";	
	
	private final String SYS_KEY = "System";
	private final String INTERFACE_TYPE_KEY = "InterfaceType";
	private final String INTERFACING_SYS_KEY = "InterfacingSystem";
	private final String PROBABILITY_KEY = "Probability";
	private final String ICD_KEY = "Interface";
	private final String DATA_KEY = "Data";	
	private final String FORMAT_KEY = "Format";
	private final String FREQ_KEY = "Freq";
	private final String PROT_KEY = "Prot";
	private final String DHMSM = "DHMSM";

	private final String DOWNSTREAM_KEY = "Downstream";
	private final String DHMSM_PROVIDE_KEY = "Provider";
	private final String DHMSM_CONSUME_KEY = "Consume";
	private final String LPI_KEY = "LPI";
	//	private final String lpniKey = "LPNI"; 
	private final String HPI_KEY = "HPI";
	private final String HPNI_KEY = "HPNI";

	private final String DHMSM_URI = "http://health.mil/ontologies/Concept/System/DHMSM";
	private final String GLTAG_URI = "http://health.mil/ontologies/Concept/GLTag/";
	private final String SDLC_PHASE_URI = "http://health.mil/ontologies/Concept/SDLCPhase/";
	
	private final String provideInstanceRel = "http://health.mil/ontologies/Relation/Provide/";
	private final String consumeInstanceRel = "http://health.mil/ontologies/Relation/Consume/";
	private final String payloadInstanceRel = "http://health.mil/ontologies/Relation/Payload/";
	private final String inputInstanceRel = "http://health.mil/ontologies/Relation/Input/";
	private final String outputInstanceRel = "http://health.mil/ontologies/Relation/Output/";
	private final String taggedByInstanceRel = "http://health.mil/ontologies/Relation/TaggedBy/";
	private final String belongsToInstanceRel = "http://health.mil/ontologies/Relation/BelongsTo/";
	private final String precedesInstanceRel = "http://health.mil/ontologies/Relation/Precedes/";
	private final String influencesInstanceRel = "http://health.mil/ontologies/Relation/Influences/";
	
	private final String semossPropURI = "http://semoss.org/ontologies/Relation/Contains/";
	private final String newProp = "TypeWeight";

	private IEngine engine;
	private String[] names;
	private ArrayList<Object[]> list;

	// for future DB generation
	private boolean generateNewTriples;
	private boolean usePhase;

	private ArrayList<Object[]> relList;
	private ArrayList<Object[]> relPropList;
	private ArrayList<String> addedInterfaces;
	private ArrayList<String> removedInterfaces;
	private Set<String> sysList;
	
	private ArrayList<Object[]> costRelList;
	private ArrayList<Object[]> loeList;
	private Set<String> glItemList;
	private Set<String> sysCostList;

	public void isGenerateCost(boolean generateCost) {
		this.generateCost = generateCost;
	}
	
	public double getTotalDirectCost() {
		return totalDirectCost;
	}

	public double getTotalIndirectCost() {
		return totalIndirectCost;
	}
	
	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setEngine(IEngine engine){
		this.engine = engine;
	}

	public HashMap<Integer, HashMap<String, Double>> getSysCostInfo() {
		return sysCostInfo;
	}

	public HashMap<String, Double> getConsolidatedSysCostInfo() {
		return consolidatedSysCostInfo;
	}
	
	public void setSysCostInfo(HashMap<Integer, HashMap<String, Double>> sysCostInfo) {
		this.sysCostInfo = sysCostInfo;
	}

	public void setConsolidatedSysCostInfo(HashMap<String, Double> consolidatedSysCostInfo) {
		this.consolidatedSysCostInfo = consolidatedSysCostInfo;
	}
	
	public void setGenerateNewTriples(boolean generateNewTriples) {
		this.generateNewTriples = generateNewTriples;
	}
	
	public void setUsePhase(boolean usePhase){
		this.usePhase = usePhase;
	}

	public String[] getNames(){
		return names;
	}

	public ArrayList<Object[]> getList(){
		return list;
	}
	
	public ArrayList<Object[]> getRelList(){
		return relList;
	}
	
	public ArrayList<Object[]> getPropList(){
		return relPropList;
	}
	
	public ArrayList<String> getAddedInterfaces(){
		return addedInterfaces;
	}
	
	public ArrayList<String> getRemovedInterfaces(){
		return removedInterfaces;
	}
	
	public Set<String> getSysList(){
		return sysList;
	}
	
	public ArrayList<Object[]> getCostRelList(){
		return costRelList;
	}
	
	public ArrayList<Object[]> getLoeList(){
		return loeList;
	}
	
	public Set<String> getGlItemList(){
		return glItemList;
	}
	
	public Set<String> getSysCostList(){
		return sysCostList;
	}
	
	public ArrayList<Object[]> generateReport() {
		list = new ArrayList<Object[]>();

		if(sysDataSOR.isEmpty()) {
			sysDataSOR = DHMSMTransitionUtility.processSysDataSOR(engine);
		}
		if(sysTypeHash.isEmpty()) {
			sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);
		}
		
		//Process main query
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		LOGGER.info("Main Query: " + query);
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		// get the bindings from it
		names = wrapper.getVariables();

		list = processBusinessRules(wrapper, names, sysDataSOR, sysTypeHash);

		return list;
	}

	private ArrayList<Object[]> processBusinessRules(SesameJenaSelectWrapper sjw, String[] names, HashSet<String> sorV, HashMap<String, String> sysTypeHash){
		ArrayList<Object[]> retList = new ArrayList<Object[]>();
		relList = new ArrayList<Object[]>();
		relPropList = new ArrayList<Object[]>();
		addedInterfaces = new ArrayList<String>();
		removedInterfaces = new ArrayList<String>();
		sysList = new HashSet<String>();
		
		costRelList = new ArrayList<Object[]>();
		loeList = new ArrayList<Object[]>();
		glItemList = new HashSet<String>();
		sysCostList = new HashSet<String>();
		
		// becomes true if either user 
		boolean getComments = false;
		
		// for generating cost
		ArrayList<Integer> indexArr = new ArrayList<Integer>();
		String previousSystem = "";
		String previousDataObject = "";
		boolean deleteOtherInterfaces = false;
		boolean directCost = true;
		boolean skipFistIteration = false;
		totalDirectCost = 0;
		totalIndirectCost = 0;
		HashSet<String> servicesProvideList = new HashSet<String>();
		int rowIdx = 0;
		
		while(sjw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjw.next();
			// get var's
			String sysName = "";
			String interfaceType = "";
			String interfacingSysName = "";
			String interfaceingSysProbability = "";
			String icd = "";
			String data = "";
			String format = "";
			String freq = "";
			String prot = "";
			String dhmsmSOR = "";

			String sysProbability = sysTypeHash.get(sysName);
			if(sysProbability == null) {
				sysProbability = "No Probability";
			}
			
			if(sjss.getVar(SYS_KEY) != null) {
				sysName = sjss.getVar(SYS_KEY).toString();
			}
			if(sjss.getVar(INTERFACE_TYPE_KEY) != null) {
				interfaceType = sjss.getVar(INTERFACE_TYPE_KEY).toString();
			}
			if(sjss.getVar(INTERFACING_SYS_KEY) != null) {
				interfacingSysName = sjss.getVar(INTERFACING_SYS_KEY).toString();
			}
			if(sjss.getVar(PROBABILITY_KEY) != null) {
				interfaceingSysProbability = sjss.getVar(PROBABILITY_KEY).toString();
			}
			if(sjss.getVar(ICD_KEY) != null) {
				icd = sjss.getVar(ICD_KEY).toString();
			}
			if(sjss.getVar(DATA_KEY) != null) {
				data = sjss.getVar(DATA_KEY).toString();
			}
			if(sjss.getVar(FORMAT_KEY) != null) {
				format = sjss.getVar(FORMAT_KEY).toString();
			}
			if(sjss.getVar(FREQ_KEY) != null) {
				freq = sjss.getVar(FREQ_KEY).toString();
			}
			if(sjss.getVar(PROT_KEY) != null) {
				prot = sjss.getVar(PROT_KEY).toString();
			}
			if(sjss.getVar(DHMSM) != null) {
				dhmsmSOR = sjss.getVar(DHMSM).toString();
			}
			
			// get uri's
			String system = "";
			String interfacingSystem = "";
			String icdURI = "";
			String dataURI = "";

			if(sjss.getRawVar(SYS_KEY) != null) {
				system = sjss.getRawVar(SYS_KEY).toString();
			}
			if(sjss.getRawVar(INTERFACING_SYS_KEY) != null) {
				interfacingSystem = sjss.getRawVar(INTERFACING_SYS_KEY).toString();
			}
			if(sjss.getRawVar(ICD_KEY) != null) {
				icdURI = sjss.getRawVar(ICD_KEY).toString();
			}
			if(sjss.getRawVar(DATA_KEY) != null) {
				dataURI = sjss.getRawVar(DATA_KEY).toString();
			}

			String comment = "";
			Object[] values;
			if(!generateNewTriples) {
				getComments = true;
			} else {
				sysList.add(DHMSM_URI);
			}
			if(generateCost) {
				values = new Object[names.length + 3];
				values[0] = sysName;
				values[1] = interfaceType;
				values[2] = interfacingSysName;
				values[3] = interfaceingSysProbability;
				values[4] = icd;
				values[5] = data;
				values[6] = format;
				values[7] = freq;
				values[8] = prot;
				values[9] = dhmsmSOR;
				values[10] = ""; //services
				values[11] = comment;
				values[12] = ""; //direct cost
				values[13] = ""; //indirect cost;
				
				// output the services
				String servicesList = serviceToDataHash.get(data);
				if(servicesList == null) {
					servicesList = "No Services.";
				}
				values[10] = servicesList;
			} else {
				values = new Object[names.length];
				values[0] = sysName;
				values[1] = interfaceType;
				values[2] = interfacingSysName;
				values[3] = interfaceingSysProbability;
				values[4] = icd;
				values[5] = data;
				values[6] = format;
				values[7] = freq;
				values[8] = prot;
				values[9] = dhmsmSOR;
				values[10] = comment;
			}
			
			//reset values if looping through multiple systems
			if(!previousSystem.equals(sysName))
			{
				previousSystem = sysName;
				totalDirectCost = 0;
				totalIndirectCost = 0;
				servicesProvideList.clear();
				sysCostInfo.clear();
			}
			if(!previousDataObject.equals(data))
			{
				previousDataObject = data;
				indexArr = new ArrayList<Integer>();
				deleteOtherInterfaces = false;
			}
			
			// determine which system is upstream or downstream
			String upstreamSysName = "";
			String upstreamSystemURI = "";
			String downstreamSysName = "";
			String downstreamSystemURI = "";
			if(interfaceType.contains(DOWNSTREAM_KEY)) { // lp system is providing data to interfacing system
				upstreamSystemURI = system;
				upstreamSysName = sysName;
				downstreamSystemURI = interfacingSystem;
				downstreamSysName = interfacingSysName;
			} else { // lp system is receiving data from interfacing system
				upstreamSystemURI = interfacingSystem;
				upstreamSysName = interfacingSysName;
				downstreamSystemURI = system;
				downstreamSysName = sysName;
			}
			
			String upstreamSysType = sysTypeHash.get(upstreamSysName);
			if(upstreamSysType == null) {
				upstreamSysType = "No Probability";
			}
			String downstreamSysType = sysTypeHash.get(downstreamSysName);
			if(downstreamSysType == null) {
				downstreamSysType = "No Probability";
			}
			
			// necessary to define even if not generating cost or new triples
			String newICD = "";
			String payloadURI = "";
			Double finalCost = null;
			boolean costCalculated = false;
			boolean noCost = false;
			// DHMSM is SOR of data
			if(dhmsmSOR.contains(DHMSM_PROVIDE_KEY)) {
				if(upstreamSysType.equals(LPI_KEY)) { // upstream system is LPI
					comment = comment.concat("Need to add interface DHMSM->").concat(upstreamSysName).concat(". ");
					
					// direct cost if system is upstream and indirect is downstream
					if(generateCost && !costCalculated) {
						costCalculated = true;
						if(sysName.equals(upstreamSysName)) {
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, upstreamSysName, "Consume", false, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, upstreamSysName, "Consume", false, servicesProvideList);
							}
							
							deleteOtherInterfaces = true;
							skipFistIteration = true;
							for(Integer index : indexArr)
							{
								if(index < retList.size() && retList.get(index) != null) // case when first row is the LPI system and hasn't been added to newData yet
								{
									Object[] modifyCost = retList.get(index);
									modifyCost[11] = "Interface already taken into consideration.";
									modifyCost[12] = "";
									modifyCost[13] = "";
								}
							}
						} else {
							directCost = false;
							if(usePhase) {
								finalCost = calculateCost(data, upstreamSysName, "Consume", false, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, upstreamSysName, "Consume", false, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, upstreamSysName, "Consume", false, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMProviderOfICD(icdURI, upstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMProvider(newICD, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future cost triple
						addFutureDBCostRelTriples("", newICD, DHMSM_URI, dataURI, data, rowIdx);
					}
					
					// if downstream system is HP, remove interface
					if(downstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY)) {
						comment = comment.concat(" Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).") ;
						// future db triples - removed interface
						removedInterfaces.add(icdURI);
						String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/")+1)).concat(":").concat(data);
						addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
						addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
					}
					
				} 
				// new business rule might be added - will either un-comment or remove after discussion today
				// else if (upstreamSysType.equals(lpniKey)) { // upstream system is LPNI
				// 		comment += "Recommend review of developing interface DHMSM->" + upstreamSysName + ". ";
				// } 
				else if(downstreamSysType.equals(LPI_KEY)) { // upstream system is not LPI and downstream system is LPI
					comment = comment.concat("Need to add interface DHMSM->").concat(downstreamSysName).concat(".").concat(" Recommend review of removing interface ")
							.concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(". ");
					// direct cost if system is downstream
					if(generateCost  && !costCalculated) {
						if(sysName.equals(downstreamSysName)) {
							costCalculated = true;
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, downstreamSysName, "Consume", false, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, downstreamSysName, "Consume", false, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, downstreamSysName, "Consume", false, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMProviderOfICD(icdURI, downstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMProvider(newICD, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future db triples - removed interface
						removedInterfaces.add(icdURI);
						String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/")+1)).concat(":").concat(data);
						addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
						addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
						// future cost triple
						addFutureDBCostRelTriples(icdURI, newICD, DHMSM_URI, dataURI, data, rowIdx);
					}
				} 
				else
				{
					noCost = true;
					if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
						comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
						if(generateNewTriples) { 
							// future db triples - removed interface
							removedInterfaces.add(icdURI);
							String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/")+1)).concat(":").concat(data);
							addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
							addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
						}
					} else {
						comment = "Stay as-is beyond FOC.";
					}
				}
			} else if(dhmsmSOR.contains(DHMSM_CONSUME_KEY)) {  // DHMSM is consumer of data
				boolean otherwise = true;
				if(upstreamSysType.equals(LPI_KEY) && sorV.contains(upstreamSystemURI + dataURI)) { // upstream system is LPI and SOR of data
					otherwise = false;
					comment = comment.concat("Need to add interface ").concat(upstreamSysName).concat("->DHMSM. ");
					
					// direct cost if system is upstream
					if(generateCost && !costCalculated) {
						if(sysName.equals(upstreamSysName)) {
							costCalculated = true;
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, upstreamSysName, "Provide", true, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, upstreamSysName, "Provide", true, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, upstreamSysName, "Provide", true, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMConsumerOfICD(icdURI, upstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMConsumer(newICD, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future cost triple
						addFutureDBCostRelTriples("", newICD, upstreamSystemURI, dataURI, data, rowIdx);
					}
				} else if(sorV.contains(upstreamSystemURI + dataURI) && !upstreamSysType.equals(HPI_KEY) && !upstreamSysType.equals(HPNI_KEY) && !interfaceingSysProbability.equals("null") && !interfaceingSysProbability.equals("") ) { // upstream system is SOR and has a probability
					otherwise = false;
					comment = comment.concat("Recommend review of developing interface between ").concat(upstreamSysName).concat("->DHMSM. ");
					
					// direct cost if system is upstream
					if(generateCost && !costCalculated) {
						costCalculated = true;
						if(sysName.equals(upstreamSysName)) {
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, upstreamSysName, "Provide", true, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, upstreamSysName, "Provide", true, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, upstreamSysName, "Provide", true, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMConsumerOfICD(icdURI, upstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMConsumer(newICD, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future cost triple
						addFutureDBCostRelTriples("", newICD, upstreamSystemURI, dataURI, data, rowIdx);
					}
				} 
				if(downstreamSysType.equals(LPI_KEY) && sorV.contains(downstreamSystemURI + dataURI)) { // downstream system is LPI and SOR of data
					otherwise = false;
					comment = comment.concat("Need to add interface ").concat(downstreamSysName).concat("->DHMSM. ");
					
					// direct cost if system is upstream
					if(generateCost && !costCalculated) {
						if(sysName.equals(downstreamSysName)) {
							costCalculated = true;
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, downstreamSysName, "Provide", true, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, downstreamSysName, "Provide", true, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, downstreamSysName, "Provide", true, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMConsumerOfICD(icdURI, downstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMConsumer(newICD, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future cost triple
						addFutureDBCostRelTriples("", newICD, downstreamSystemURI, dataURI, data, rowIdx);
					}
				} else if(sorV.contains(downstreamSystemURI + dataURI) && !downstreamSysType.equals(HPNI_KEY) && !downstreamSysType.equals(HPI_KEY) && !interfaceingSysProbability.equals("null") && !interfaceingSysProbability.equals("") ) { // downstream system is SOR and has a probability
					otherwise = false;
					comment = comment.concat("Recommend review of developing interface between ").concat(downstreamSysName).concat("->DHMSM. ");
					
					// direct cost if system is upstream
					if(generateCost && !costCalculated) {
						if(sysName.equals(downstreamSysName)) {
							costCalculated = true;
							directCost = true;
							if(usePhase) {
								finalCost = calculateCost(data, downstreamSysName, "Provide", true, servicesProvideList, rowIdx);
							} else {
								finalCost = calculateCost(data, downstreamSysName, "Provide", true, servicesProvideList);
							}
						}
					}
					
					if(generateNewTriples) {
						finalCost = calculateCost(data, downstreamSysName, "Provide", true, new HashSet(), rowIdx);
						// future db triples - new interface
						newICD = makeDHMSMConsumerOfICD(icdURI, downstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/")+1)).concat(":").concat(data);
						addedInterfaces.add(newICD);
						addTripleWithDHMSMConsumer(newICD, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// future cost triple
						addFutureDBCostRelTriples("", newICD, downstreamSystemURI, dataURI, data, rowIdx);
					}
				} 
				if(otherwise) {
					noCost = true;
					if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
						comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
						if(generateNewTriples) {
							// future db triples - removed interface
							removedInterfaces.add(icdURI);
							String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/")+1)).concat(":").concat(data);
							addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
							addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
						}
					} else {
						comment = "Stay as-is beyond FOC.";
					}
				}
			} else { // other cases DHMSM doesn't touch data object
				noCost = true;
				if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
					comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
					if(generateNewTriples) { 
						// future db triples - removed interface
						removedInterfaces.add(icdURI);
						String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/")+1)).concat(":").concat(data);
						addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
						addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
					}
				} else {
					comment = "Stay as-is beyond FOC.";
				}
			}
			if(getComments && !generateCost) {
				values[10] = comment;
			}
			if(generateCost) {
				if(noCost && !generateNewTriples) {
					values[11] = comment;
					values[12] = "";
					values[13] = "";
				} else {
					if(finalCost == null) {
						if(usePhase) {
							sysCostInfo.remove(rowIdx);
						}
						if(deleteOtherInterfaces && !skipFistIteration) {
							values[11] = "Interface already taken into consideration.";
							skipFistIteration = false;
						} else {
							values[11] = "Cost already taken into consideration.";
						}
						values[12] = "";
						values[13] = "";
					} else if(finalCost != (double) 0){
						if(directCost) {
							values[11] = comment;
							values[12] = finalCost;
							values[13] = "";
							totalDirectCost += finalCost;
						} else {
							values[11] = comment;
							values[12] = "";
							values[13] = finalCost;
							totalIndirectCost += finalCost;
						}
					} else {
						if(usePhase) {
							sysCostInfo.remove(rowIdx);
						}
						values[11] = "No data present to calculate loe.";
						values[12] = "";
						values[13] = "";
					}
				}
			}
			retList.add(values);
			// update rowIdx
			rowIdx++;
		}
		return retList;
	}

	private void addTripleWithDHMSMProvider(String icdURI, String downstreamSysURI, String downstreamSysName, String dataURI, String data, String payloadURI) {
		// change DHMSM to type System
		String upstreamSysURI = DHMSM_URI;
		String upstreamSysName = DHMSM;
		
		// dhmsm -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		
		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":").concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		
		// icd -> payload -> data 
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
		
		// dhmsm -> provide -> data
		values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideInstanceRel.concat(upstreamSysName).concat(":").concat(data);
		values[2] = dataURI;
		relList.add(values);
	}
	
	private void addTripleWithDHMSMConsumer(String icdURI, String upstreamSysURI, String upstreamSysName, String dataURI, String data, String payloadURI) {
		// change DHMSM to type System
		String downstreamSysURI = DHMSM_URI;
		String downstreamSysName = DHMSM;
		
		// upstream -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		
		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":").concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		
		// icd -> payload -> data 
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
	}
	
	private void addTriples(String icdURI, String upstreamSysURI, String upstreamSysName, String downstreamSysURI, String downstreamSysName, String dataURI, String data, String payloadURI) {
		sysList.add(upstreamSysURI);
		sysList.add(downstreamSysURI);
		
		// upstream -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		
		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":").concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		
		// icd -> payload -> data 
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
	}

	private void addPropTriples(String payloadURI, String format, String freq, String prot, String comment, double weight) {
		// payload -> contains -> prop
		Object[] values = new Object[]{payloadURI, semossPropURI.concat("Format"), format};
		relPropList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Frequency"), freq};
		relPropList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Protocol"), prot};
		relPropList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Recommendation"), comment};
		relPropList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat(newProp), weight};
		relPropList.add(values);
	}

	private void addFutureDBCostRelTriples(String decommissionedICD, String proposedICD, String sysURI, String dataURI, String data, int index) {
		HashMap<String, Double> info = sysCostInfo.get(index);
		String previousGLItemURI = "";
		String previousGLItemName = "";
		
		// if no cost information, I make an educated guess
		if(info == null) {
			info = new HashMap<String, Double>();
			if(sysURI.equals(DHMSM)){
				info.put("Provider+Requirements", 100.0);
				info.put("Provider+Design", 100.0);
				info.put("Provider+Develop", 100.0);
				info.put("Provider+Test", 100.0);
				info.put("Provider+Deploy", 100.0);
			} else {
				info.put("Consumer+Requirements", 70.0);
				info.put("Consumer+Design", 70.0);
				info.put("Consumer+Develop", 70.0);
				info.put("Consumer+Test", 70.0);
			}
		}
		
		String[] orderedResults = orderResults(info.keySet());
		Object[] values = new Object[3];
		
		String sys = Utility.getInstanceName(sysURI);
		for(String tagAndPhase : orderedResults) {
			String[] split = tagAndPhase.split("\\+");
			String glTag = split[0];
			String sdlcPhase = split[1];
			String glTagURI = GLTAG_URI.concat(glTag);
			String sdlcPhaseURI = SDLC_PHASE_URI.concat(sdlcPhase);
			
			String input = "None";
			if(!decommissionedICD.equals("")) {
				input = Utility.getInstanceName(decommissionedICD);
			}
			String output = Utility.getInstanceName(proposedICD);
			String glItemName = data.concat("%").concat(output).concat("%").concat(sys).concat("%").concat(glTag).concat("%").concat(sdlcPhase);
			String glItemURI = "http://health.mil/ontologies/Concept/".concat(sdlcPhase).concat("GLItem/").concat(glItemName);
			// this relationship may not always exist
			if(!decommissionedICD.equals("")) {
				// removedICD -> input -> glItem
				values = new Object[3];
				values[0] = decommissionedICD;
				values[1] = inputInstanceRel.concat(input).concat(":").concat(glItemName);
				values[2] = glItemURI;
				costRelList.add(values);
			}
			// glItem -> output -> proposedICD
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = outputInstanceRel.concat(glItemName).concat(":").concat(output);
			values[2] = proposedICD;
			costRelList.add(values);
			// glItem -> taggedBy -> gltag
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = taggedByInstanceRel.concat(glItemName).concat(":").concat(glTag);
			values[2] = glTagURI;
			costRelList.add(values);
			// glItem -> belongsTo -> sdlc
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = belongsToInstanceRel.concat(glItemName).concat(":").concat(sdlcPhase);
			values[2] = sdlcPhaseURI;
			costRelList.add(values);
			// system -> influences -> glitem
			values = new Object[3];
			values[0] = sysURI;
			values[1] = influencesInstanceRel.concat(sys).concat(":").concat(glItemName);
			values[2] = glItemURI;
			costRelList.add(values);
			// data input gl items
			values = new Object[3];
			values[0] = dataURI;
			values[1] = inputInstanceRel.concat(data).concat(":").concat(glItemName);
			values[2] = glItemURI;
			costRelList.add(values);
			// ordering of gl items
			if(previousGLItemURI.equals("")) {
				previousGLItemURI = glItemURI;
				previousGLItemName = glItemName;
			} else {
				// glItem -> precedes -> glitem
				values = new Object[3];
				values[0] = previousGLItemURI;
				values[1] = precedesInstanceRel.concat(previousGLItemName).concat(":").concat(glItemName);
				values[2] = glItemURI;
				costRelList.add(values);
				previousGLItemURI = glItemURI;
			}
			// glItem -> contains -> loe
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = semossPropURI.concat("LOEcalc");
			values[2] = info.get(tagAndPhase);
			loeList.add(values);
			
			//keep track of all glitems
			glItemList.add(glItemURI);
			//keep track of all systems
			sysCostList.add(sysURI);
		}
	}

	private String[] orderResults(Set<String> keySet) {
		String[] retResults = new String[5];
		for(String s : keySet) {
			if(s.contains("Requirements")) {
				retResults[0] = s;
			} else if(s.contains("Design")) {
				retResults[1] = s;
			} else if(s.contains("Develop")) {
				retResults[2] = s;
			} else if(s.contains("Test")) {
				retResults[3] = s;
			}  else if(s.contains("Deploy")) {
				retResults[4] = s;
			}
		}
		
		return (String[]) ArrayUtilityMethods.removeAllNulls(retResults);
	}

	private String makeDHMSMConsumerOfICD(final String icd, final String sysProvider, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/')+1);
		return base.concat(sysProvider).concat("-").concat(DHMSM).concat("-").concat(dataObject);
	}

	private String makeDHMSMProviderOfICD(final String icd, final String sysConsumer, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/')+1);
		return base.concat(DHMSM).concat("-").concat(sysConsumer).concat("-").concat(dataObject);
	}
	
	private Double calculateCost(String dataObject, String system, String tag, boolean includeGenericCost, HashSet<String> servicesProvideList)
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
			finalCost = (double) (Math.round(sysGLItemCost + genericCost) * COST_PER_HOUR);
		}

		return finalCost;
	}
	
	public Double calculateCost(String dataObject, String system, String tag, boolean includeGenericCost, HashSet<String> servicesProvideList, int rowIdx)
	{
		double sysGLItemCost = 0;
		double genericCost = 0;

		ArrayList<String> sysGLItemServices = new ArrayList<String>();
		// get sysGlItem for provider lpi systems
		HashMap<String, HashMap<String, Double>> sysGLItem = loeForSysGlItemAndPhaseHash.get(dataObject);
		HashMap<String, HashMap<String, Double>> avgSysGLItem = avgLoeForSysGLItemAndPhaseHash.get(dataObject);

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
							HashMap<String, Double> phaseHash = sysGLItem.get(sysSerGLTag);
							for(String phase : phaseHash.keySet()) {
								double loe = phaseHash.get(phase);
								addToSysCostHash(tag, phase, loe, rowIdx);
								sysGLItemCost += loe;
							}
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
							HashMap<String, Double> phaseHash = avgSysGLItem.get(serGLTag);
							for(String phase : phaseHash.keySet()) {
								double loe = phaseHash.get(phase);
								addToSysCostHash(tag, phase, loe, rowIdx);
								sysGLItemCost += loe;
							}
						} else {
							servicesAllUsed = true;
						}
					}
				}
			}
		}

		if(includeGenericCost)
		{
			HashMap<String, HashMap<String, Double>> genericGLItem = genericLoeForSysGLItemAndPhaseHash.get(dataObject);
			if(genericGLItem != null)
			{
				for(String ser : genericGLItem.keySet())
				{
					if(sysGLItemServices.contains(ser)) {
						HashMap<String, Double> phaseHash = genericGLItem.get(ser);
						for(String phase : phaseHash.keySet()) {
							double loe = phaseHash.get(phase);
							addToSysCostHash(tag, phase, loe, rowIdx);
							genericCost += loe;
						}
					} 
				}
			}
		}

		Double finalCost = null;
		if(!servicesAllUsed) {
			finalCost = (sysGLItemCost + genericCost) * COST_PER_HOUR;
		}

		return finalCost;
	}
	
	
	private void addToSysCostHash(String tag, String phase, double loe, int rowIdx) {
		String key = tag.concat("+").concat(phase);
		HashMap<String, Double> innerHash = new HashMap<String, Double>();
		if(sysCostInfo.containsKey(rowIdx)) {
			innerHash = sysCostInfo.get(rowIdx);
			if(innerHash.containsKey(key)){
				double newLoe = innerHash.get(key) + loe;
				innerHash.put(key, newLoe);
			} else {
				innerHash.put(key, loe);
			}
		} else {
			innerHash.put(key, loe);
			sysCostInfo.put(rowIdx, innerHash);
		}
	}
	
	public void consolodateCostHash() {
		for(Integer val : sysCostInfo.keySet()) {
			HashMap<String, Double> innerHash = sysCostInfo.get(val);
			for(String key: innerHash.keySet()) {
				double loe = innerHash.get(key);
				if(consolidatedSysCostInfo.containsKey(key)) {
					loe += consolidatedSysCostInfo.get(key);
					consolidatedSysCostInfo.put(key, loe);
				} else {
					consolidatedSysCostInfo.put(key, loe);
				}
			}
		}
	}
	
	public void getCostInfo(final IEngine TAP_Cost_Data){
		// get data for all systems
		if(loeForGenericGlItemHash.isEmpty()) {
			loeForGenericGlItemHash = DHMSMTransitionUtility.getGenericGLItem(TAP_Cost_Data);
		}
		if(avgLoeForSysGlItemHash.isEmpty()) {
			avgLoeForSysGlItemHash = DHMSMTransitionUtility.getAvgSysGLItem(TAP_Cost_Data);
		} 
		if(serviceToDataHash.isEmpty()) {
			serviceToDataHash = DHMSMTransitionUtility.getServiceToData(TAP_Cost_Data);
		}
		if(loeForSysGlItemHash.isEmpty()) {
			loeForSysGlItemHash = DHMSMTransitionUtility.getSysGLItem(TAP_Cost_Data);
		}
	}
	
	public void getCostInfoAtPhaseLevel(final IEngine TAP_Cost_Data){
		if(loeForSysGlItemAndPhaseHash.isEmpty()) {
			loeForSysGlItemAndPhaseHash = DHMSMTransitionUtility.getSysGLItemAndPhase(TAP_Cost_Data);
		}
		if(genericLoeForSysGLItemAndPhaseHash.isEmpty()) {
			genericLoeForSysGLItemAndPhaseHash = DHMSMTransitionUtility.getGenericGLItemAndPhase(TAP_Cost_Data);
		}
		if(avgLoeForSysGLItemAndPhaseHash.isEmpty()) {
			avgLoeForSysGLItemAndPhaseHash = DHMSMTransitionUtility.getAvgSysGLItemAndPhase(TAP_Cost_Data);
		}
		if(serviceToDataHash.isEmpty()) {
			serviceToDataHash = DHMSMTransitionUtility.getServiceToData(TAP_Cost_Data);
		}
	}
	
	public void getLPNIInfo(final IEngine HR_Core) {
		if(dhmsmSORList == null) {
			dhmsmSORList = DHMSMTransitionUtility.runVarListQuery(HR_Core, DHMSMTransitionUtility.DHMSM_SOR_QUERY);
		}
		if(lpiSystemList == null) {
			lpiSystemList = DHMSMTransitionUtility.runVarListQuery(HR_Core, DHMSMTransitionUtility.LPI_SYS_QUERY);
		}
	}
	
}
