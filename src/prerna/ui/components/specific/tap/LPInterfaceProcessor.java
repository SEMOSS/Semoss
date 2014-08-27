package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DHMSMTransitionUtility;

public class LPInterfaceProcessor {

	private String query = "SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?LPSystem) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType)  (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem) (COALESCE(IF(BOUND(?y),IF((?UpstreamSysProb1 != 'High' && ?UpstreamSysProb1 != 'Question'),'Low','High'),IF(BOUND(?x),IF((?DownstreamSysProb1!='High' && ?DownstreamSysProb1!='Question'),'Low','High'),'')), '') AS ?Probability) ?Interface ?Data ?Format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provides','Consumes')) AS ?DHMSM) ?Recommendation WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND('N' AS ?InterfaceYN) BIND('Y' AS ?InterfaceDHMSM) BIND('Y' AS ?ReceivedInformation) LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}  {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?InterfaceDHMSM;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;}OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Received_Information> ?ReceivedInformation;} FILTER(?Prob in ('Low','Medium','Medium-High')) {?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob;}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}  {?Interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> ?InterfaceDHMSM;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Received_Information> ?ReceivedInformation;}FILTER(?Prob in ('Low','Medium','Medium-High')) {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob;} {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}  {?Interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} } ORDER BY ?LPSystem ?InterfacingSystem";

	private final String LP_SYS_KEY = "LPSystem";
	private final String INTERFACE_TYPE_KEY = "InterfaceType";
	private final String INTERFACING_SYS_KEY = "InterfacingSystem";
	private final String PROBABILITY_KEY = "Probability";
	private final String ICD_KEY = "Interface";
	private final String DATA_KEY = "Data";	
	private final String FORMAT_KEY = "Format";
	private final String FREQ_KEY = "Freq";
	private final String PROT_KEY = "Prot";
	private final String DHMSM = "DHMSM";
	private final String COMMENT_KEY = "Recommendation";

	private final String DOWNSTREAM_KEY = "Downstream";
	private final String DHMSM_PROVIDE_KEY = "Provide";
	private final String DHMSM_CONSUME_KEY = "Consumes";
	private final String LPI_KEY = "LPI";
	//	private final String lpniKey = "LPNI"; 
	private final String HPI_KEY = "HPI";
	private final String HPNI_KEY = "HPNI";

	private final String DHMSM_URI = "http://health.mil/ontologies/Concept/DHMSM/DHMSM";

	private final String provideInstanceRel = "http://health.mil/ontologies/Relation/Provide/";
	private final String consumeInstanceRel = "http://health.mil/ontologies/Relation/Consume/";
	private final String payloadInstanceRel = "http://health.mil/ontologies/Relation/Payload/";

	private final String semossPropURI = "http://semoss.org/ontologies/Relation/Contains/";
	private final String newProp = "TypeWeight";

	private IEngine engine;
	private boolean generateComments;
	private boolean generateNewTriples;
	private String[] names;
	private ArrayList<Object[]> list;

	// for future DB generation
	private ArrayList<Object[]> relList;
	private ArrayList<Object[]> propList;
	private ArrayList<String> addedInterfaces;
	private ArrayList<String> removedInterfaces;


	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setEngine(IEngine engine){
		this.engine = engine;
	}

	public void setGenerateComments(boolean generateComments) {
		this.generateComments = generateComments;
	}

	public void setGenerateNewTriples(boolean generateNewTriples) {
		this.generateNewTriples = generateNewTriples;
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
		return propList;
	}
	
	public ArrayList<String> getAddedInterfaces(){
		return addedInterfaces;
	}
	
	public ArrayList<String> getRemovedInterfaces(){
		return removedInterfaces;
	}
	
	public ArrayList<Object[]> generateReport() {
		list = new ArrayList<Object[]>();

		HashSet<String> sysDataSOR = DHMSMTransitionUtility.processSysDataSOR(engine);
		HashMap<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);

		//Process main query
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
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
		propList = new ArrayList<Object[]>();
		addedInterfaces = new ArrayList<String>();
		removedInterfaces = new ArrayList<String>();
		
		// becomes true if either user 
		boolean getComments = false;
		while(sjw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjw.next();
			// get var's
			String sysName = sjss.getVar(LP_SYS_KEY).toString();
			String interfaceType = sjss.getVar(INTERFACE_TYPE_KEY).toString();
			String interfacingSysName = sjss.getVar(INTERFACING_SYS_KEY).toString(); 
			String probability = sjss.getVar(PROBABILITY_KEY).toString();
			String icd = sjss.getVar(ICD_KEY).toString();
			String data = sjss.getVar(DATA_KEY).toString();
			String format = sjss.getVar(FORMAT_KEY).toString();
			String freq = sjss.getVar(FREQ_KEY).toString();
			String prot = sjss.getVar(PROT_KEY).toString();
			String dhmsmSOR = sjss.getVar(DHMSM).toString();

			// get uri's
			String system = "";
			String interfaceTypeURI = "";
			String interfacingSystem = "";
			String probabilityURI = "";
			String icdURI = "";
			String dataURI = "";
			String formatURI = "";
			String freqURI = "";
			String protURI = "";
			String dhmsmSORURI = "";

			if(sjss.getRawVar(LP_SYS_KEY) != null) {
				system = sjss.getRawVar(LP_SYS_KEY).toString();
			}
			if(sjss.getVar(INTERFACE_TYPE_KEY) != null) {
				interfaceTypeURI = sjss.getVar(INTERFACE_TYPE_KEY).toString();
			}
			if(sjss.getRawVar(INTERFACING_SYS_KEY) != null) {
				interfacingSystem = sjss.getRawVar(INTERFACING_SYS_KEY).toString();
			}
			if(sjss.getRawVar(PROBABILITY_KEY) != null) {
				probabilityURI = sjss.getRawVar(PROBABILITY_KEY).toString();
			}
			if(sjss.getRawVar(ICD_KEY) != null) {
				icdURI = sjss.getRawVar(ICD_KEY).toString();
			}
			if(sjss.getRawVar(DATA_KEY) != null) {
				dataURI = sjss.getRawVar(DATA_KEY).toString();
			}
			if(sjss.getRawVar(FORMAT_KEY) != null) {
				formatURI = sjss.getRawVar(FORMAT_KEY).toString();
			}
			if(sjss.getRawVar(FREQ_KEY) != null) {
				freqURI = sjss.getRawVar(FREQ_KEY).toString();
			}
			if(sjss.getRawVar(PROT_KEY) != null) {
				protURI = sjss.getRawVar(PROT_KEY).toString();
			}
			if(sjss.getRawVar(DHMSM) != null) {
				dhmsmSORURI = sjss.getRawVar(DHMSM).toString();
			}

			String comment = "";
			Object[] values;
			if(generateComments || !generateNewTriples) {
				getComments = true;
				values = new Object[names.length];

				values[0] = sysName;
				values[1] = interfaceType;
				values[2] = interfacingSysName;
				values[3] = probability;
				values[4] = icd;
				values[5] = data;
				values[6] = format;
				values[7] = freq;
				values[8] = prot;
				values[9] = dhmsmSOR;
				values[10] = comment;
			} else {
				values = new Object[3];
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
			String newICD = "";
			String payloadURI = "";

			// DHMSM is SOR of data
			if(dhmsmSOR.contains(DHMSM_PROVIDE_KEY)) {

				// used by all statements if dhmsm is provider 
				if(generateNewTriples) {
					newICD = makeDHMSMProviderOfICD(icdURI, sysName, data);
					System.out.println(icdURI + "--------------" + newICD);
					payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/"))).concat(":").concat(data);
				}

				if(upstreamSysType.equals(LPI_KEY)) { // upstream system is LPI
					comment = comment.concat("Need to add interface DHMSM->").concat(upstreamSysName).concat(". ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, DHMSM_URI, DHMSM, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} 
				// new business rule might be added - will either un-comment or remove after discussion today
				//						else if (upstreamSysType.equals(lpniKey)) { // upstream system is LPNI
				//							comment += "Recommend review of developing interface DHMSM->" + upstreamSysName + ". ";
				//						} 
				else if (downstreamSysType.equals(LPI_KEY)) { // upstream system is not LPI and downstream system is LPI
					comment = comment.concat("Need to add interface DHMSM->").concat(downstreamSysName).concat(".").concat(" Recommend review of removing interface ")
							.concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(". ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, DHMSM_URI, DHMSM, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// add removed interface
						removedInterfaces.add(icdURI);
						addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(icdURI, format, freq, prot, comment, (double) 0);
					}
				} 
				if (upstreamSysType.equals(HPI_KEY)) { // upstream is HPI
					comment = comment.concat("Provide temporary integration between DHMSM->").concat(upstreamSysName).concat(" until all deployment sites for ").concat(upstreamSysName)
							.concat(" field DHMSM (and any additional legal requirements). ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, DHMSM_URI, DHMSM, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} else if(downstreamSysType.equals(HPI_KEY)) { // upstream sys is not HPI and downstream is HPI
					comment = comment.concat("Provide temporary integration between DHMSM->").concat(downstreamSysName).concat(" until all deployment sites for ").concat(downstreamSysName).concat(" field DHMSM (and any additional legal requirements).")
							.concat(" Recommend review of removing interface ").concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(". ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, DHMSM_URI, DHMSM, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
						// add removed interface
						removedInterfaces.add(icdURI);
						addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						addPropTriples(icdURI, format, freq, prot, comment, (double) 0);
					}
				} 
				if(!upstreamSysType.equals(LPI_KEY) && !upstreamSysType.equals(HPI_KEY) && !downstreamSysType.equals(LPI_KEY) && !downstreamSysType.equals(HPI_KEY))
				{
					if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
						comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
					} else {
						comment = "Stay as-is beyond FOC.";
					}
				}
			} else if(dhmsmSOR.contains(DHMSM_CONSUME_KEY)) {  // DHMSM is consumer of data

				// used by all statements if dhmsm is provider 
				if(generateNewTriples) {
					newICD = makeDHMSMConsumerOfICD(icdURI, sysName, data);
					payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/"))).concat(":").concat(data);
				}
				
				boolean otherwise = true;
				if(upstreamSysType.equals(LPI_KEY) && sorV.contains(upstreamSystemURI + dataURI)) { // upstream system is LPI and SOR of data
					otherwise = false;
					comment = comment.concat("Need to add interface ").concat(upstreamSysName).concat("->DHMSM. ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, upstreamSystemURI, upstreamSysName, DHMSM_URI, DHMSM, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} else if(sorV.contains(upstreamSystemURI + dataURI) && !probability.equals("null") && !probability.equals("") ) { // upstream system is SOR and has a probability
					otherwise = false;
					comment = comment.concat("Recommend review of developing interface between ").concat(upstreamSysName).concat("->DHMSM. ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, upstreamSystemURI, upstreamSysName, DHMSM_URI, DHMSM, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} 
				if(downstreamSysType.equals(LPI_KEY) && sorV.contains(downstreamSystemURI + dataURI)) { // downstream system is LPI and SOR of data
					otherwise = false;
					comment = comment.concat("Need to add interface ").concat(downstreamSysName).concat("->DHMSM. ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, downstreamSystemURI, downstreamSysName, DHMSM_URI, DHMSM, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} else if(sorV.contains(downstreamSystemURI + dataURI) && (!probability.equals("null") && !probability.equals("")) ) { // downstream system is SOR and has a probability
					otherwise = false;
					comment = comment.concat("Recommend review of developing interface between ").concat(downstreamSysName).concat("->DHMSM. ");
					if(generateNewTriples) {
						// add new interface
						addedInterfaces.add(newICD);
						addTriples(newICD, downstreamSystemURI, downstreamSysName, DHMSM_URI, DHMSM, dataURI, data, payloadURI);
						addPropTriples(payloadURI, format, freq, prot, comment, (double) 5);
					}
				} 
				if(otherwise) {
					if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
						comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
					} else {
						comment = "Stay as-is beyond FOC.";
					}
				}
			} else { // other cases DHMSM doesn't touch data object
				if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
					comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
				} else {
					comment = "Stay as-is beyond FOC.";
				}
			}
			if(getComments) {
				values[10] = comment;
				retList.add(values);
			}
		}
		return retList;
	}

	private void addTriples(String icdURI, String upstreamSysURI, String upstreamSysName, String downstreamSysURI, String downstreamSysName, String dataURI, String data, String payloadURI) {
		// upstream -> provide -> icd
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data);
		values[2] = icdURI;
		relList.add(values);
		// icd -> consume -> downstream
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":").concat(downstreamSysName);
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
		propList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Frequency"), freq};
		propList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Protocol"), prot};
		propList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat("Recommendation"), comment};
		propList.add(values);
		values = new Object[]{payloadURI, semossPropURI.concat(newProp), weight};
		propList.add(values);
	}


	private String makeDHMSMConsumerOfICD(final String icd, final String sysProvider, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/')+1);
		return base.concat(sysProvider).concat("-").concat(DHMSM).concat("-").concat(dataObject);
	}

	private String makeDHMSMProviderOfICD(final String icd, final String sysConsumer, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/')+1);
		return base.concat(DHMSM).concat("-").concat(sysConsumer).concat("-").concat(dataObject);
	}

}
