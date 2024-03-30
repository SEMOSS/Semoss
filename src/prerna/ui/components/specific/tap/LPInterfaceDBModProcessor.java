package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK;
import prerna.ui.components.specific.tap.FutureStateInterfaceResult.INTERFACE_TYPES;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class LPInterfaceDBModProcessor extends AbstractLPInterfaceProcessor{

	private FutureInterfaceCostProcessor processor;
	private IDatabaseEngine tapCost;
	private IDatabaseEngine futureDB;
	private IDatabaseEngine tapCore;
	
	private List<Object[]> relList = new ArrayList<Object[]>();
	private List<Object[]> relPropList = new ArrayList<Object[]>();
	private List<String> addedInterfaces = new ArrayList<String>();
	private List<String> removedInterfaces = new ArrayList<String>();
	private Set<String> sysList = new HashSet<String>();

	private List<Object[]> costRelList = new ArrayList<Object[]>();
	private List<Object[]> loeList = new ArrayList<Object[]>();
	private Set<String> glItemList = new HashSet<String>();
	private Set<String> sysCostList = new HashSet<String>();
	private Set<String> labelList = new HashSet<String>();
	private Set<String> labelCostList = new HashSet<String>();
	private Set<String> sysTrainingList = new HashSet<String>();
	
	private final String DHMSM_URI = "http://health.mil/ontologies/Concept/System/MHS_GENESIS";
	private final String GLTAG_URI = "http://health.mil/ontologies/Concept/GLTag/";
	private final String SDLC_PHASE_URI = "http://health.mil/ontologies/Concept/SDLCPhase/";
	private final String FORMAT_BASE_URI = "http://health.mil/ontologies/Concept/DForm/";
	private final String FREQUENCY_BASE_URI = "http://health.mil/ontologies/Concept/DFreq/";
	private final String PROTOCOL_BASE_URI = "http://health.mil/ontologies/Concept/DProt/";

	private final String provideInstanceRel = "http://health.mil/ontologies/Relation/Provide/";
	private final String consumeInstanceRel = "http://health.mil/ontologies/Relation/Consume/";
	private final String hasInstanceRel = "http://health.mil/ontologies/Relation/Has/";
	
	private final String payloadInstanceRel = "http://health.mil/ontologies/Relation/Payload/";
	private final String inputInstanceRel = "http://health.mil/ontologies/Relation/Input/";
	private final String outputInstanceRel = "http://health.mil/ontologies/Relation/Output/";
	private final String taggedByInstanceRel = "http://health.mil/ontologies/Relation/TaggedBy/";
	private final String belongsToInstanceRel = "http://health.mil/ontologies/Relation/BelongsTo/";
	private final String precedesInstanceRel = "http://health.mil/ontologies/Relation/Precedes/";
	private final String influencesInstanceRel = "http://health.mil/ontologies/Relation/Influences/";

	private final String semossPropURI = "http://semoss.org/ontologies/Relation/Contains/";
	private final String newProp = "TypeWeight";
	
	private Map<String, Map<String, String>> providerFutureIcdFrequency;
	private Map<String, Map<String, String>> consumerFutureIcdFrequency;

	//TODO: move engine definitions outside class to keep reusable
	public LPInterfaceDBModProcessor() throws IOException {
		super();
		tapCost = Utility.getDatabase(MasterDatabaseUtility.testDatabaseIdIfAlias("TAP_Cost_Data"));
		if(tapCost == null) {
			throw new IOException("TAP_Cost_Data not found.");
		}
		futureDB = Utility.getDatabase(MasterDatabaseUtility.testDatabaseIdIfAlias("FutureDB"));
		if(futureDB == null) {
			throw new IOException("FutureDB engine not found");
		}
		tapCore = Utility.getDatabase(MasterDatabaseUtility.testDatabaseIdIfAlias("TAP_Core_Data"));
		if(tapCore == null) {
			throw new IOException("TAP_Core_Data engine not found");
		}
		processor = new FutureInterfaceCostProcessor();
		processor.setCostEngines(new IDatabaseEngine[]{tapCost});
		processor.setCostFramework(COST_FRAMEWORK.P2P); // should define this via parameter
		processor.getCostInfo();
		
		sysList.add(DHMSM_URI);
		sysCostList.add(DHMSM_URI);
		labelList.add(DHMSM_URI);
		labelCostList.add(DHMSM_URI);
	}

	public void generateTriples() {
		Set<String> selfReportedSystems = new HashSet<String>();
		IDatabaseEngine futureDB = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("FutureDB");
		if(futureDB != null) {
			selfReportedSystems = DHMSMTransitionUtility.getAllSelfReportedSystemNames(futureDB);
		}
		Set<String> sorV = DHMSMTransitionUtility.processSysDataSOR(engine);
		Map<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);

		// Process main query
		ISelectWrapper wrapper1 = WrapperManager.getInstance().getSWrapper(engine, upstreamQuery);
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper(engine, downstreamQuery);

		ISelectWrapper[] wrappers = new ISelectWrapper[]{wrapper1, wrapper2};
		String[] headers = wrapper1.getVariables();
		Set<String> consumeSet = new HashSet<String>();
		Set<String> provideSet = new HashSet<String>();

		for(ISelectWrapper wrapper : wrappers) {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				// get var's
				String sysName = "";
				String interfaceType = "";
				String interfacingSysName = "";
				String icd = "";
				String data = "";
				String format = "";
				String freq = "";
				String prot = "";
				String dhmsmSOR = "";
				
				String sysProbability = sysTypeHash.get(sysName);
				if (sysProbability == null || sysProbability.equals("TBD")) {
					sysProbability = "No Probability";
				}
				if (sjss.getVar(SYS_KEY) != null) {
					sysName = sjss.getVar(SYS_KEY).toString();
				}
				if (sjss.getVar(INTERFACE_TYPE_KEY) != null) {
					interfaceType = sjss.getVar(INTERFACE_TYPE_KEY).toString();
				}
				if (sjss.getVar(INTERFACING_SYS_KEY) != null) {
					interfacingSysName = sjss.getVar(INTERFACING_SYS_KEY).toString();
				}
				if (sjss.getVar(ICD_KEY) != null) {
					icd = sjss.getVar(ICD_KEY).toString();
				}
				if (sjss.getVar(DATA_KEY) != null) {
					data = sjss.getVar(DATA_KEY).toString();
				}
				if (sjss.getVar(FORMAT_KEY) != null) {
					format = sjss.getVar(FORMAT_KEY).toString();
				}
				if (sjss.getVar(FREQ_KEY) != null) {
					freq = sjss.getVar(FREQ_KEY).toString();
				}
				if (sjss.getVar(PROT_KEY) != null) {
					prot = sjss.getVar(PROT_KEY).toString();
				}
				if (sjss.getVar(DHMSM) != null) {
					dhmsmSOR = sjss.getVar(DHMSM).toString();
				}

				// get uri's
				String systemURI = "";
				String interfacingSystemURI = "";
				String icdURI = "";
				String dataURI = "";
				
				if (sjss.getRawVar(SYS_KEY) != null) {
					systemURI = sjss.getRawVar(SYS_KEY).toString();
				}
				if (sjss.getRawVar(INTERFACING_SYS_KEY) != null) {
					interfacingSystemURI = sjss.getRawVar(INTERFACING_SYS_KEY).toString();
				}
				if (sjss.getRawVar(ICD_KEY) != null) {
					icdURI = sjss.getRawVar(ICD_KEY).toString();
				}
				if (sjss.getRawVar(DATA_KEY) != null) {
					dataURI = sjss.getRawVar(DATA_KEY).toString();
				}
				
				FutureStateInterfaceResult result = FutureStateInterfaceProcessor.processICD(
						sysName, 
						interfaceType, 
						interfacingSysName, 
						icd, 
						data, 
						dhmsmSOR, 
						selfReportedSystems, 
						sorV, 
						sysTypeHash,
						consumeSet,
						provideSet);

				// grab result values
				INTERFACE_TYPES recommendation = (INTERFACE_TYPES) result.get(FutureStateInterfaceResult.RECOMMENDATION);
				String upstreamSysName = (String) result.get(FutureStateInterfaceResult.LEGACY_UPSTREAM_SYSTEM);
				String downstreamSysName = (String) result.get(FutureStateInterfaceResult.LEGACY_DOWNSTREAM_SYSTEM);
				String upstreamSystemURI = "";
				String downstreamSystemURI = "";
				if(sysName.equals(upstreamSysName)) {
					upstreamSystemURI = systemURI;
					downstreamSystemURI = interfacingSystemURI;
				} else {
					upstreamSystemURI = interfacingSystemURI;
					downstreamSystemURI = systemURI;
				}
				String comment = (String) result.get(FutureStateInterfaceResult.COMMENT);
				Boolean costTakenIntoConsideration = result.isCostTakenIntoConsideration();
				
				//If cost taken into consideration, icd is already added
				if(!costTakenIntoConsideration) {
					// calculate cost
					Map<String, Double> costResults = null;
					String tag = "";
					if(recommendation != INTERFACE_TYPES.DECOMMISSIONED && recommendation != INTERFACE_TYPES.STAY_AS_IS) {
						if(recommendation == INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM ||
								recommendation == INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION ||
								recommendation == INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM ||
								recommendation == INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION ) 
						{
							tag = "Consumer";
						} else {
							tag = "Provider";
						}
						if(!result.isCostTakenIntoConsideration()) {
							costResults = processor.calculateCost(data, sysName, tag);
						}
					}
	
					// generate appropriate triples based on recommendation
					String newICD = "";
					String payloadURI = "";
					
					// triples associated with generating new interfaces
					if(recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM ||
							recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION) 
					{
						newICD = makeDHMSMProviderOfICD(icdURI, downstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/") + 1)).concat(":").concat(data);
						addTripleWithDHMSMProvider(newICD, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						
						// for new interfaces, the props and nodes for format, protocol, and frequency are the same
						// and should match standardized nomenclature
						String[] propVals = getIcdPropValsConsumer(downstreamSysName, data);
						addPropTriples(payloadURI, propVals, comment, (double) 5);
						addIcdPropRels(newICD, propVals);
						
						addFutureDBCostRelTriples(icdURI, newICD, DHMSM_URI, dataURI, data, "Consumer", costResults);
						sysTrainingList.add(downstreamSystemURI);
						
					} else if(recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM || 
							recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION) 
					{
						newICD = makeDHMSMConsumerOfICD(icdURI, downstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/") + 1)).concat(":").concat(data);
						addTripleWithDHMSMConsumer(newICD, downstreamSystemURI, downstreamSysName, dataURI, data, payloadURI);
						
						// for new interfaces, the props and nodes for format, protocol, and frequency are the same
						// and should match standardized nomenclature
						String[] propVals = getIcdPropValsProvider(downstreamSysName, data);
						addPropTriples(payloadURI, propVals, comment, (double) 5);
						addIcdPropRels(newICD, propVals);
						
						addFutureDBCostRelTriples("", newICD, downstreamSystemURI, dataURI, data, "Provider", costResults);
						sysTrainingList.add(downstreamSystemURI);
	
					} else if(recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM || 
							recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION) 
					{
						newICD = makeDHMSMProviderOfICD(icdURI, upstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/") + 1)).concat(":").concat(data);
						addTripleWithDHMSMProvider(newICD, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						
						// for new interfaces, the props and nodes for format, protocol, and frequency are the same
						// and should match standardized nomenclature
						String[] propVals = getIcdPropValsConsumer(upstreamSysName, data);
						addPropTriples(payloadURI, propVals, comment, (double) 5);
						addIcdPropRels(newICD, propVals);
						
						addFutureDBCostRelTriples(icdURI, newICD, DHMSM_URI, dataURI, data, "Consumer", costResults);
						sysTrainingList.add(upstreamSystemURI);

					} else if(recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM || 
							recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION) 
					{
						newICD = makeDHMSMConsumerOfICD(icdURI, upstreamSysName, data);
						payloadURI = payloadInstanceRel.concat(newICD.substring(newICD.lastIndexOf("/") + 1)).concat(":").concat(data);
						addTripleWithDHMSMConsumer(newICD, upstreamSystemURI, upstreamSysName, dataURI, data, payloadURI);
						
						// for new interfaces, the props and nodes for format, protocol, and frequency are the same
						// and should match standardized nomenclature
						String[] propVals = getIcdPropValsProvider(upstreamSysName, data);
						addPropTriples(payloadURI, propVals, comment, (double) 5);
						addIcdPropRels(newICD, propVals);

						addFutureDBCostRelTriples("", newICD, upstreamSystemURI, dataURI, data, "Provider", costResults);
						sysTrainingList.add(upstreamSystemURI);
					} 				
					if(!newICD.isEmpty()) {
						addedInterfaces.add(newICD);
					}
				}
				
				// triples associated with removing old interfaces
				if(recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION || 
						recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION ||
						recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION ||
						recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION ||
						recommendation == FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED) 
				{
					removedInterfaces.add(icdURI);
					String oldPayload = payloadInstanceRel.concat(icdURI.substring(icdURI.lastIndexOf("/") + 1)).concat(":").concat(data);
					addTriples(icdURI, upstreamSystemURI, upstreamSysName, downstreamSystemURI, downstreamSysName, dataURI, data, oldPayload);
					// this will add the information from the props
					addPropTriples(oldPayload, format, freq, prot, comment, (double) 0);
					// this will add the information from the nodes
					addExistingIcdRels(icdURI);
				}
			}
		}
	}

	/*
	 * Add the existing format, protocol, and frequency for the interfaces
	 * that are going to be decommissioned
	 */
	private void addExistingIcdRels(String icdURI) {
		String icdName = Utility.getInstanceName(icdURI);
		
		String[] returnUris = DHMSMTransitionUtility.getExistingIcdRelUris(tapCore, icdURI);
		
		// icd -> has -> format
		String formatUri = returnUris[0];
		if(formatUri != null) {
			Object[] values = new Object[3];
			String hasURI = hasInstanceRel.concat(icdName).concat(":").concat(Utility.getInstanceName(formatUri));
			values[0] = icdURI;
			values[1] = hasURI;
			values[2] = formatUri;
			relList.add(values);
			addToLabelList(labelList, values);
		}

		// icd -> has -> protocol
		String protocolUri = returnUris[1];
		if(protocolUri != null) {
			Object[] values = new Object[3];
			String hasURI = hasInstanceRel.concat(icdName).concat(":").concat(Utility.getInstanceName(protocolUri));
			values[0] = icdURI;
			values[1] = hasURI;
			values[2] = protocolUri;
			relList.add(values);
			addToLabelList(labelList, values);
		}
		
		// icd -> has -> frequency
		String frequencyUri = returnUris[2];
		if(frequencyUri != null) {
			Object[] values = new Object[3];
			String hasURI = hasInstanceRel.concat(icdName).concat(":").concat(Utility.getInstanceName(frequencyUri));
			values[0] = icdURI;
			values[1] = hasURI;
			values[2] = frequencyUri;
			relList.add(values);
			addToLabelList(labelList, values);
		}
	}

	/*
	 * Add DForm, DProt, and DFreq relationships for the new icd
	 */
	private void addIcdPropRels(String newICD, String[] propVals) {
		Object[] values = new Object[3];
		String icdName = Utility.getInstanceName(newICD);
		
		// icd -> has -> format
		String format = propVals[0];
		String hasURI = hasInstanceRel.concat(icdName).concat(":").concat(format);
		values[0] = newICD;
		values[1] = hasURI;
		values[2] = FORMAT_BASE_URI.concat(format);
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> has -> protocol
		String protocol = propVals[1];
		hasURI = hasInstanceRel.concat(icdName).concat(":").concat(protocol);
		values = new Object[3];
		values[0] = newICD;
		values[1] = hasURI;
		values[2] = PROTOCOL_BASE_URI.concat(protocol);
		relList.add(values);
		addToLabelList(labelList, values);
		
		// icd -> has -> frequency
		String frequency = propVals[2];
		hasURI = hasInstanceRel.concat(icdName).concat(":").concat(frequency);
		values = new Object[3];
		values[0] = newICD;
		values[1] = hasURI;
		values[2] = FREQUENCY_BASE_URI.concat(frequency);
		relList.add(values);
		addToLabelList(labelList, values);
	}

	/*
	 * Get the estimate values for the consumer
	 * Always assume TCPIP/HL7 -> but we determine the frequency based on the 
	 * fastest frequency we have for this system-data object combination
	 * else, we say it is TBD
	 * 
	 * return is [format, protocol, frequency]
	 */
	private String[] getIcdPropValsConsumer(String system, String data) {
		String[] values = new String[3];
		// index 0 is format HL7
		values[0] = "HL7";
		// index 1 is protocol TCPIP
		values[1] = "TCPIP";
		// try to find the frequency
		// default is TBD
		String frequency = "TBD";
		if(consumerFutureIcdFrequency.containsKey(system)) {
			Map<String, String> dataMap = consumerFutureIcdFrequency.get(system);
			if(dataMap != null && dataMap.containsKey(data)) {
				frequency = dataMap.get(data);
			}
		}
		// if we dont have any consumer icds, check the provider ones
		else if(providerFutureIcdFrequency.containsKey(system)) {
			Map<String, String> dataMap = consumerFutureIcdFrequency.get(system);
			if(dataMap != null && dataMap.containsKey(data)) {
				frequency = dataMap.get(data);
			}
		}
		values[2] = frequency;
		return values;
	}

	/*
	 * Get the estimate values for the provider icd
	 * Always assume TCPIP/HL7 -> but we determine the frequency based on the 
	 * fastest frequency we have for this system-data object combination
	 * else, we say it is TBD
	 * 
 	 * return is [format, protocol, frequency]
	 */
	private String[] getIcdPropValsProvider(String system, String data) {
		String[] values = new String[3];
		// index 0 is format HL7
		values[0] = "HL7";
		// index 1 is protocol TCPIP
		values[1] = "TCPIP";
		// try to find the frequency
		// default is TBD
		String frequency = "TBD";
		if(providerFutureIcdFrequency.containsKey(system)) {
			Map<String, String> dataMap = providerFutureIcdFrequency.get(system);
			if(dataMap.containsKey(data)) {
				frequency = dataMap.get(data);
			}
		}
		// if we dont have any provider icds, check the consumer ones
		else if(consumerFutureIcdFrequency.containsKey(system)) {
			Map<String, String> dataMap = consumerFutureIcdFrequency.get(system);
			if(dataMap.containsKey(data)) {
				frequency = dataMap.get(data);
			}
		}
		values[2] = frequency;
		return values;
	}
	
	private void addTripleWithDHMSMProvider(String icdURI, String downstreamSysURI, String downstreamSysName, String dataURI, String data,
			String payloadURI) {
		// change DHMSM to type System
		String upstreamSysURI = DHMSM_URI;
		String upstreamSysName = DHMSM;
		sysList.add(downstreamSysURI);

		// dhmsm -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName)
				.concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":")
				.concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> payload -> data
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// dhmsm -> provide -> data
		values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideInstanceRel.concat(upstreamSysName).concat(":").concat(data);
		values[2] = dataURI;
		relList.add(values);
		addToLabelList(labelList, values);
	}

	private void addTripleWithDHMSMConsumer(String icdURI, String upstreamSysURI, String upstreamSysName, String dataURI, String data,
			String payloadURI) {
		// change DHMSM to type System
		String downstreamSysURI = DHMSM_URI;
		String downstreamSysName = DHMSM;
		sysList.add(upstreamSysURI);

		// upstream -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName)
				.concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":")
				.concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> payload -> data
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
		addToLabelList(labelList, values);
	}

	private void addTriples(String icdURI, String upstreamSysURI, String upstreamSysName, String downstreamSysURI, String downstreamSysName,
			String dataURI, String data, String payloadURI) {
		sysList.add(upstreamSysURI);
		sysList.add(downstreamSysURI);

		// upstream -> provide -> icd
		String provideURI = provideInstanceRel.concat(upstreamSysName).concat(":").concat(upstreamSysName).concat("-").concat(downstreamSysName)
				.concat("-").concat(data);
		Object[] values = new Object[3];
		values[0] = upstreamSysURI;
		values[1] = provideURI;
		values[2] = icdURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> consume -> downstream
		String consumeURI = consumeInstanceRel.concat(upstreamSysName).concat("-").concat(downstreamSysName).concat("-").concat(data).concat(":")
				.concat(downstreamSysName);
		values = new Object[3];
		values[0] = icdURI;
		values[1] = consumeURI;
		values[2] = downstreamSysURI;
		relList.add(values);
		addToLabelList(labelList, values);

		// icd -> payload -> data
		values = new Object[3];
		values[0] = icdURI;
		values[1] = payloadURI;
		values[2] = dataURI;
		relList.add(values);
		addToLabelList(labelList, values);
	}

	private void addPropTriples(String payloadURI, String format, String freq, String prot, String comment, double weight) {
		// payload -> contains -> prop
		Object[] values = new Object[] { payloadURI, semossPropURI.concat("Format"), format };
		relPropList.add(values);
		values = new Object[] { payloadURI, semossPropURI.concat("Frequency"), freq };
		relPropList.add(values);
		values = new Object[] { payloadURI, semossPropURI.concat("Protocol"), prot };
		relPropList.add(values);
		values = new Object[] { payloadURI, semossPropURI.concat("Recommendation"), comment };
		relPropList.add(values);
		values = new Object[] { payloadURI, semossPropURI.concat(newProp), weight };
		relPropList.add(values);
	}

	private void addPropTriples(String payloadURI, String[] propVals, String comment, double weight) {
		Object[] values = null;
		// payload -> contains -> prop
		if(propVals[0] != null) {
			values = new Object[] { payloadURI, semossPropURI.concat("Format"), propVals[0] };
			relPropList.add(values);
		}
		if(propVals[1] != null) {
			values = new Object[] { payloadURI, semossPropURI.concat("Protocol"), propVals[1] };
			relPropList.add(values);
		}
		if(propVals[2] != null) {
			values = new Object[] { payloadURI, semossPropURI.concat("Frequency"), propVals[2] };
			relPropList.add(values);
		}
		values = new Object[] { payloadURI, semossPropURI.concat("Recommendation"), comment };
		relPropList.add(values);
		values = new Object[] { payloadURI, semossPropURI.concat(newProp), weight };
		relPropList.add(values);
	}

	private void addFutureDBCostRelTriples(String decommissionedICD, String proposedICD, String sysURI, String dataURI, String data, String glTag, Map<String, Double> costInfo) {
		String previousGLItemURI = "";
		String previousGLItemName = "";

		String[] orderedResults = orderResults(costInfo.keySet());
		Object[] values = new Object[3];

		String sys = Utility.getInstanceName(sysURI);
		for (String phase : orderedResults) {
			String glTagURI = GLTAG_URI.concat(glTag);
			String sdlcPhaseURI = SDLC_PHASE_URI.concat(phase);

			String input = "None";
			if (!decommissionedICD.equals("")) {
				input = Utility.getInstanceName(decommissionedICD);
			}
			String output = Utility.getInstanceName(proposedICD);
			String glItemName = data.concat("%").concat(output).concat("%").concat(sys).concat("%").concat(glTag).concat("%").concat(phase);
			String glItemURI = "http://health.mil/ontologies/Concept/".concat(phase).concat("GLItem/").concat(glItemName);
			// this relationship may not always exist
			if (!decommissionedICD.equals("")) {
				// removedICD -> input -> glItem
				values = new Object[3];
				values[0] = decommissionedICD;
				values[1] = inputInstanceRel.concat(input).concat(":").concat(glItemName);
				values[2] = glItemURI;
				costRelList.add(values);
				addToLabelList(labelCostList, values);
			}
			// glItem -> output -> proposedICD
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = outputInstanceRel.concat(glItemName).concat(":").concat(output);
			values[2] = proposedICD;
			costRelList.add(values);
			addToLabelList(labelCostList, values);

			// glItem -> taggedBy -> gltag
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = taggedByInstanceRel.concat(glItemName).concat(":").concat(glTag);
			values[2] = glTagURI;
			costRelList.add(values);
			addToLabelList(labelCostList, values);

			// glItem -> belongsTo -> sdlc
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = belongsToInstanceRel.concat(glItemName).concat(":").concat(phase);
			values[2] = sdlcPhaseURI;
			costRelList.add(values);
			addToLabelList(labelCostList, values);

			// system -> influences -> glitem
			values = new Object[3];
			values[0] = sysURI;
			values[1] = influencesInstanceRel.concat(sys).concat(":").concat(glItemName);
			values[2] = glItemURI;
			costRelList.add(values);
			addToLabelList(labelCostList, values);

			// data input gl items
			values = new Object[3];
			values[0] = dataURI;
			values[1] = inputInstanceRel.concat(data).concat(":").concat(glItemName);
			values[2] = glItemURI;
			costRelList.add(values);
			addToLabelList(labelCostList, values);

			// ordering of gl items
			if (previousGLItemURI.equals("")) {
				previousGLItemURI = glItemURI;
				previousGLItemName = glItemName;
			} else {
				// glItem -> precedes -> glitem
				values = new Object[3];
				values[0] = previousGLItemURI;
				values[1] = precedesInstanceRel.concat(previousGLItemName).concat(":").concat(glItemName);
				values[2] = glItemURI;
				costRelList.add(values);
				addToLabelList(labelCostList, values);

				previousGLItemURI = glItemURI;
			}
			// glItem -> contains -> loe
			values = new Object[3];
			values[0] = glItemURI;
			values[1] = semossPropURI.concat("LOEcalc");
			values[2] = costInfo.get(phase);
			loeList.add(values);

			// keep track of all glitems
			glItemList.add(glItemURI);
			// keep track of all systems
			sysCostList.add(sysURI);
		}
	}

	private String[] orderResults(Set<String> keySet) {
		String[] retResults = new String[5];
		for (String s : keySet) {
			if (s.contains("Requirements")) {
				retResults[0] = s;
			} else if (s.contains("Design")) {
				retResults[1] = s;
			} else if (s.contains("Develop")) {
				retResults[2] = s;
			} else if (s.contains("Test")) {
				retResults[3] = s;
			} else if (s.contains("Deploy")) {
				retResults[4] = s;
			}
		}

		return (String[]) ArrayUtilityMethods.removeAllNulls(retResults);
	}

	private String makeDHMSMConsumerOfICD(final String icd, final String sysProvider, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/') + 1);
		return base.concat(sysProvider).concat("-").concat(DHMSM).concat("-").concat(dataObject);
	}

	private String makeDHMSMProviderOfICD(final String icd, final String sysConsumer, final String dataObject) {
		String base = icd.substring(0, icd.lastIndexOf('/') + 1);
		return base.concat(DHMSM).concat("-").concat(sysConsumer).concat("-").concat(dataObject);
	}

	private void addToLabelList(Set<String> list, Object[] value) {
		for (Object instance : value) {
			list.add(instance.toString());
		}
	}

	public List<String> getAddedInterfaces() {
		return this.addedInterfaces;
	}
	
	public List<Object[]> getRelList() {
		return this.relList;
	}
	
	public List<String> getRemovedInterfaces() {
		return removedInterfaces;
	}

	public List<Object[]> getCostRelList() {
		return costRelList;
	}

	public List<Object[]> getRelPropList() {
		return relPropList;
	}

	public Set<String> getSysList() {
		return sysList;
	}

	public List<Object[]> getLoeList() {
		return loeList;
	}

	public Set<String> getGlItemList() {
		return glItemList;
	}

	public Set<String> getSysCostList() {
		return sysCostList;
	}

	public Set<String> getLabelList() {
		return labelList;
	}

	public Set<String> getLabelCostList() {
		return labelCostList;
	}

	public void setRelList(List<Object[]> relList) {
		this.relList = relList;
	}

	public void setProviderFutureICDProp(Map<String, Map<String, String>> providerFutureIcdFrequency) {
		this.providerFutureIcdFrequency = providerFutureIcdFrequency;
	}
	
	public void setConsumerFutureICDProp(Map<String, Map<String, String>> consumerFutureIcdFrequency) {
		this.consumerFutureIcdFrequency = consumerFutureIcdFrequency;
	}
	
	public Set<String> getSysTrainingList() {
		return this.sysTrainingList;
	}

}
