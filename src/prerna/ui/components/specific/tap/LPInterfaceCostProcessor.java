package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK;
import prerna.ui.components.specific.tap.FutureStateInterfaceResult.COST_TYPES;
import prerna.ui.components.specific.tap.FutureStateInterfaceResult.INTERFACE_TYPES;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

public class LPInterfaceCostProcessor extends AbstractLPInterfaceProcessor {

	private FutureInterfaceCostProcessor processor;
	private IDatabase tapCost;
	private IDatabase futureDB;
	
	//TODO: move enigne definitions outside class to keep reusable
	public LPInterfaceCostProcessor() throws IOException {
		tapCost = (IDatabase) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(tapCost == null) {
			throw new IOException("TAP Cost Data not found.");
		}
		futureDB = (IDatabase) DIHelper.getInstance().getLocalProp("FutureDB");
		if(futureDB == null) {
			throw new IOException("FutureDB engine not found");
		}
		processor = new FutureInterfaceCostProcessor();
		processor.setCostEngines(new IDatabase[]{tapCost});
		processor.setCostFramework(COST_FRAMEWORK.P2P); // should define this via parameter
		processor.getCostInfo();
	}
	
	public double generateSystemCost(String specificSystem,
			Set<String> selfReportedSystems,
			Set<String> sorV,
			Map<String, String> sysTypeHash,
			AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK framework) {
		
		Map<String, Map<String, Double>> cost = generateSystemCostByTagPhase(specificSystem, selfReportedSystems, sorV, sysTypeHash, framework);
		
		double loe = 0.0;
		for(String tagKey : cost.keySet()) {
			Map<String, Double> innerHash = cost.get(tagKey);
			loe += aggregateCost(innerHash);
		}
		return loe;
	}
	
	public Map<String, Map<String, Double>> generateSystemCostByTagPhase(
			String specificSystem,
			Set<String> selfReportedSystems,
			Set<String> sorV,
			Map<String, String> sysTypeHash,
			AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK framework) {

		Map<String, Map<String, Double>> retMap = new HashMap<String, Map<String, Double>>();
		// Process main query
		setQueriesForSysName(specificSystem);
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
				String dhmsmSOR = "";
				
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
				if (sjss.getVar(DHMSM) != null) {
					dhmsmSOR = sjss.getVar(DHMSM).toString();
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
				
				Map<String, Double> costResults = null;
				String tag = "";
				INTERFACE_TYPES recommendation = (INTERFACE_TYPES) result.get(FutureStateInterfaceResult.RECOMMENDATION);
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
				
				//only consider direct costs for a system
				if(result.get(FutureStateInterfaceResult.COST_TYPE) == FutureStateInterfaceResult.COST_TYPES.DIRECT) {
					if(costResults != null && !costResults.isEmpty()) {
						if(retMap.containsKey(tag)) {
							Map<String, Double> previousCost = retMap.get(tag);
							for(String key : costResults.keySet()) {
								Double newLoe = previousCost.get(key) + costResults.get(key);
								previousCost.put(key, newLoe);
							}
						} else {
							retMap.put(tag, costResults);
						}
					}
				}
			}
		}
		
		return retMap;
	}
	
	//TODO: should remove this and not override method to keep reusable
	public Map<String, Object> generateSystemTransitionReport(String specificSystem, AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK framework) {
		Set<String> selfReportedSystems = DHMSMTransitionUtility.getAllSelfReportedSystemNames(futureDB);
		Set<String> sorV = DHMSMTransitionUtility.processSysDataSOR(engine);
		Map<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);
		
		return generateSystemTransitionReport(specificSystem, selfReportedSystems, sorV, sysTypeHash, framework);
	}
	
	public Map<String, Object> generateSystemTransitionReport(
			String specificSystem,
			Set<String> selfReportedSystems,
			Set<String> sorV,
			Map<String, String> sysTypeHash,
			AbstractFutureInterfaceCostProcessor.COST_FRAMEWORK framework) {

		double directCost = 0.0;
		double indirectCost = 0.0;
		
		List<Object[]> retList = new ArrayList<Object[]>();
		// Process main query
		setQueriesForSysName(specificSystem);
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
				String interfacingSysDisposition = "";
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
				if (sjss.getVar(DISPOSITION_KEY) != null) {
					interfacingSysDisposition = sjss.getVar(DISPOSITION_KEY).toString();
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
				
				Object[] newRow = new Object[headers.length];
				newRow[0] = sysName;
				newRow[1] = interfaceType;
				newRow[2] = interfacingSysName;
				newRow[3] = interfacingSysDisposition;
				newRow[4] = icd;
				newRow[5] = data;
				newRow[6] = format;
				newRow[7] = freq;
				newRow[8] = prot;
				newRow[9] = dhmsmSOR;
				newRow[10] = result.get(FutureStateInterfaceResult.COMMENT);
				retList.add(newRow);
				
				Map<String, Double> costResults = null;
				if(!result.isCostTakenIntoConsideration()) {
					INTERFACE_TYPES recommendation = (INTERFACE_TYPES) result.get(FutureStateInterfaceResult.RECOMMENDATION);
					if(recommendation != INTERFACE_TYPES.DECOMMISSIONED && recommendation != INTERFACE_TYPES.STAY_AS_IS) {
						if(recommendation == INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM ||
							recommendation == INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION ||
							recommendation == INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM ||
							recommendation == INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION ) 
						{
							costResults = processor.calculateCost(data, sysName, "Consumer");
						} else {
							costResults = processor.calculateCost(data, sysName, "Provider");
						}
					}
				}
				
				if(costResults != null) {
					COST_TYPES costType = (COST_TYPES) result.get(FutureStateInterfaceResult.COST_TYPE);
					if(costType == COST_TYPES.DIRECT) {
						directCost += aggregateCost(costResults);
					} else if(costType == COST_TYPES.INDIRECT) {
						indirectCost += aggregateCost(costResults);
					}
				}
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("data", retList);
		retMap.put("headers", headers);
		retMap.put("directCost", directCost);
		retMap.put("indirectCost", indirectCost);
		return retMap;
	}
}
