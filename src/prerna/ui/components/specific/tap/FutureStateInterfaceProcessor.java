package prerna.ui.components.specific.tap;

import java.util.Map;
import java.util.Set;

public class FutureStateInterfaceProcessor {

	private static final String DHMSM_PROVIDE_KEY = "Provider";
	private static final String DHMSM_CONSUME_KEY = "Consumer";
	private static final String LPI_KEY = "LPI";
	private static final String HPI_KEY = "HPI";
	private static final String HPNI_KEY = "HPNI";
	private static final String NO_PROBABILITY = "No Probability";
	private static final String DOWNSTREAM_KEY = "Downstream";

	private FutureStateInterfaceProcessor() {

	}

	public static FutureStateInterfaceResult processICD(
			String sysName,
			String interfaceType,
			String interfacingSysName,
			String icd,
			String data,
			String dhmsmSOR,
			Set<String> selfReportedSystems,
			Set<String> sorV, 
			Map<String, String> sysTypeHash, 
			Set<String> consumeSet, 
			Set<String> provideSet) 
	{
		String upstreamSysName = "";
		String upstreamSysType = "";
		String downstreamSysName = "";
		String downstreamSysType = "";
		if (interfaceType.contains(DOWNSTREAM_KEY)) { // lp system is providing data to interfacing system
			upstreamSysName = sysName;
			upstreamSysType = sysTypeHash.get(sysName);
			downstreamSysName = interfacingSysName;
			downstreamSysType = sysTypeHash.get(interfacingSysName);
		} else { // lp system is receiving data from interfacing system
			upstreamSysName = interfacingSysName;
			upstreamSysType = sysTypeHash.get(interfacingSysName);
			downstreamSysName = sysName;
			downstreamSysType = sysTypeHash.get(sysName);
		}

		if(upstreamSysType == null || upstreamSysType.isEmpty()) {
			upstreamSysType = NO_PROBABILITY;
		}
		if(downstreamSysType == null || downstreamSysType.isEmpty()) {
			downstreamSysType = NO_PROBABILITY;
		}

		FutureStateInterfaceResult results = calculateResponse(
				upstreamSysName, 
				upstreamSysType, 
				downstreamSysName, 
				downstreamSysType, 
				sysName, 
				icd, 
				data, 
				dhmsmSOR, 
				selfReportedSystems, 
				sorV,
				consumeSet,
				provideSet);

		results.put(FutureStateInterfaceResult.LEGACY_DOWNSTREAM_SYSTEM, downstreamSysName);
		results.put(FutureStateInterfaceResult.LEGACY_UPSTREAM_SYSTEM, upstreamSysName);

		return results;
	}

	public static FutureStateInterfaceResult calculateResponse(
			String upstreamSysName,
			String upstreamSysType,
			String downstreamSysName,
			String downstreamSysType,
			String sysName,
			String icd,
			String data,
			String dhmsmSOR,
			Set<String> selfReportedSystems,
			Set<String> sorV, 
			Set<String> consumeSet, 
			Set<String> provideSet) 
	{
		FutureStateInterfaceResult results = new FutureStateInterfaceResult();
		// variables to place in Map
		String comment = "";
		boolean directCost = false;
		boolean decommission = false;
		boolean isSelfReported = false;
		if (dhmsmSOR.contains(DHMSM_PROVIDE_KEY)) {
			if (upstreamSysType.equals(LPI_KEY)) { // upstream system is LPI
				if (!selfReportedSystems.contains(upstreamSysName)) {
					comment = comment.concat("Need to add interface DHMSM->").concat(upstreamSysName).concat(". ");
					// direct cost if system is upstream and indirect is downstream
					if (sysName.equals(upstreamSysName)) {
						directCost = true;
					} else {
						directCost = false;
					}
				} else {
					isSelfReported = true;
				}
				// if downstream system is HP, remove interface
				if (downstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY)) {
					comment = comment.concat(" Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).");
					decommission = true;
				} else if(!isSelfReported){
					comment = comment.concat(" System " + upstreamSysName + " currently gets data object, " + data + ", from system " + downstreamSysName + ". "
							+ "DHMSM is also projected to be a source of record for this data object, therefore, we recommend reviewing the proposed DHMSM interface.");
					decommission = false;
				}

				// update results and return
				results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
				if(directCost) {
					results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.DIRECT );
				} else {
					results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.INDIRECT );
				}
				if(isSelfReported) {
					if(decommission) {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED);
					}
				} else {
					if(decommission) {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION);
					} else {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_CONSUMER_FROM_DHMSM);
					}
				}
				if(consumeSet.contains("DHMSM+++" + upstreamSysName + "+++" + data)) {
					results.setCostTakenIntoConsideration(true);
				} else {
					consumeSet.add("DHMSM+++" + upstreamSysName + "+++" + data);
				}
			}
			// new business rule might be added - will either un-comment or remove after discussion today
			// else if (upstreamSysType.equals(lpniKey)) { // upstream system is LPNI
			// comment += "Recommend review of developing interface DHMSM->" + upstreamSysName + ". ";
			// }
			else if (downstreamSysType.equals(LPI_KEY)) { // upstream system is not LPI and downstream system is LPI
				if (!selfReportedSystems.contains(downstreamSysName)) {
//					comment = comment.concat("Need to add interface DHMSM->").concat(downstreamSysName).concat(".").concat(
//							" Recommend review of removing interface ").concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(
//									". ");
					comment = comment.concat("Need to add interface DHMSM->").concat(downstreamSysName).concat(".");
					// direct cost if system is downstream
					if (sysName.equals(downstreamSysName)) {
						directCost = true;
					} else {
						directCost = false;
					}
					if (upstreamSysType.equals(HPNI_KEY) || upstreamSysType.equals(HPI_KEY)) {
						comment = comment.concat(" Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).");
						decommission = true;
					} else {
						decommission = false;
					}
					
					results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
					if(directCost) {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.DIRECT );
					} else {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.INDIRECT );
					}
					if(decommission) {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION);
					} else {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_CONSUMER_FROM_DHMSM);
					}
					if(consumeSet.contains("DHMSM+++" + downstreamSysName + "+++" + data)) {
						results.setCostTakenIntoConsideration(true);
					} else {
						consumeSet.add("DHMSM+++" + downstreamSysName + "+++" + data);
					}
				} else {
					// if upstream system is HP, remove interface
					if (upstreamSysType.equals(HPNI_KEY) || upstreamSysType.equals(HPI_KEY)) {
						comment = comment.concat(" Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).");
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED);
						results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
					}
				}
			} else {
				if (upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY)
						|| downstreamSysType.equals(HPNI_KEY)) { // if either system is HP
					comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).";
					results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
					results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
					results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED);
				} else {
					comment = "Stay as-is beyond FOC.";
					results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
					results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
					results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.STAY_AS_IS);
				}
			}
		} else if (dhmsmSOR.contains(DHMSM_CONSUME_KEY)) { // DHMSM is consumer of data
			boolean otherwise = true;
			if (upstreamSysType.equals(LPI_KEY) && sorV.contains(upstreamSysName + data)) { // upstream system is LPI and SOR of data
				otherwise = false;
				if (!selfReportedSystems.contains(upstreamSysName)) {
					comment = comment.concat("Need to add interface ").concat(upstreamSysName).concat("->DHMSM. ");
					// direct cost if system is upstream
					if (sysName.equals(upstreamSysName)) {
						directCost = true;
					} else {
						directCost = false;
					}
					if(directCost) {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.DIRECT );
					} else {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.INDIRECT );
					}
					results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
					results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM);
					if(provideSet.contains(upstreamSysName + "+++DHMSM+++" + data)) {
						results.setCostTakenIntoConsideration(true);
					} else {
						provideSet.add(upstreamSysName + "+++DHMSM+++" + data);
					}
				}
			} else if (downstreamSysType.equals(LPI_KEY) && sorV.contains(downstreamSysName + data)) { // downstream system is LPI and SOR of
				otherwise = false;
				if (!selfReportedSystems.contains(downstreamSysName)) {
					comment = comment.concat("Need to add interface ").concat(downstreamSysName).concat("->DHMSM. ");
					if (sysName.equals(downstreamSysName)) {
						directCost = true;
					} else {
						directCost = false;
					}
					if(directCost) {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.DIRECT );
					} else {
						results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.INDIRECT );
					}
					results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
					results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM);
					if(provideSet.contains(downstreamSysName + "+++DHMSM+++" + data)) {
						results.setCostTakenIntoConsideration(true);
					} else {
						provideSet.add(downstreamSysName + "+++DHMSM+++" + data);
					}
				}
			} 
			// check if we should decommission the current interface
			if (upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY)
					|| downstreamSysType.equals(HPNI_KEY)) { // if either system is HP
				comment = comment.concat("Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).");
				results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
				results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
				if(results.containsKey(FutureStateInterfaceResult.RECOMMENDATION)) {
					if(results.get(FutureStateInterfaceResult.RECOMMENDATION) == FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM) {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DOWNSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION);
					} else if(results.get(FutureStateInterfaceResult.RECOMMENDATION) == FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM) {
						results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.UPSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION);
					} else {
						// this case cannot occur... cannot have both be adding to dhmsm and one of them being hp
					}
				} else {
					results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED);
				}
			} else if(otherwise) {
				comment = "Stay as-is beyond FOC.";
				results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
				results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
				results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.STAY_AS_IS);
			}
		} else { // other cases DHMSM doesn't touch data object
			if (upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) 
					|| downstreamSysType.equals(HPNI_KEY)) { // if either system is HP
				comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements).";
				results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
				results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
				results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.DECOMMISSIONED);
			} else {
				comment = "Stay as-is beyond FOC.";
				results.put(FutureStateInterfaceResult.COST_TYPE, FutureStateInterfaceResult.COST_TYPES.NO_COST );
				results.put(FutureStateInterfaceResult.COMMENT, comment.trim());
				results.put(FutureStateInterfaceResult.RECOMMENDATION, FutureStateInterfaceResult.INTERFACE_TYPES.STAY_AS_IS);
			} 
		}
		
		return results;
	}
}
