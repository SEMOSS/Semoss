package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Map;

public class FutureStateInterfaceCost {

	private FutureStateInterfaceCost() {

	}

	public static Map<String, Double> calculateSOATypePhaseCost() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 
	 * Note that 
	 * @param dataObject
	 * @param system
	 * @param tag
	 * @param loeForSysGlItemAndPhaseHashByAvgSer
	 * @param avgLoeForSysGLItemAndPhaseHashByAvgSer
	 * @return
	 */
	public static Map<String, Double> calculateP2PTypePhaseCost(
			String dataObject,
			String system,
			String tag,
			Map<String, Map<String, Map<String, Map<String, Double>>>> loeForSysGlItemAndPhaseHashByAvgSer,
			Map<String, Map<String, Map<String, Double>>> avgLoeForSysGLItemAndPhaseHashByAvgSer,
			Map<String, Map<String, Double>> genericGLItemAndPhaseByAvgServ)
	{
		Map<String, Double> loeByPhase = new HashMap<String, Double>();

		boolean useAverage = true;
		if(loeForSysGlItemAndPhaseHashByAvgSer.containsKey(dataObject)) {
			Map<String, Map<String, Map<String, Double>>> innerHash1 = loeForSysGlItemAndPhaseHashByAvgSer.get(dataObject);
			if(innerHash1.containsKey(system)) {
				Map<String, Map<String, Double>> innerHash2 = innerHash1.get(system);
				if(innerHash2.containsKey(tag)) {
					Map<String, Double> innerHash3 = innerHash2.get(tag);
					for(String phase : innerHash3.keySet()) {
						Double loe = innerHash3.get(phase);
						if(loeByPhase.containsKey(phase)) {
							System.err.println("Should not have multiple loe's for single icd");
						} else {
							if(loe != null && loe > 0) {
								useAverage = false;
								loeByPhase.put(phase, loe);
							}
						}
					}

				}
			}
		}

		if(useAverage) {
			Map<String, Map<String, Double>> innerHash1 = avgLoeForSysGLItemAndPhaseHashByAvgSer.get(dataObject);
			if(innerHash1 == null) {
				//TODO: need to discuss what to do when no LOE
				if(tag.equals("Consumer")) {
					loeByPhase.put("Requirements", 30.0);
					loeByPhase.put("Design", 30.0);
					loeByPhase.put("Develop", 30.0);
					loeByPhase.put("Test", 30.0);
				} else {
					loeByPhase.put("Requirements", 80.0);
					loeByPhase.put("Design", 80.0);
					loeByPhase.put("Develop", 80.0);
					loeByPhase.put("Test", 80.0);
					loeByPhase.put("Deploy", 80.0);
				}
			} else {
				if(innerHash1.containsKey(tag)) {
					Map<String, Double> innerHash2 = innerHash1.get(tag);
					for(String phase : innerHash2.keySet()) {
						Double loe = innerHash2.get(phase);
						if(loeByPhase.containsKey(phase)) {
							System.err.println("Should not have multiple loe's for single icd");
						} else {
							if(loe != null && loe > 0) {
								loeByPhase.put(phase, loe);
							}
						}
					}
				}
			}
		}
		
		// include generic cost
		if(tag.equals("Provider")) {
			Map<String, Double> phaseInfo = genericGLItemAndPhaseByAvgServ.get(dataObject);
			for(String phase : phaseInfo.keySet()) {
				Double loe = phaseInfo.get(phase);
				if(loeByPhase.containsKey(phase)) {
					Double currLOE = loeByPhase.get(phase);
					if(loe != null && loe > 0) {
						currLOE += loe;
						loeByPhase.put(phase, currLOE);
					}
				} else {
					if(loe != null && loe > 0) {
						loeByPhase.put(phase, loe);
					}				
				}
			}
		}

		return loeByPhase;
	}

}
