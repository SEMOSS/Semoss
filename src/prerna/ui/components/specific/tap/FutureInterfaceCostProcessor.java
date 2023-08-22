package prerna.ui.components.specific.tap;

import java.util.Map;

import prerna.engine.api.IDatabaseEngine;
import prerna.util.DHMSMTransitionUtility;

public class FutureInterfaceCostProcessor extends AbstractFutureInterfaceCostProcessor{

	private COST_FRAMEWORK costFramework;
	
	private Map<String, Map<String, Map<String, Map<String, Map<String, Double>>>>> loeForSysGlItemAndPhaseHash;
	private Map<String, Map<String, Map<String, Double>>> genericLoeForSysGLItemAndPhaseHash;
	private Map<String, Map<String, Map<String, Map<String, Double>>>> avgLoeForSysGLItemAndPhaseHash;
	private Map<String, String> serviceToDataHash;

	private Map<String, Map<String, Map<String, Map<String, Double>>>> loeForSysGlItemAndPhaseHashByAvgSer;
	private Map<String, Map<String, Map<String, Double>>> avgLoeForSysGLItemAndPhaseHashByAvgSer;
	private Map<String, Map<String, Double>> genericGLItemAndPhaseHashByAvgServ;
	
	private IDatabaseEngine[] engines;
	
	public FutureInterfaceCostProcessor() {

	}
	
	public FutureInterfaceCostProcessor(COST_FRAMEWORK costFramework) {
		this.costFramework = costFramework;
	}
	
	@Override
	public Map<String, Double> calculateCost(String dataObject, String system, String tag) {
		if(costFramework.equals(COST_FRAMEWORK.SOA)) {
			return FutureStateInterfaceCost.calculateSOATypePhaseCost();
			
		} else if(costFramework.equals(COST_FRAMEWORK.P2P)) {
			return FutureStateInterfaceCost.calculateP2PTypePhaseCost(
					dataObject,
					system,
					tag,
					loeForSysGlItemAndPhaseHashByAvgSer,
					avgLoeForSysGLItemAndPhaseHashByAvgSer,
					genericGLItemAndPhaseHashByAvgServ
					);
		} else {
			throw new IllegalArgumentException("Must select a valid cost framework... Please choose either SOA or P2P.");
		}
	}
	
	@Override
	public void getCostInfo() {
		if(costFramework.equals(COST_FRAMEWORK.SOA)) {
			if(loeForSysGlItemAndPhaseHash == null || loeForSysGlItemAndPhaseHash.isEmpty()) {
				loeForSysGlItemAndPhaseHash = DHMSMTransitionUtility.getSysGLItemAndPhase(engines[0]);
			}
			if(genericLoeForSysGLItemAndPhaseHash == null || genericLoeForSysGLItemAndPhaseHash.isEmpty()) {
				genericLoeForSysGLItemAndPhaseHash = DHMSMTransitionUtility.getGenericGLItemAndPhase(engines[0]);
			}
			if(avgLoeForSysGLItemAndPhaseHash == null || avgLoeForSysGLItemAndPhaseHash.isEmpty()) {
				avgLoeForSysGLItemAndPhaseHash = DHMSMTransitionUtility.getAvgSysGLItemAndPhase(engines[0]);
			}
			if(serviceToDataHash == null || serviceToDataHash.isEmpty()) {
				serviceToDataHash = DHMSMTransitionUtility.getServiceToData(engines[0]);
			}
		} else if(costFramework.equals(COST_FRAMEWORK.P2P)) {
			if(loeForSysGlItemAndPhaseHashByAvgSer == null || loeForSysGlItemAndPhaseHashByAvgSer.isEmpty()) {
				loeForSysGlItemAndPhaseHashByAvgSer = DHMSMTransitionUtility.getSysGLItemAndPhaseByAvgServ(engines[0]);
			}
			if(avgLoeForSysGLItemAndPhaseHashByAvgSer == null || avgLoeForSysGLItemAndPhaseHashByAvgSer.isEmpty()) {
				avgLoeForSysGLItemAndPhaseHashByAvgSer = DHMSMTransitionUtility.getAvgSysGLItemAndPhaseByAvgServ(engines[0]);
			}
			if(genericGLItemAndPhaseHashByAvgServ == null || genericGLItemAndPhaseHashByAvgServ.isEmpty()) {
				genericGLItemAndPhaseHashByAvgServ = DHMSMTransitionUtility.getGenericGLItemAndPhaseByAvgServ(engines[0]);
			}
		} else {
			throw new IllegalArgumentException("Must select a valid cost framework... Please choose either SOA or P2P.");
		}
		
	}
	
	@Override
	public void setCostEngines(IDatabaseEngine[] engines) {
		this.engines = engines;
	}
	
	@Override
	public void setCostFramework(COST_FRAMEWORK costFramework) {
		this.costFramework = costFramework;
	}

}
