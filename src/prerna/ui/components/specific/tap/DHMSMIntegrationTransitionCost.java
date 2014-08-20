package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DHMSMTransitionQueryConstants;

public class DHMSMIntegrationTransitionCost {
	
	static final Logger LOGGER = LogManager.getLogger(DHMSMIntegrationTransitionCost.class.getName());

	private static HashMap<String, HashMap<String, HashMap<String, Double>>> loeForSysGlItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	private static HashMap<String, HashMap<String, HashMap<String, Double>>> genericLoeForSysGLItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	private static HashMap<String, HashMap<String, HashMap<String, Double>>> avgLoeForSysGLItemAndPhaseHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();
	
	private IEngine hrCore;
	
	private double costPerHr;
	
	public DHMSMIntegrationTransitionCost() {
		generateCostInformation();
	}
	
	public void setCostPerHr(double costPerHr) {
		this.costPerHr = costPerHr;
	}
	
	public void setHrCore(IEngine hrCore) {
		this.hrCore = hrCore;
	}
	
	public void generateCostInformation() {
		if(loeForSysGlItemAndPhaseHash.isEmpty()) {
			loeForSysGlItemAndPhaseHash = getSysGLItemAndPhase(hrCore, DHMSMTransitionQueryConstants.SYS_SPECIFIC_LOE_AND_PHASE_QUERY);
		}
		if(genericLoeForSysGLItemAndPhaseHash.isEmpty()) {
			genericLoeForSysGLItemAndPhaseHash = getGenericGLItemAndPhase(hrCore, DHMSMTransitionQueryConstants.GENERIC_LOE_AND_PHASE_QUERY);
		}
		if(avgLoeForSysGLItemAndPhaseHash.isEmpty()) {
			avgLoeForSysGLItemAndPhaseHash = getAvgSysGLItemAndPhase(hrCore, DHMSMTransitionQueryConstants.AVERAGE_LOE_AND_PHASE_QUERY);
		}
	}
	
	private static HashMap<String, HashMap<String, HashMap<String, Double>>> getSysGLItemAndPhase(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
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
				innerHash.put(phase, loe);
				outerHash.put(sys + "+++" + ser + "+++" + glTag, innerHash);
			}
		}
		return dataHash;
	}

	private HashMap<String, HashMap<String, HashMap<String, Double>>> getAvgSysGLItemAndPhase(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
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
				innerHash.put(phase, loe);
				outerHash.put(ser + "+++" + glTag, innerHash);
			}
		}
		return dataHash;
	}

	private HashMap<String, HashMap<String, HashMap<String, Double>>> getGenericGLItemAndPhase(IEngine engine, String query)
	{
		HashMap<String, HashMap<String, HashMap<String, Double>>> dataHash = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
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
				innerHash.put(phase, loe);
				outerHash.put(ser, innerHash);
			}
		}
		return dataHash;
	}
	
	
	public Double calculateCost(String dataObject, String system, String tag, boolean includeGenericCost, HashSet<String> servicesProvideList)
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
								sysGLItemCost += phaseHash.get(phase);
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
								sysGLItemCost += phaseHash.get(phase);
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
							genericCost += phaseHash.get(phase);
						}
					} 
				}
			}
		}

		Double finalCost = null;
		if(!servicesAllUsed) {
			finalCost = (sysGLItemCost + genericCost) * costPerHr;
		}

		return finalCost;
	}
	
	private static SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		LOGGER.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}
}
