package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DHMSMTransitionQueryConstants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsertInterfaceModernizationProperty {

	static final Logger LOGGER = LogManager.getLogger(InsertInterfaceModernizationProperty.class.getName());

	private final String sysURIPrefix = "http://health.mil/ontologies/Concept/System/";
	private final String totalDirectCostKey = "directCost";
	private final String costPropertyURI = "http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost";

	private IEngine HR_Core;

	public void insert() throws EngineException
	{
		try{
			HR_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			if(HR_Core==null)
				throw new EngineException("Database not found");
		} catch(EngineException e) {
			Utility.showError("Could not find necessary database: HR_Core. Cannot generate report.");
			return;
		}
		getCostFromInterfaceReport();
	}

	private void getCostFromInterfaceReport() throws EngineException 
	{
		Hashtable<String,String> reportTypeHash = processReportTypeQuery();

		IndividualSystemTransitionReport generateCostInfo = new IndividualSystemTransitionReport();

		IEngine TAP_Cost = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost == null) {
			throw new EngineException("TAP_Cost_Data Database not found");
		}
		
		generateCostInfo.setTAP_Cost_Data(TAP_Cost);
		generateCostInfo.setHR_Core(HR_Core);
		generateCostInfo.getCostInfo();
		generateCostInfo.getLPNIInfo();

		for(String sysName : reportTypeHash.keySet()){
			String sysURI = sysURIPrefix + sysName;
			generateCostInfo.setSystemURI(sysURI);
			generateCostInfo.setSystemName(sysName);
			String reportType = reportTypeHash.get(sysName);
			generateCostInfo.setReportType(reportType);
			HashMap<String, Object> sysLPInterfaceWithCostHash = generateCostInfo.calculateInterfaceModernizationCost();
			Object cost = (Double) sysLPInterfaceWithCostHash.get(totalDirectCostKey);
			if(cost == null) {
				cost = "NA";
			}
			addProperty(sysURI, costPropertyURI, cost, false);
		}
	}

	private void addProperty(String sub, String pred, Object obj, boolean concept_triple) 
	{
		( (BigDataEngine) HR_Core).addStatement(sub, pred, obj, concept_triple);
		( (BigDataEngine) HR_Core).commit();
		System.out.println(sub + " >>> " + pred + " >>> " + obj);
	}

	public Hashtable<String,String> processReportTypeQuery() {
		Hashtable<String,String> retList = new Hashtable<String,String>();

		SesameJenaSelectWrapper sjsw = processQuery(HR_Core, DHMSMTransitionQueryConstants.SYS_TYPE_QUERY);
		String[] varName = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String entity = sjss.getRawVar(varName[0]).toString();
			String instance = Utility.getInstanceName(entity);
			String reportType = sjss.getVar(varName[1]).toString();
			retList.put(instance,reportType);
		}

		return retList;
	}

	private SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		LOGGER.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}

}
