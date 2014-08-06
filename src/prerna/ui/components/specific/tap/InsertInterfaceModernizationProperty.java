package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsertInterfaceModernizationProperty {

	static final Logger logger = LogManager.getLogger(InsertInterfaceModernizationProperty.class.getName());

	private String lpiSysListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }} ORDER BY ?entity BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String lpniSysListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }} ORDER BY ?entity BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";

	private String totalDirectCostKey = "directCost";
	private String costPropertyURI = "http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost";

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
		HashSet<String> lpiList = runListQuery(HR_Core, lpiSysListQuery);
		HashSet<String> lpniList = runListQuery(HR_Core, lpniSysListQuery);

		IndividualSystemTransitionReport generateCostInfo = new IndividualSystemTransitionReport();

		IEngine TAP_Cost = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(TAP_Cost == null) {
			throw new EngineException("TAP_Cost_Data Database not found");
		}
		
		generateCostInfo.setTAP_Cost_Data(TAP_Cost);
		generateCostInfo.setHR_Core(HR_Core);
		generateCostInfo.getCostInfo();
		generateCostInfo.getLPNIInfo();

		for(String sysURI : lpiList) {
			generateCostInfo.setSystemURI(sysURI);
			generateCostInfo.setSystemName(sysURI.substring(sysURI.lastIndexOf("/")+1));
			generateCostInfo.setReportType("LPI");
			HashMap<String, Object> sysLPInterfaceWithCostHash = generateCostInfo.calculateInterfaceModernizationCost();
			Object cost = (Double) sysLPInterfaceWithCostHash.get(totalDirectCostKey);
			if(cost == null) {
				cost = "NA";
			}
			addProperty(sysURI, costPropertyURI, cost, false);
		}

		for(String sysURI : lpniList) {
			generateCostInfo.setSystemURI(sysURI);
			generateCostInfo.setSystemName(sysURI.substring(sysURI.lastIndexOf("/")+1));
			generateCostInfo.setReportType("LPNI");
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

	private HashSet<String> runListQuery(IEngine engine, String query) 
	{
		HashSet<String> dataSet = new HashSet<String>();
		SesameJenaSelectWrapper sjsw = processQuery(engine, query);
		String[] names = sjsw.getVariables();

		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String var = sjss.getRawVar(names[0]).toString().replace("\"", "");
			dataSet.add(var);
		}

		return dataSet;
	}

	private SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}

}
