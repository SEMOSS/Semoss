package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class SystemPropertyGridPlaySheet  extends GridPlaySheet {

	private String costQuery = "SELECT DISTINCT ?System (SUM(?FY15) as ?fy15) (SUM(?FY16) as ?fy16) (SUM(?FY17) as ?fy17) (SUM(?FY18) as ?fy18) WHERE { { SELECT DISTINCT ?System ?FY15 ?FY16 ?FY17 ?FY18 WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudgetGLItem} OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY15 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY15>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY16 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY16>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY17 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY17>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY18 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY18>} } } } UNION { SELECT DISTINCT ?System ?FY15 ?FY16 ?FY17 ?FY18 WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemServiceBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem> ;} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudgetGLItem} OPTIONAL { {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY15 ;} {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY15>} } OPTIONAL { {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY16 ;} {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY16>} } OPTIONAL { {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY17 ;} {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY17>} } OPTIONAL { {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY18 ;} {?SystemServiceBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY18>} } } } } GROUP BY ?System ";
	private String tapPortfolio = "TAP_Portfolio";
	private String tapCost = "TAP_Cost_Data";
	
	private String[] costDataVarNames;
	private Integer costDataLength = 0;
	@Override
	public void createData() {
		Hashtable<String, Hashtable<String, Double>> costHash = new Hashtable<String, Hashtable<String, Double>>();

		boolean includeCost = true;
		IEngine portfolioEngine =  (IEngine) DIHelper.getInstance().getLocalProp(tapPortfolio);
		IEngine costEngine = (IEngine) DIHelper.getInstance().getLocalProp(tapCost);
		if(portfolioEngine != null) { // process cost query from portfolio (faster on smaller db)
			costHash = processCostQuery(portfolioEngine);
		} else if(costEngine != null) {
			costHash = processCostQuery(costEngine);
		} else {
			includeCost = false;
		}
		
		SesameJenaSelectWrapper runQuery = new SesameJenaSelectWrapper();
		runQuery.setQuery(this.query);
		runQuery.setEngine(this.engine);
		runQuery.executeQuery();
		String[] varNames = runQuery.getVariables();
		
		this.names = new String[varNames.length + costDataLength];
		for(int i = 0; i < names.length; i++) {
			if(i < varNames.length) {
				names[i] = varNames[i];
			} else {
				names[i] = costDataVarNames[i-varNames.length+1];
			}
		}
		
		list = new ArrayList<Object[]>();
		while(runQuery.hasNext()) {
			Object[] addRow = new Object[varNames.length + costDataLength];
			
			SesameJenaSelectStatement sjss = runQuery.next();
			String sysName = "";
			for(int i = 0; i < varNames.length + 1; i++) {
				if(i == 0) {
					sysName = sjss.getVar(varNames[i]).toString();
					addRow[i] = sysName;
				} else if(i == varNames.length) {
					if(includeCost) {
						Hashtable<String, Double> costInfo = costHash.get(sysName);
						if(costInfo != null) {
							for(int j = 0; j < costDataLength; j++) {
								addRow[i+j] = Math.round(costInfo.get(costDataVarNames[j+1]));
							}
						} else {
							for(int j = 0; j < costDataLength; j++) {
								addRow[i+j] = "No cost info received.";
							}
						}
					} else {
						for(int j = 0; j < costDataLength; j++) {
							addRow[i+j] = "*";
						}
					}
				} else {
					addRow[i] = sjss.getVar(varNames[i]);
				}
			}
			list.add(addRow);
		}
		
	}

	private Hashtable<String, Hashtable<String, Double>> processCostQuery(IEngine engine) 
	{
		Hashtable<String, Hashtable<String, Double>> costHash = new Hashtable<String, Hashtable<String, Double>>();
		SesameJenaSelectWrapper costWrapper = new SesameJenaSelectWrapper();
		costWrapper.setQuery(costQuery);
		costWrapper.setEngine(engine);
		costWrapper.executeQuery();
		
		costDataVarNames = costWrapper.getVariables();
		costDataLength = costDataVarNames.length - 1;
		while(costWrapper.hasNext()) {
			SesameJenaSelectStatement sjss = costWrapper.next();
			String sys = sjss.getVar(costDataVarNames[0]).toString();
			Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
			costHash.put(sys, innerHash);
			for(int i = 1; i < costDataVarNames.length; i++) {
				innerHash.put(costDataVarNames[i], (Double) sjss.getVar(costDataVarNames[i]));
			}
		}
		
		return costHash;
	}


	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
	@Override
	public void setQuery(String query) 
	{
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else
		{
			logger.info("Query " + query);

			String addConditions = "";
			String addBindings = "";

			int semicolon1 = query.indexOf(";");
			int semicolon2 = query.indexOf(";",semicolon1+1);
			int semicolon3 = query.indexOf(" SELECT",semicolon2+1);

			String probabilityUserResponse = query.substring(0,semicolon1);
			String interfaceUserResponse = query.substring(semicolon1+1,semicolon2);
			String archiveUserResponse = query.substring(semicolon2+1, semicolon3);
			query = query.substring(semicolon3+1);

			if(probabilityUserResponse.equals("All"))
			{
				// do nothing
			} else {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} ");
				if(probabilityUserResponse.equals("High"))
				{
					addBindings = addBindings.concat("BINDINGS ?Probability {('High')('Question')}");
				} else {
					addBindings = addBindings.concat("BINDINGS ?Probability {('Low')('Medium')('Medium-High')}");
				}
			}


			if(interfaceUserResponse.equals("All"))
			{
				// do nothing
			} else if(interfaceUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }");
			} else if(interfaceUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }");
			}

			if(archiveUserResponse.equals("All"))
			{
				// do nothing
			} else if(archiveUserResponse.equals("Yes"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'Y' }");
			} else if(archiveUserResponse.equals("No"))
			{
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'N' }");
			}

			String retString = "";
			retString = retString.concat(query.substring(0,query.lastIndexOf("}"))).concat(addConditions).concat("} ").concat("ORDER BY ?System ").concat(addBindings);
			logger.info("New Query " + retString);
			this.query = retString;
		}
	}
}
