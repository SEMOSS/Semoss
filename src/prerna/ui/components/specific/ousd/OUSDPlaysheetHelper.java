package prerna.ui.components.specific.ousd;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

public final class OUSDPlaysheetHelper {

	protected static final Logger LOGGER = LogManager.getLogger(OUSDPlaysheetHelper.class.getName());
	
	private OUSDPlaysheetHelper(){
		
	}

	public static Map<String, Double> getBudgetData(){
		String costDbName = null;
		IEngine costDb = null;
		
		costDbName = DIHelper.getInstance().getProperty(OUSDConstants.COST_ENGINE_NAME);
		costDb = (IEngine) DIHelper.getInstance().getLocalProp(costDbName);
		
		boolean costDbExists = costDb!=null;
		LOGGER.info("Cost db " + costDbName + " exists ::::  " + costDbExists);
		if(!costDbExists){
			return null;
		}

		String budgetQuery = DIHelper.getInstance().getProperty(OUSDConstants.SYSTEM_SUSTAINMENT_BUDGET_QUERY);
		LOGGER.info("RUNNING COST QUERY::::::: db is " + costDbName + " and query is " + budgetQuery);
		Map<String, Double> sysBudgets =  new HashMap<String, Double>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(costDb, budgetQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			sysBudgets.put((String) iss.getVar(wNames[0]), (Double) iss.getVar(wNames[1]));
		}
		LOGGER.info("Got cost data :: " + sysBudgets.toString());

		return sysBudgets;
	}
}
