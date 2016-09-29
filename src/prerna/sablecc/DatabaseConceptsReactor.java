package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.nameserver.MasterDatabaseQueries;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabaseConceptsReactor extends AbstractReactor {

	public DatabaseConceptsReactor() {
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATABASE_CONCEPTS;
	}
	
	@Override
	public Iterator process() {
		
		Object engineName = myStore.get(PKQLEnum.WORD_OR_NUM);
		
		String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

		if(engines.startsWith((String) engineName) || engines.contains(";"+engineName+";") || engines.endsWith(";"+engineName)){
			IEngine masterDB = (IEngine) DIHelper.getInstance().getLocalProp("LocalMasterDatabase");

			Map<String, Object> result = new HashMap<String, Object>();
			List<String> conceptsList = new ArrayList<String>();
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(masterDB, MasterDatabaseQueries.GET_ALL_KEYWORDS_AND_ENGINES2);
			
			String [] names = wrapper.getDisplayVariables();
			while(wrapper.hasNext())
			{
				ISelectStatement iss = wrapper.next();
				
				String engine = iss.getVar(names[0]) + "";
				if(!engine.equals(engineName))
					continue;
				String conceptURI = iss.getRawVar(names[1]) + ""; // logical name				
				String instanceName = Utility.getInstanceName(conceptURI);
				conceptsList.add(instanceName);
				result.put("list", conceptsList);				
			}
			
			myStore.put("database.concepts", result);
			myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		}
		
		
		
		return null;
	}

}
