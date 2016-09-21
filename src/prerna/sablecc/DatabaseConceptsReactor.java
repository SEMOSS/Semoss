package prerna.sablecc;

import java.util.Iterator;
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
		//if(DIHelper.getInstance().getLocalProp((String) engineName) instanceof IEngine) {
			IEngine masterDB = (IEngine) DIHelper.getInstance().getLocalProp("LocalMasterDatabase");

			Map<String, Map<String, Map<String, String>>> result = new TreeMap<String, Map<String, Map<String, String>>>();
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(masterDB, MasterDatabaseQueries.GET_ALL_KEYWORDS_AND_ENGINES2);
			String [] names = wrapper.getDisplayVariables();
			while(wrapper.hasNext())
			{
				// this actually has three things within it
				// engineName
				// concept name - This will end up being physical
				// Logical Name - this is the concept name really
				ISelectStatement iss = wrapper.next();
				
				String engine = iss.getVar(names[0]) + "";
				if(!engine.equals(engineName))
					continue;
				String conceptURI = iss.getRawVar(names[1]) + ""; // logical name
				String physicalURI = iss.getRawVar(names[2]) + ""; // physical name
				
				System.out.println(engine + "<>" + conceptURI + "<>" + physicalURI);
				
				String instanceName = Utility.getInstanceName(conceptURI);
				// ok now comes the magic
				// step one do I have this engine
				Map<String, Map<String,String>> conceptMap = null;
				
				if(result.containsKey(engine))
					conceptMap = result.get(engine);
				else
					conceptMap = new TreeMap<String, Map<String, String>>();
				
				// not sure if I should check to see if this concept is there oh wait I should
				
				Map <String, String> physical = null;
				if(conceptMap.containsKey(instanceName))
					physical = conceptMap.get(instanceName);
				else
					physical = new TreeMap<String, String>();
				
				
				// now the phhysical
				physical.put("physicalName", instanceName);

				// put the physical back
				conceptMap.put(physicalURI, physical);
				result.put(engine, conceptMap);
				
				
			}
			
			myStore.put("database.concepts", result);
			myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		}
		//}
		
		
		
		return null;
	}

}
