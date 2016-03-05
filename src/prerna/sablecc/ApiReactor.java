package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.impl.rdbms.RDBMSQueryAPI;

public class ApiReactor extends AbstractReactor {
	
	
	public ApiReactor()
	{
		String [] thisReacts = {TokenEnum.COL_CSV, TokenEnum.FILTER, TokenEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.API;
	}
	

	@Override
	public Iterator process() {
		
		System.out.println("Processed.. " + myStore);
		// TODO Auto-generated method stub
		/*
		 * Come to you in a min
		 * 
		 */
			//System.out.println("Will pull the data on this one and then make the calls to add to other things");
			
			
			Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
			Vector <String> tinkerSelectors = new Vector<String>();
			
			String engine = (String)myStore.get("ENGINE");
			String insight = (String)myStore.get("INSIGHT");
			
			// I need to instantiate the engine here
			// for now hard coding it
			IApi qapi = new RDBMSQueryAPI();
			qapi.set("ENGINE", engine);
			
			QueryStruct qs = new QueryStruct();
			
			if(myStore.containsKey(TokenEnum.COL_CSV) && ((Vector)myStore.get(TokenEnum.COL_CSV)).size() > 0)
				qapi.set(TokenEnum.SELECTOR, myStore.get(TokenEnum.COL_CSV));
			if(myStore.containsKey(TokenEnum.FILTER) && ((Vector)myStore.get(TokenEnum.FILTER)).size() > 0)
				qapi.set(TokenEnum.FILTER, myStore.get(TokenEnum.FILTER));
			if(myStore.containsKey(TokenEnum.JOINS) && ((Vector)myStore.get(TokenEnum.JOINS)).size() > 0)
				qapi.set("JOINS",myStore.get(TokenEnum.JOINS));
			
			Iterator thisIterator = qapi.process();
			String nodeStr = (String)myStore.get(whoAmI);

			myStore.put(nodeStr, thisIterator);

			// eventually I need this iterator to set this back for this particular node
		return null;
	}

}
