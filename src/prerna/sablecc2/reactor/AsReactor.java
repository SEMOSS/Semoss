package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;

public class AsReactor extends SampleReactor {
	
	public Object Out()
	{
		updatePlan();
		System.out.println("Out of as reactor");
		return parentReactor;
	}

	public void updatePlan()
	{
		// add the inputs from the store as well as this operation
		// first is all the inputs
		// really has one job pick the parent.. 
		// replace the as Name
		// the as name could come in as an array too
		// for now I will go with the name
		Enumeration <String> keys = store.nounRow.keys();

		String [] asNames = null;
		while(keys.hasMoreElements())
		{
			String singleKey = keys.nextElement();
			GenRowStruct struct = store.nounRow.get(singleKey);
			Vector <String> inputs = struct.getAllColumns(); // ideally this should get only one column for now

			asNames = new String[1];
			asNames[0] = inputs.elementAt(0).trim();
		}
		if(this.parentReactor != null && asNames != null)
		{
			// get the columns on as
			parentReactor.setAs(asNames);
		}		
	}
}