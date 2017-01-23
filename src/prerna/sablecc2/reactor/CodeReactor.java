package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.GenRowStruct.COLUMN_TYPE;

public class CodeReactor extends SampleReactor {
	
	public Object Out()
	{
		updatePlan();
		System.out.println("Out of as reactor");
		return parentReactor;
	}

	public void updatePlan()
	{
		// set the code in parent
		// done
		if(parentReactor != null)
			parentReactor.setProp("CODE", this.signature);
	}
}