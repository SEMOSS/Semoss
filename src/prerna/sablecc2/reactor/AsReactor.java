package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class AsReactor extends AbstractReactor {
	
	@Override
	public void In() {
		curNoun("all");	
	}
	
	public Object Out()
	{
		updatePlan();
		return parentReactor;
	}
	
	public Object execute() {
		String alias = (String)curRow.get(0);
		NounMetadata noun = new NounMetadata(alias, PkslDataTypes.ALIAS);
		return noun;
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
			List <String> inputs = struct.getAllColumns(); // ideally this should get only one column for now

			asNames = new String[1];
			asNames[0] = inputs.get(0).trim();
		}
		if(this.parentReactor != null && asNames != null)
		{
			// get the columns on as
			parentReactor.setAs(asNames);
			
			NounMetadata asNoun = new NounMetadata(parentReactor, PkslDataTypes.LAMBDA);
			this.planner.addVariable(asNames[0], asNoun);
		}		
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		// this is used primarily for the planner
		// we do not need to add these steps since 
		// the parent will automatically take these 
		// into consideration
		return null;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// this is used primarily for the planner
		// we do not need to add these steps since 
		// the parent will automatically take these 
		// into consideration
		return null;
	}
}