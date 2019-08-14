package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Set;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AsReactor extends AbstractReactor {
	
	public Object Out()
	{
		return parentReactor;
	}
	
	public NounMetadata execute() {
		String alias = (String) curRow.get(0);
		NounMetadata noun = new NounMetadata(alias, PixelDataType.ALIAS);
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
		Set<String> keys = store.nounRow.keySet();

		String [] asNames = null;
		for(String singleKey : keys) {
			GenRowStruct struct = store.nounRow.get(singleKey);
			List<String> inputs = struct.getAllStrValues(); // ideally this should get only one column for now
			asNames = new String[1];
			asNames[0] = inputs.get(0).trim();
		}
		
		if(this.parentReactor != null && asNames != null) {
			// get the columns on as
			parentReactor.setAs(asNames);
			
			NounMetadata asNoun = new NounMetadata(parentReactor, PixelDataType.LAMBDA);
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