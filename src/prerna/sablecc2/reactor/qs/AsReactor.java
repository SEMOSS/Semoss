package prerna.sablecc2.reactor.qs;

import java.util.Enumeration;
import java.util.List;

import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;

public class AsReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		 //add the inputs from the store as well as this operation
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
		}		
		
		//TODO create as for select Reactor
		NounMetadata noun = new NounMetadata(asNames, PkslDataTypes.ALIAS);
		planner.addVariable("AS", noun);
		
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		return qs;
	}
}
