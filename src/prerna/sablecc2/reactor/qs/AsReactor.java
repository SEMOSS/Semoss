package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.query.querystruct.QueryStruct2;

public class AsReactor extends AbstractQueryStructReactor {

	@Override
	protected QueryStruct2 createQueryStruct() {
		//add the inputs from the store as well as this operation
		// first is all the inputs
		// really has one job pick the parent.. 
		// replace the as Name
		// the as name could come in as an array too
		// for now I will go with the name
		List<String> aliasInput = curRow.getAllColumns();
		
		if(this.parentReactor != null && aliasInput != null && !aliasInput.isEmpty()) {
			// get the columns on as
			parentReactor.setAs(aliasInput.toArray(new String[]{}));
		}
		
		return qs;
	}
}
