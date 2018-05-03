package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;

public class AsReactor extends AbstractQueryStructReactor {

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		//add the inputs from the store as well as this operation
		// first is all the inputs
		// really has one job pick the parent.. 
		// replace the as Name
		// the as name could come in as an array too
		// for now I will go with the name
		List<String> aliasInput = curRow.getAllColumns();
		if(this.parentReactor != null && aliasInput != null && !aliasInput.isEmpty()) {
			// I need to make sure there are no __ since it causes issues
			int size = aliasInput.size();
			String[] aliasArray = new String[size];
			for(int i = 0; i < size; i++) {
				aliasArray[i] = aliasInput.get(i).replaceAll("_{2}", "_");
			}
			parentReactor.setAs(aliasArray);
		}
		
		return qs;
	}
}
