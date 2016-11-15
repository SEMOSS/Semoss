package prerna.sablecc;

import java.util.Iterator;

import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Constants;

public class VarReactor extends AbstractReactor {

	// this is responsible for doing the replacing of variables in the script
	// with their actual objects
	// the map of vars to objects will either sit on runner or in insight
	// for now we will put it on the insight since runner isn't singleton wrt
	// insight

	public VarReactor() {
		String[] thisReacts = { PKQLEnum.VAR_TERM, PKQLEnum.EXPR_TERM, PKQLEnum.INPUT, Constants.ENGINE,
				PKQLEnum.OPEN_DATA, PKQLEnum.VAR_PARAM, "VAR"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLReactor.VAR.toString();
	}

	@Override
	public Iterator process() {

		// get the expression term that the var is to be set as
		// expression should have already been moded
		// set it in my store
		if (myStore.get(PKQLEnum.EXPR_TERM) != null) {
			modExpression();
			String nodeStr = (String) myStore.get(whoAmI);
			myStore.put(nodeStr, myStore.get("MOD_" + whoAmI));
			System.out.println("Printing the myStore..  " + myStore);

		}
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
