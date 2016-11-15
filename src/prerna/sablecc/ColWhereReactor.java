package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.ExpressionReducer;
import prerna.sablecc.meta.IPkqlMetadata;

public class ColWhereReactor extends AbstractReactor {

	// every single thing I am listening to
	// I need
	// guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	// It is almost like this will always react only to
	// EXPR_TERM
	// oh wait.. array of expr_term
	// unless of course someone is trying to just sum the number

	Vector<String> replacers = new Vector<String>();
	ExpressionReducer red = null;

	public ColWhereReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.COL_DEF + "_1", PKQLEnum.COL_DEF + "_2", PKQLEnum.ROW_CSV, PKQLEnum.VAR_TERM };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.WHERE;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// everytime I see this..
		// I need to parse it out and do something

		String nodeStr = (String) myStore.get(whoAmI);

		System.out.println("COL WHERE  my Store" + myStore);

		// just for the sake of clarity..
		// I should take the stuff out and get it set right
		Hashtable<String, Object> finalHash = new Hashtable<String, Object>();
		finalHash.put(PKQLEnum.FROM_COL, myStore.get(whatIReactTo[1]));
		if (myStore.containsKey(whatIReactTo[2]))
			finalHash.put(PKQLEnum.TO_COL, myStore.get(whatIReactTo[2]));
		if (myStore.containsKey(whatIReactTo[3]))
			finalHash.put("TO_DATA", myStore.get(whatIReactTo[3]));
		if (myStore.containsKey(whatIReactTo[4]))
			finalHash.put("TO_DATA", myStore.get(whatIReactTo[4]));
		finalHash.put("COMPARATOR", myStore.get("COMPARATOR"));
		myStore.put(nodeStr, finalHash);

		// create the iterator and keep it moving
		// this is where I do the out math fun

		// this is where I would create the iterator to do various things

		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
