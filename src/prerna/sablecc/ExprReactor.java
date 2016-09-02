package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;

public class ExprReactor extends AbstractReactor {

	// every single thing I am listening to
	// I need
	// guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	//

	public ExprReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.EXPLAIN };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.EXPR_TERM;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub

		// I see this is the logic to execute when I need the iterator
		// come to think of it..
		// if it came here
		// there is no point doing anything
		// but I will ignore that for now
		if (myStore.get(PKQLEnum.EXPR_TERM) != null) {
			modExpression();
			String nodeStr = (String) myStore.get(whoAmI);
			myStore.put(nodeStr, myStore.get("MOD_" + whoAmI));
			System.out.println("Printing the myStore..  " + myStore);

		}
		// this is where I would create the iterator to do various things

		return null;
	}

	@Override
	public String explain() {
		String msg = "";
		HashMap<String, Object> values = new HashMap<String, Object>();
		if (!myStore.containsKey(PKQLEnum.EXPLAIN)) {
		    String template = " using {{operation}}";
			values.put("operation", myStore.get("MOD_EXPR_TERM"));
		    values.put("whoAmI", whoAmI);
		    msg += generateExplain(template, values);
		}
		return msg;
	}

}
