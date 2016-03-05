package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.engine.api.IScriptReactor;

public class ExprReactor extends AbstractReactor {
	
	// every single thing I am listening to
	// I need
	//guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	// 
	
	public ExprReactor()
	{
		String [] thisReacts = {TokenEnum.COL_DEF, TokenEnum.DECIMAL, TokenEnum.NUMBER};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.EXPR_TERM;
	}
	
	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		
		// I see this is the logic to execute when I need the iterator
		// come to think of it.. 
		// if it came here
		// there is no point doing anything
		// but I will ignore that for now
		if(myStore.get(myStore.get(TokenEnum.EXPR_TERM)) == null)
		{			
			modExpression();
			String nodeStr = (String)myStore.get(whoAmI);
			myStore.put(nodeStr, myStore.get("MOD_" + whoAmI));
			System.out.println("Printing the myStore..  " + myStore);
			
		}
			// this is where I would create the iterator to do various things
		return null;
	}
	
}
