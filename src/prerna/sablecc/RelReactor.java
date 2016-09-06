//package prerna.sablecc;
//
//import java.util.Hashtable;
//import java.util.Iterator;
//import java.util.Vector;
//
//import prerna.ds.ExpressionReducer;
//import prerna.ds.TinkerFrame;
//import prerna.engine.api.IScriptReactor;
//
//public class RelReactor extends AbstractReactor {
//	
//	// every single thing I am listening to
//	// I need
//	//guns.. lot and lots of guns
//	// COL_DEF
//	// DECIMAL
//	// NUMBER - listening just for the fun of it for now
//	// It is almost like this will always react only to 
//	// EXPR_TERM
//	// oh wait.. array of expr_term
//	// unless of course someone is trying to just sum the number
//	
//	Vector <String> replacers = new Vector<String>();
//	ExpressionReducer red = null;
//	
//	public RelReactor()
//	{
//		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.COL_DEF+ "_1", PKQLEnum.COL_DEF+ "_2", PKQLEnum.REL_TYPE};
//		super.whatIReactTo = thisReacts;
//		super.whoAmI = PKQLEnum.REL_DEF;
//	}
//
//	@Override
//	public Iterator process() {
//		// TODO Auto-generated method stub
//		// everytime I see this.. 
//		// I need to parse it out and do something
//		
//		String nodeStr = (String)myStore.get(whoAmI);
//		
//		System.out.println("REL DEF my Store"  + myStore);
//		
//		// just for the sake of clarity..
//		// I should take the stuff out and get it set right
//		Hashtable <String, Object> finalHash = new Hashtable<String, Object>();
//		finalHash.put(PKQLEnum.FROM_COL, myStore.get(whatIReactTo[1]));
//		if(myStore.containsKey(whatIReactTo[2]))
//			finalHash.put(PKQLEnum.TO_COL, myStore.get(whatIReactTo[2]));
//		finalHash.put(PKQLEnum.REL_TYPE, myStore.get(PKQLEnum.REL_TYPE));
//		myStore.put(nodeStr, finalHash);
//
//		// create the iterator and keep it moving
//		// this is where I do the out math fun
//		
//		// this is where I would create the iterator to do various things
//		return null;
//	}
//	
//	
//}
