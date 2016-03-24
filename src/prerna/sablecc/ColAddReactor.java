package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;
import prerna.engine.api.ISelectStatement;

public class ColAddReactor extends AbstractReactor {
	
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public ColAddReactor()
	{
		String [] thisReacts = {TokenEnum.COL_DEF,TokenEnum.COL_DEF + "_1", TokenEnum.API}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.COL_ADD;
		
		
		// this is the point where I specify what are the child values required for various input
		String [] dataFromExpr = {TokenEnum.COL_DEF};
		values2SyncHash.put(TokenEnum.EXPR_TERM, dataFromExpr);

		String [] dataFromApi = {TokenEnum.COL_CSV};
		values2SyncHash.put(TokenEnum.API, dataFromApi);

	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		TinkerFrame frame = (TinkerFrame) myStore.get("G");
		
		String [] joinCols = null;
		Iterator it = null;
		String newCol = (String)myStore.get(TokenEnum.COL_DEF + "_1");
		
		// ok this came in as an expr term
		// I need to do the iterator here
		System.err.println(myStore.get(TokenEnum.EXPR_TERM));
		
		// ok.. so it would be definitely be cool to pass this to an expr script right now and do the op
		// however I dont have this shit
		String expr = (String) myStore.get(TokenEnum.EXPR_TERM);
		Vector <String> cols = (Vector <String>)myStore.get(TokenEnum.COL_DEF);
		it = getTinkerData(cols, frame);
		joinCols = convertVectorToArray(cols);
		Object value = myStore.get(expr);
		if(value == null) value = myStore.get(TokenEnum.API);
		
		if (value instanceof Iterator) {
			it = (Iterator)value;
		} else {
			it = new ExpressionIterator(it, joinCols, value.toString());
		}

		frame.connectTypes(joinCols[0], newCol);
		
		while(it.hasNext()) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			Object newVal = it.next();
			if ((newVal instanceof List) && ((List)newVal).size() == 1)
				row.put(newCol, ((List)newVal).get(0));
			else {
				row.put(newCol, newVal);
			}
			for(int i = 0; i < joinCols.length; i++) {
				if (it instanceof ExpressionIterator) {
					row.put(joinCols[i], ((ExpressionIterator)it).getOtherBindings().get(joinCols[i]));
				}
			}
			if (newVal instanceof ISelectStatement) {
				System.out.println(((ISelectStatement)newVal).getPropHash());
			}
			frame.addRelationship(row, row);
		}
		// I have no idea what we are trying to do here 
		// need to ask kevin
		frame.setTempExpressionResult("SUCCESS");
		
		return null;
	}

	// gets all the values to synchronize for this 
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}


}
