package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;

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
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		TinkerFrame frame = (TinkerFrame) myStore.get("G");
		
		String [] joinCols = null;
		Iterator it = null;
		String newCol = (String)myStore.get(TokenEnum.COL_DEF + "_1");
		
		if(myStore.containsKey(TokenEnum.API))
		{
			joinCols = (String [])myStore.get(TokenEnum.API + "_" + TokenEnum.COL_CSV);
			it = (Iterator)myStore.get(TokenEnum.API);
		}
		else
		{
			// ok this came in as an expr term
			// I need to do the iterator here
			System.err.println(myStore.get(TokenEnum.EXPR_TERM));
			
			// ok.. so it would be definitely be cool to pass this to an expr script right now and do the op
			// however I dont have this shit
			String expr = (String) myStore.get(TokenEnum.EXPR_TERM);
			
			Vector <String> cols = (Vector <String>)myStore.get(TokenEnum.COL_DEF);
			
			
			cols.remove(newCol);
			it = getTinkerData(cols, frame);
			joinCols = convertVectorToArray(cols);
		}

		frame.connectTypes(joinCols[0], newCol);
		
		while(it.hasNext()) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			row.put(newCol, it.next());
			for(int i = 0; i < joinCols.length; i++) {
				if (it instanceof ExpressionIterator) {
					row.put(joinCols[i], ((ExpressionIterator)it).getOtherBindings().get(joinCols[i]));
				}
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
		return null;
	}


}
