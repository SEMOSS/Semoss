package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;
import prerna.engine.api.ISelectStatement;

public class ImportDataReactor extends AbstractReactor {
	
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public ImportDataReactor()
	{
		String [] thisReacts = {TokenEnum.API, TokenEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.IMPORT_DATA;

		String [] dataFromApi = {TokenEnum.COL_CSV};
		values2SyncHash.put(TokenEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		TinkerFrame frame = (TinkerFrame) myStore.get("G");
		
		Iterator it = null;
		Object value = myStore.get(TokenEnum.API);
		
		it = (Iterator)value;

//		frame.connectTypes(joinCols[0], newCol); 
//		Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
//		Map<String, Set<String>> cleanedEdgeHash = this.mergeQSEdgeHash(edgeHash, engine);
		while(it.hasNext()){
			ISelectStatement ss = (ISelectStatement) it.next();
			System.out.println(((ISelectStatement)ss).getPropHash());
			frame.addRelationship(ss.getPropHash(), ss.getRPropHash(), new HashMap());
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
