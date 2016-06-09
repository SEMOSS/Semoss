package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.ExpressionReducer;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.PKQLRunner.STATUS;

public class InputReactor extends AbstractReactor {
	
	// this is responsible for doing the replacing of variables in the script with their actual objects
	// the map of vars to objects will either sit on runner or in insight
	// for now we will put it on the insight since runner isn't singleton wrt insight
	
	public InputReactor()
	{
		String [] thisReacts = {PKQLEnum.VAR_TERM, PKQLEnum.EXPR_TERM, PKQLEnum.API};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLReactor.INPUT.toString();
	}

	@Override
	public Iterator process() {

		// get the list of options
		// it is either an api (wrapper)
		// or csv col (list)
		// etc.
		// just iterate through and add to options
		// then set back into mystore
		List<Object> options = new Vector<Object>();
		if(myStore.get(PKQLEnum.API) != null)
		{			
			Iterator<IHeadersDataRow> it = (Iterator) myStore.get(PKQLEnum.API);
			while(it.hasNext()){
				IHeadersDataRow ss = it.next();
				options.add(ss.getValues()[0]);
			}
		}
		else if(myStore.get(PKQLEnum.ROW_CSV) != null)
		{			
			options = (List) myStore.get(PKQLEnum.ROW_CSV);
		}
		myStore.put("options", options);
		return null;
	}
	
	
}
