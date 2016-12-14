package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
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

	private static String fromColVal = PKQLEnum.COL_DEF + "_1";
	private static String toColVal = PKQLEnum.COL_DEF + "_2";

	public ColWhereReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF, fromColVal, toColVal, PKQLEnum.ROW_CSV, PKQLEnum.VAR_TERM, PKQLEnum.API, PKQLEnum.RAW_API };
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
		// put the from column
		finalHash.put(PKQLEnum.FROM_COL, myStore.get(fromColVal));
		
		// put the to column
		// not there when this is for a variable... i think
		if (myStore.containsKey(toColVal)) {
			finalHash.put(PKQLEnum.TO_COL, myStore.get(toColVal));
		}
		
		// based on input type, put the to_data
		if (myStore.containsKey(PKQLEnum.ROW_CSV)) {
			finalHash.put("TO_DATA", myStore.get(PKQLEnum.ROW_CSV));
		} else if (myStore.containsKey(PKQLEnum.VAR_TERM)) {
			finalHash.put("TO_DATA", myStore.get(PKQLEnum.VAR_TERM));
		} else if(myStore.containsKey(PKQLEnum.API)) {
			// flush out iterator
			finalHash.put("TO_DATA", flushOutIterator( (Iterator<IHeadersDataRow>) myStore.get(PKQLEnum.API)));
		} else if(myStore.containsKey(PKQLEnum.RAW_API)) {
			// flush out iterator
			finalHash.put("TO_DATA", flushOutIterator( (Iterator<IHeadersDataRow>) myStore.get(PKQLEnum.RAW_API)));
		}
		
		// put in the comparator
		finalHash.put("COMPARATOR", myStore.get("COMPARATOR"));
		myStore.put(nodeStr, finalHash);

		return null;
	}
	
	public List<Object> flushOutIterator(Iterator<IHeadersDataRow> it) {
		// ASSUMPTION - ONLY ONE COLUMN BEING RETURNED!!!
		List<Object> values = new Vector<Object>();
		while(it.hasNext()) {
			values.add(it.next().getValues()[0]);
		}
		return values;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return null;
	}

}
