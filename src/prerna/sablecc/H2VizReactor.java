package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.expressions.sql.H2SqlExpressionGenerator;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;

public class H2VizReactor extends AbstractVizReactor {

	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public H2VizReactor() {
		String[] thisReacts = {PKQLEnum.WORD_OR_NUM, "TERM" };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.VIZ;
	}

	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) getValue("G");

		Vector<Object> selectors = (Vector<Object>) getValue("TERM");
		if(selectors == null || selectors.size() == 0) {
			// this is the case when user wants a grid of everything
			// we do not send back any data through the pkql
			// they get it in the getNextTableData call
			return null;
		}
		
		H2SqlExpressionIterator currIt = null;
		// we have at least one selector
		int size = selectors.size();
		Object term = selectors.get(0);
		if(term instanceof H2SqlExpressionIterator) {
			currIt = (H2SqlExpressionIterator) term;
		} else {
			// setting the expression and group by to empties/null
			// will result in a sql expression with just the column to return
			currIt = new H2SqlExpressionIterator(frame, null, null, new String[]{getValue(term.toString().trim()).toString()}, null);
		}
		
		for(int i = 1; i < size; i++) {
			Object nextTerm = selectors.get(i);
			currIt = H2SqlExpressionGenerator.combineExpressionsForMultipleReturns(frame, currIt, getValue(nextTerm.toString().trim()));
		}
		
		List<Object[]> data = new Vector<Object[]>();
		while(currIt.hasNext()) {
			data.add(currIt.next());
		}
		
		myStore.put("VizTableKeys", currIt.getHeaders());
		myStore.put("VizTableValues", data);
			
		return null;
	}
}
