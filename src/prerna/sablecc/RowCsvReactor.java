package prerna.sablecc;

import java.util.Iterator;

public class RowCsvReactor extends AbstractReactor {

	public RowCsvReactor() {
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM, PKQLEnum.EXPLAIN };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.ROW_CSV;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		if (myStore.containsKey(PKQLEnum.WORD_OR_NUM)) {
			myStore.put(nodeStr, myStore.get(PKQLEnum.WORD_OR_NUM));
		}

		return null;
	}

	@Override
	public String explain() {
		String msg = "";
		msg += "RowCsvReactor";
		return msg;
	}

}
