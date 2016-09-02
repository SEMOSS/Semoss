package prerna.sablecc;

import java.util.Iterator;

public class ExprRowReactor extends AbstractReactor {

	public ExprRowReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.EXPLAIN };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.EXPR_ROW;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		if (myStore.containsKey(PKQLEnum.EXPR_ROW)) {
			// Object[] row = (Object[]) myStore.get(PKQLEnum.EXPR_ROW);
			// Object[] translatedRow = new Object[row.length];
			// for(int i = 0; i < row.length ; i++){
			// Object cell = row[i];
			// translatedRow[i] = myStore.get(cell);
			// }
			myStore.put(nodeStr, myStore.get(PKQLEnum.EXPR_ROW));
		}

		return null;
	}

	@Override
	public String explain() {
		String msg = "";
//		msg += "ExprRowReactor";
		return msg;
	}

}
