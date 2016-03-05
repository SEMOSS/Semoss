package prerna.sablecc;

import java.util.Iterator;

public class RowCsvReactor extends AbstractReactor {
	
	public RowCsvReactor()
	{
		String [] thisReacts = {TokenEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.ROW_CSV;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		if(myStore.containsKey(TokenEnum.WORD_OR_NUM))
		{
			myStore.put(nodeStr, myStore.get(TokenEnum.WORD_OR_NUM));
		}
		return null;
	}

}
