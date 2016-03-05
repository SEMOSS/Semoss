package prerna.sablecc;

import java.util.Iterator;

public class ColCsvReactor extends AbstractReactor {
	
	public ColCsvReactor()
	{
		String [] thisReacts = {TokenEnum.COL_DEF};
		super.whatIReactTo = thisReacts;
		super.whoAmI = TokenEnum.COL_CSV;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		if(myStore.containsKey(TokenEnum.COL_DEF))
		{
			myStore.put(nodeStr, myStore.get(TokenEnum.COL_DEF));
		}
		return null;
	}

}
