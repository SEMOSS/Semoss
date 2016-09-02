package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;

public class ColCsvReactor extends AbstractReactor {
	
	public ColCsvReactor()
	{
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.EXPLAIN};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_CSV;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		if(myStore.containsKey(PKQLEnum.COL_DEF))
		{
			myStore.put(nodeStr, myStore.get(PKQLEnum.COL_DEF));
		}

		return null;
	}

	@Override
	public String explain() {
		// values that are added to template engine
		HashMap<String, Object> values = new HashMap<String, Object>();
		values.put("columns", myStore.get("COL_DEF"));
	    values.put("whoAmI", whoAmI);
	    String template = " {{columns}}.";
		return generateExplain(template, values);
	}

}
