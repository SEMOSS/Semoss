package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IScriptReactor;

public abstract class AbstractReactor implements IScriptReactor {
	
	// every single thing I am listening to
	// I need
	//guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now
	// 
	protected String [] whatIReactTo = null;
	String whoAmI = null;
	Hashtable <String, Object> myStore = new Hashtable <String, Object>();
	Vector <String> replacers = new Vector<String>();
	
	
	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return whatIReactTo;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		// I need to find a way to do the save data routine here
		saveData(key, value); // need to re align some of the other pieces here
	}

	@Override
	public abstract Iterator process() ;
	
	// null method make sure you over ride
	public String[] getValues2Sync(String input)
	{
		return null;
	}

	@Override
	public void addReplacer(String pattern, Object value) {
		// TODO Auto-generated method stub
		replacers.add(pattern);
		myStore.put(pattern, value);
	}

	@Override
	public void removeReplacer(String pattern) {
		// TODO Auto-generated method stub
		replacers.remove(pattern);
		myStore.remove(pattern);
	}

	@Override
	public Object getValue(String key) {
		// TODO Auto-generated method stub
		return myStore.get(key);
	}
	
	public void saveData(String key, Object value)
	{
		if(isKeyInTracker(key))
		{
			// ok which means this is there
			int count = 1;
			if(myStore.containsKey(key+"_COUNT")) // somebody should slap me for not using constants
				count = (Integer)myStore.get(key+"_COUNT") + 1;
			
			if(isKeyInTracker(key + "_" + count))
				//myStore.put(whatICallThisInMyWorld.get(key  + "_" + count), data);
				myStore.put(key  + "_" + count, value);
			//else if(count != 1)
			//	dataKeeper.put("COL_DEF_" + count, node.getColname());
			// stack everything into a vector
				Vector valVector = new Vector();
			if(myStore.containsKey(key))
				valVector = (Vector)myStore.get(key);
			if(!valVector.contains(value))
				valVector.add(value);
			myStore.put(key, valVector);
			// random for now
			myStore.put(key+"_COUNT",count);
			//dataKeeper.put((node + "").trim(), dataKeeper.keys());			
		}
	}

	private boolean isKeyInTracker(String key)
	{
		boolean found = false;
		for(int paramIndex = 0;paramIndex < whatIReactTo.length;paramIndex++)
		{
			if(key.equals(whatIReactTo[paramIndex]))
			{
				found = true;
				break;
			}
		}
		return found;
	}
	
	protected void modExpression()
	{
		String curExpression = (String)myStore.get(whoAmI);
		System.out.println("Replacers.. " + replacers);
		for(int ripIndex = 0;ripIndex < replacers.size();ripIndex++)
		{
			String tobeReplaced = replacers.elementAt(ripIndex);
			if(myStore.containsKey(tobeReplaced))
			{
				String replacedBy = myStore.get(tobeReplaced) + "";
				curExpression = curExpression.replace(tobeReplaced, replacedBy);
			}			
		}
		myStore.put("MOD_" + whoAmI, curExpression);
	}
	
	@Override
	public void put(String key, Object value) {
		// TODO Auto-generated method stub
		myStore.put(key, value);
		
	}

	protected Iterator getTinkerData(Vector <String> columns, TinkerFrame frame)
	{
		if(columns != null && columns.size() <= 1)
			columns.add(columns.get(0));
		// now I need to ask tinker to build me something for this
		Iterator iterator = frame.getIterator(columns);
		if(iterator.hasNext())
		{
			//System.out.println(iterator.next());
		}
		return iterator;
	}
	
	// move to utility
	protected String[] convertVectorToArray(Vector <String> columns)
	{
		// convert this column array
		String [] colArr = new String[columns.size()];
		for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
		return colArr;
	}


	
}
