package prerna.sablecc;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.expressions.IExpressionBuilder;

@Deprecated
public abstract class AbstractReactor implements IScriptReactor {

	// every single thing I am listening to
	// I need
	//guns.. lot and lots of guns
	// COL_DEF
	// DECIMAL
	// NUMBER - listening just for the fun of it for now

	protected String insightId;
	protected String[] whatIReactTo = null;
	protected String whoAmI = null;
	protected HashMap <String, Object> myStore = new HashMap <String, Object>();
	protected Vector <String> replacers = new Vector<String>();
	
	private PKQLRunner runner = null;

	//added for PKQL command definition to be sent to the FE
//	private String[] pkqlDefinition = {"title", "pkqlCommand", "description", "showMenu", "pinned", "input", "console"};
//	private HashMap <String, Object> pkqlMetaData = null;

	private String lastKeyAdded = null;
	
	
	// MAHER TEST CODE
	// IGNORE
	
//	/*
//	 * These two hashtables will be used to store the values of internal pkql components that do not require
//	 * a reactor.  Any operation that does not work on the dataframe should not require a reactor.
//	 * 
//	 * Example: a COL_CSV with the following expression "[c:Title, c:Studio, c:Genre]"
//	 * 
//	 * componentsToValue would contain 			{ "COL_CSV" -> ["Title", "Studio", "Genre] }
//	 * expressionStrToValue would contain		{ "[c:Title, c:Studio, c:Genre]" -> ["Title", "Studio", "Genre] }
//	 * 
//	 * The expressionStrToValue would be used in situations where modification of the pkql expression needs to occur.
//	 * This is important in situations where there is the possibility of infinitely many  nested expressions (math within math within math)
//	 * 
//	 * The componentsToValue would be used in situations where we know exactly what the inputs should be and can predictably
//	 * and accurately grab values where necessary.
//	 * This significantly simplifies the logic involved in using a QueryApiReactor which knows that the COL_CSV
//	 * are the selectors for the query
//	 * 
//	 */
//	protected Map<String, Object> componentToValue = new Hashtable<String, Object>();
//	protected Map<String, Object> expressionStrToValue = new Hashtable<String, Object>();
//	
//	/**
//	 * Add a components and its value into the current reactor
//	 * @param componenet			The type of component -> COL_DEF, COL_CSV, etc.
//	 * @param value					The value of the component -> Title, [Title, Studio, Genre]
//	 */
//	public void addComponentValue(String componenet, Object value) {
//		int counter = 1;
//		String valueToStore = componenet;
//		while(componentToValue.containsKey(valueToStore)) {
//			valueToStore = componenet + "_" + counter;
//			counter++;
//		}
//		componentToValue.put(valueToStore, value);
//	}
//	
//	/**
//	 * Add a expression to its value into the current reactor
//	 * @param componenet			The type of component -> c:Title, [c:Title, c:Studio
//	 * @param value					The value of the component -> Title, [Title, Studio, Genre]
//	 */
//	public void addExpressionToValue(String componenet, Object value) {
//		expressionStrToValue.put(componenet, value);
//	}
	
	
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
			//			if(!valVector.contains(value)) // We need to hold on to duplicates in a flex selector row.. will this have downstream affects?
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
		Object curExpression = (String)myStore.get(whoAmI);

		curExpression = modExpression(curExpression);

		myStore.put("MOD_" + whoAmI, curExpression);

		return;
	}

	protected Object modExpression(Object curExpression){

		System.out.println("Replacers.. " + replacers);
		for(int ripIndex = replacers.size()-1;ripIndex > -1 ;ripIndex--)
		{
			String tobeReplaced = replacers.elementAt(ripIndex);
			if(myStore.containsKey(tobeReplaced))
			{
				Object replacedBy = myStore.get(tobeReplaced);
				if(curExpression.equals(tobeReplaced) || replacedBy instanceof Iterator || replacedBy instanceof IExpressionBuilder) {
					curExpression = replacedBy;
				} else{
					curExpression = (curExpression+"").replace(tobeReplaced, replacedBy+"");
				}
			}			
		}
		return curExpression;
	}

	@Override
	public void put(String key, Object value) {
		myStore.put(key, value);
		this.lastKeyAdded = key;
	}

	protected Iterator getTinkerData(List <String> columns, ITableDataFrame frame, boolean dedup) {
		return getTinkerData(columns, frame, null, dedup);
	}

	protected Iterator getTinkerData(List <String> columns, ITableDataFrame frame, Map<String, Object> valMap, boolean dedup) {
		//		if(columns != null && columns.size() <= 1)
		//			columns.add(columns.get(0));
		// now I need to ask tinker to build me something for this

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(AbstractTableDataFrame.SELECTORS, columns);

		if(valMap!=null){
			options.put(AbstractTableDataFrame.TEMPORAL_BINDINGS, valMap);
		}
		if(dedup) {
			options.put(AbstractTableDataFrame.DE_DUP, dedup);
		}

		Iterator iterator = frame.iterator();
		if(iterator.hasNext())
		{
			//System.out.println(iterator.next());
		}
		return iterator;
	}

	protected Iterator<List<Object[]>> getUniqueScaledData(String instance, List<String> columns, ITableDataFrame frame) {
		return frame.scaledUniqueIterator(instance, columns);
	}

	// move to utility
	protected String[] convertVectorToArray(Vector <String> columns)
	{
		// convert this column array
		String [] colArr = new String[columns.size()];
		for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
		return colArr;
	}

	public String generateExplain(String template, HashMap<String, Object> values) {
		String msg = "";
		StringWriter writer = new StringWriter();
		MustacheFactory mf = new DefaultMustacheFactory();
		try {
			Mustache mustache = mf.compile(new StringReader(template), (String) values.get("whoAmI"));
			mustache.execute(writer, values);
			writer.flush();
			msg = writer.toString();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return msg;
	}

	@Override
	public String getLastStoredKey() {
		return this.lastKeyAdded;
	}
	
	@Override
	public Object remove(String key) {
		return this.myStore.remove(key);
	}
	
	@Override
	public Object removeLastStoredKey() {
		return this.myStore.remove(this.lastKeyAdded);
	}
	
	@Override
	public Set<String> getKeys() {
		return myStore.keySet();
	}
	
	public void emit(String message)
	{
		PKQLRunner runner = (PKQLRunner)myStore.get("PKQL_RUNNER");
		//runner.emit(whoAmI + " : " + message);
		// eventually I should do this with thread
	}
	
	@Override
	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}
}
