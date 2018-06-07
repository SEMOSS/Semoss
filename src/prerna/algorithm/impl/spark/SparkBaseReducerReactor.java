//package prerna.algorithm.impl.spark;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Vector;
//
//import javax.script.Bindings;
//import javax.script.CompiledScript;
//import javax.script.ScriptContext;
//import javax.script.ScriptException;
//
//import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.ds.ExpressionReducer;
//import prerna.ds.TinkerFrame;
//import prerna.sablecc.PKQLEnum;
//import prerna.sablecc.PKQLRunner.STATUS;
//import prerna.sablecc.SparkMathReactor;
//
//public abstract class SparkBaseReducerReactor extends SparkMathReactor implements ExpressionReducer {
//
//	protected Iterator inputIterator = null;
//	protected String [] ids = null;
//	protected String script = null;
//	protected CompiledScript cs = null;
//	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
//	protected boolean errored = false;
//	protected java.lang.Object baseException = null;
//	protected Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
//	protected String propToGet = TinkerFrame.TINKER_NAME;
//	
//	protected Vector<String> groupBys = null;
//	protected Vector<String> columns = null;
//	protected String expr = null;
////	public abstract HashMap<HashMap<Object,Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it);
//
//	public void setData(Iterator inputIterator, String[] ids, String script) {
//		setData(inputIterator, ids,script, null);
//	}
//	
//	public void setData(Iterator inputIterator, String[] ids, String script, String prop ) {
//		// TODO Auto-generated method stub
//		this.inputIterator = inputIterator;
//		this.ids = ids;
//		this.script = script;
//		if(prop != null)
//			propToGet = prop;
//		
//		try {
//			cs = engine.compile(script);
//			
//		} catch (ScriptException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//	}
//	
//	public void setData(Map<Object, Object> options) {
//		groupBys = (Vector<String>)options.get("groupBys");
//		columns = (Vector<String>)options.get("columns");
//		expr = (String)options.get("expr");
//	}
//	
//	private void addDataToBindings()
//	{
//		Object daObject = inputIterator.next();
//		Map <Object, Object> row = null;
//		Object[] array = null;
//		Vertex vert = null;
//		Double doubleValue = null;
//		
//		if(daObject instanceof Map)
//			row = (Map<Object, Object>)daObject;
//		else if(daObject instanceof Object[])
//			array = (Object[])daObject;
//		else if(daObject instanceof Vertex)
//			vert = (Vertex) daObject;
//		else if(daObject instanceof Double)
//			doubleValue = (Double)daObject;
//		if(daObject != null && !errored)
//		{
//			// put things into the bindings first
//			if(row != null)
//			{
//				for(int colIndex = 0;colIndex < ids.length;colIndex++)
//				{
//					Vertex v = (Vertex)row.get(ids[colIndex]);
//					Object val = v.value(propToGet);
//					System.out.println("Values is " + val);
//					otherBindings.put(ids[colIndex], val);
//				}
//			}
//			else if(array != null)
//			{
//				for(int colIndex = 0;colIndex < ids.length;colIndex++)
//				{
//					Object val = array[colIndex];
//					System.out.println("Values is " + val);
//					otherBindings.put(ids[colIndex], val);
//				}
//			}
//			else if(vert != null)
//			{
//				Object val = vert.value(propToGet);
//				System.out.println("Values is " + val);
//				otherBindings.put(ids[0], val);
//			}
//			else if(doubleValue != null)
//			{
//				Object val = doubleValue.doubleValue();
//				System.out.println("Values is " + val);
//				otherBindings.put(ids[0], val);
//			}
//		}		
//	}
//	
//	protected Object getNextValue()
//	{
//		if(inputIterator != null)
//		{
//			addDataToBindings();
//		}
//		// who knows could be a legitimate number too for all I know
//		Object retObject= null;
//		
//		long nanoTime = System.nanoTime();
//		
//		try {
//			retObject = cs.eval();
//			System.out.println("Returning value.. " + retObject);
//			long now = System.nanoTime();
//			
//			System.out.println("Time Difference..  " + ((now - nanoTime)/1000000));
//
//		} catch (ScriptException e) {
//			// TODO Auto-generated catch block
//			retObject = e; // surprise !!
//			e.printStackTrace();
//			baseException = e;
//			errored = true;
//		}			
//		
//		return retObject;
//	}
//	
//	@Override
//	public Iterator process() {
//		modExpression();
//		String nodeStr = myStore.get(whoAmI).toString();
//		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
//		String[] columnsArray = convertVectorToArray(columns);
//		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
//		Vector<String> groupBys = (Vector <String>)myStore.get(PKQLEnum.COL_CSV);
//		
//		Object val = processAlgorithm(columns, groupBys);
//		myStore.put(nodeStr, val);
//		myStore.put("STATUS",STATUS.SUCCESS);
//		
////		if(groupBys != null && !groupBys.isEmpty()){
////			Vector<String> allColumns = new Vector<String>();
////			allColumns.addAll(groupBys);
////			allColumns.addAll(columns);
////			String[] allColumnsArray = convertVectorToArray(allColumns);
////			
////			//HERE
////			if(frame instanceof SparkDataFrame) {
////				Object val = ((SparkDataFrame)frame).mapReduce(columns, groupBys, this.getClass().getName());
////				myStore.put(nodeStr, val);
////			} else {
////				Iterator it = getTinkerData(allColumns, frame, false);			
////				HashMap<HashMap<Object,Object>,Object> groupByHash = reduceGroupBy(groupBys, columns, allColumnsArray, it);
////				myStore.put(nodeStr, groupByHash);
////			}
////			myStore.put("STATUS",STATUS.SUCCESS);
////		} else {
////			
////			//HERE
////			if(frame instanceof SparkDataFrame) {
////				Object val = ((SparkDataFrame)frame).mapReduce(columns, null, this.getClass().getName());
////				myStore.put(nodeStr, val);
////			} else {
////				Iterator iterator = getTinkerData(columns, frame, false);
////				myStore.put(nodeStr, processAlgorithm(iterator, columnsArray));
////			}
////			myStore.put("STATUS",STATUS.SUCCESS);
////		}
//		
//		return null;
//	}
//	
////	private Object processAlgorithm(Iterator iterator, String[] columnsArray) {
////		setData(iterator, columnsArray, myStore.get("MOD_" + whoAmI).toString());
////		return reduce();
////	}
//	
//	private Object processAlgorithm(Vector<String> columns, Vector<String> groupBys) {
//		Map options = new HashMap();
//		options.put("groupBys", groupBys);
//		options.put("columns", columns);
//		options.put("expr", (String) myStore.get(PKQLEnum.EXPR_TERM));
//		setData(options);
//		return reduce();
//	}
//}
