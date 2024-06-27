package prerna.ds;

import java.util.Iterator;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExpressionIterator implements ExpressionMapper, Iterator {
	
	private static final Logger classLogger = LogManager.getLogger(ExpressionIterator.class);

	protected Iterator results = null;
	protected String [] columnsUsed = null;
	String script = null;
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected boolean errored = false;
	protected Exception baseException = null;
	protected Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
	protected CompiledScript cs = null;
	protected String propToGet = TinkerFrame.TINKER_NAME;
	
	protected ExpressionIterator() {
		
	}
	
	public ExpressionIterator(Iterator results, String [] columnsUsed, String script)
	{
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		// TODO Auto-generated method stub
		// this is where we apply the calculation
		// so I need to get the next on the map
		// get the columns from the columns
		// set it into the bindings on the engine
		// run the calculation or whatever else
		// give the result or exception
		
		Object retObject = null;
		
		if(results != null && !errored)
		{
			setOtherBindings();
			long nanoTime = System.nanoTime();
			try {
				retObject = (Object)cs.eval();
				long now = System.nanoTime();
//				System.out.println("Time Difference..  " + ((now - nanoTime)/1000000));
			} catch (ScriptException e) {
				// TODO Auto-generated catch block
				retObject = e; // surprise !!
				baseException = e;
				errored = true;
			}			
		}
		return retObject;
	}
	
	public void setBinding(String name, Object value)
	{
		otherBindings.put(name,  value);
	}

	@Override
	public void setData(Iterator results, String[] columnsUsed, String script) {
		// TODO Auto-generated method stub
		this.results = results;
		this.columnsUsed = columnsUsed;
		this.script = script;
		try {
			cs = engine.compile(script);
			
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	protected void setOtherBindings() {
		Object daObject = results.next();
		Map <Object, Object> row = null;
		Object[] array = null;
		Vertex vert = null;
		Double doubleValue = null;
		if(daObject instanceof Map)
			row = (Map<Object, Object>)daObject;
		else if(daObject instanceof Object[])
			array = (Object[])daObject;
		else if(daObject instanceof Vertex)
			vert = (Vertex) daObject;
		else if(daObject instanceof Double)
			doubleValue = (Double)daObject;
		
		if(daObject != null && !errored)
		{
			if(row != null)
			{
				for(int colIndex = 0;colIndex < columnsUsed.length;colIndex++)
				{
					Vertex v = (Vertex)row.get(columnsUsed[colIndex]);
					Object val = v.value(propToGet);
//					System.out.println("Values is " + val);
					otherBindings.put(columnsUsed[colIndex], val);
				}
			}
			else if(array != null)
			{
				for(int colIndex = 0;colIndex < columnsUsed.length;colIndex++)
				{
					Object val = array[colIndex];
//					System.out.println("Values is " + val);
					otherBindings.put(columnsUsed[colIndex], val);
				}
			}
			else if(vert != null)
			{
				Object val = vert.value(propToGet);
//				System.out.println("Values is " + val);
				otherBindings.put(columnsUsed[0], val);
			}
			else if(doubleValue != null)
			{
				Object val = (Object)new Double(doubleValue.doubleValue());
//				System.out.println("Values is " + val);
				otherBindings.put(columnsUsed[0], val);
			}
		}
	}
	
	public Bindings getOtherBindings() {
		return this.otherBindings;
	}
}