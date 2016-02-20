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

public class ExpressionIterator <Object> implements ExpressionMapper, Iterator {
	
	Iterator results = null;
	String [] columnsUsed = null;
	String script = null;
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	boolean errored = false;
	java.lang.Object baseException = null;
	Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
	CompiledScript cs = null;
	String propToGet = Constants.NAME;
	
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
	public java.lang.Object next() {
		// TODO Auto-generated method stub
		// this is where we apply the calculation
		// so I need to get the next on the map
		// get the columns from the columns
		// set it into the bindings on the engine
		// run the calculation or whatever else
		// give the result or exception
		
		java.lang.Object retObject = null;
		
		if(results != null && !errored)
		{
			java.lang.Object daObject = results.next();
			Map <Object, Object> row = null;
			Vertex vert = null;
			Double doubleValue = null;
			if(daObject instanceof Map)
				row = (Map<Object, Object>)daObject;
			else if(daObject instanceof Vertex)
				vert = (Vertex) daObject;
			else if(daObject instanceof Double)
				doubleValue = (Double)daObject;
			// put things into the bindings first
			
			if(daObject != null && !errored)
			{
				// put things into the bindings first
				if(row != null)
				{
					for(int colIndex = 0;colIndex < columnsUsed.length;colIndex++)
					{
						Vertex v = (Vertex)row.get(columnsUsed[colIndex]);
						Object val = v.value(propToGet);
						System.out.println("Values is " + val);
						otherBindings.put(columnsUsed[colIndex], val);
					}
				}
				else if(vert != null)
				{
					Object val = vert.value(propToGet);
					System.out.println("Values is " + val);
					otherBindings.put(columnsUsed[0], val);
				}
				else if(doubleValue != null)
				{
					Object val = (Object)new Double(doubleValue.doubleValue());
					System.out.println("Values is " + val);
					otherBindings.put(columnsUsed[0], val);
				}
			}		
			
//			for(int colIndex = 0;colIndex < columnsUsed.length;colIndex++)
//				otherBindings.put(columnsUsed[colIndex], row.get(columnsUsed[colIndex]));
			
			long nanoTime = System.nanoTime();
			
			try {
				retObject = cs.eval();
				long now = System.nanoTime();
				
				System.out.println("Time Difference..  " + ((now - nanoTime)/1000000));

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
			e.printStackTrace();
		}
	}
	
	public Bindings getOtherBindings() {
		return this.otherBindings;
	}
}