package prerna.algorithm.impl;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.ExpressionReducer;

public abstract class BaseReducer implements ExpressionReducer {
	
	Iterator inputIterator = null;
	String [] ids = null;
	String script = null;
	CompiledScript cs = null;
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	boolean errored = false;
	java.lang.Object baseException = null;
	Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
	String propToGet = "DATA";


	public void set(Iterator inputIterator, String[] ids, String script, String prop ) {
		// TODO Auto-generated method stub
		this.inputIterator = inputIterator;
		this.ids = ids;
		this.script = script;
		if(prop != null)
			propToGet = prop;
		
		try {
			cs = engine.compile(script);
			
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private void addDataToBindings()
	{
		Object daObject = inputIterator.next();
		Map <Object, Object> row = null;
		Vertex vert = null;
		Double doubleValue = null;
		
		if(daObject instanceof Map)
			row = (Map<Object, Object>)daObject;
		else if(daObject instanceof Vertex)
			vert = (Vertex) daObject;
		else if(daObject instanceof Double)
			doubleValue = (Double)daObject;
		if(daObject != null && !errored)
		{
			// put things into the bindings first
			if(row != null)
			{
				for(int colIndex = 0;colIndex < ids.length;colIndex++)
				{
					Vertex v = (Vertex)row.get(ids[colIndex]);
					Object val = v.value(propToGet);
					System.out.println("Values is " + val);
					otherBindings.put(ids[colIndex], val);
				}
			}
			else if(vert != null)
			{
				Object val = vert.value(propToGet);
				System.out.println("Values is " + val);
				otherBindings.put(ids[0], val);
			}
			else if(doubleValue != null)
			{
				Object val = doubleValue.doubleValue();
				System.out.println("Values is " + val);
				otherBindings.put(ids[0], val);
			}
		}		
	}
	
	
	protected Object getNextValue()
	{
		if(inputIterator != null)
		{
			addDataToBindings();
		}
		// who knows could be a legitimate number too for all I know
		Object retObject= null;
		
		long nanoTime = System.nanoTime();
		
		try {
			retObject = cs.eval();
			System.out.println("Returning value.. " + retObject);
			long now = System.nanoTime();
			
			System.out.println("Time Difference..  " + ((now - nanoTime)/1000000));

		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			retObject = e; // surprise !!
			baseException = e;
			errored = true;
		}			
		
		return retObject;
	}


	@Override
	public abstract Object reduce() ;

}
