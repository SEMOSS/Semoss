package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.AlgorithmStrategy;
import prerna.ds.ExpressionReducer;
import prerna.util.Constants;

public abstract class BaseReducer implements ExpressionReducer, AlgorithmStrategy {
	
	protected Iterator inputIterator = null;
	protected String [] ids = null;
	protected String script = null;
	protected CompiledScript cs = null;
	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected boolean errored = false;
	protected java.lang.Object baseException = null;
	protected Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
	protected String propToGet = Constants.VALUE;

	
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
			else if(array != null)
			{
				for(int colIndex = 0;colIndex < ids.length;colIndex++)
				{
					Object val = array[colIndex];
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
			e.printStackTrace();
			baseException = e;
			errored = true;
		}			
		
		return retObject;
	}

}
