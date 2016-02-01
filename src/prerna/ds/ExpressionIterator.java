package prerna.ds;

import java.util.Iterator;
import java.util.Map;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ExpressionIterator <Object> implements Iterator {
	
	GraphTraversal <Vertex, Map<Object, Object>> results = null;
	String [] columnsUsed = null;
	String script = null;
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	boolean errored = false;
	java.lang.Object baseException = null;
	Bindings otherBindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
	CompiledScript cs = null;
	
	public ExpressionIterator(GraphTraversal <Vertex, Map<Object, Object>> results, String [] columnsUsed, String script)
	{
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
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return results.hasNext();
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
		
		Map <Object, Object> row = results.next();
		if(row != null && !errored)
		{
			// put things into the bindings first
			
			for(int colIndex = 0;colIndex < columnsUsed.length;colIndex++)
				otherBindings.put(columnsUsed[colIndex], row.get(columnsUsed[colIndex]));
			
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
}