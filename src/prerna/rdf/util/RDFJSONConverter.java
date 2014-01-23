package prerna.rdf.util;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RDFJSONConverter {

	public static 	Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

	// this does the following things
	// convert a scalar query against a database into a json
	public static Object getSelectAsJSON(String query, IEngine engine)
	{
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		
		
	SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
	wrapper.setEngine(engine);
	wrapper.setQuery(query);
	wrapper.executeQuery();

	// get the bindings from it
	String [] names = wrapper.getVariables();
	int count = 0;
	// now get the bindings and generate the data
	while(wrapper.hasNext())
	{
		SesameJenaSelectStatement sjss = wrapper.next();
		
		Object [] values = new Object[names.length];
		for(int colIndex = 0;colIndex < names.length;colIndex++)
		{
			values[colIndex] = sjss.getRawVar(names[colIndex])+"";
		}
		list.add(count, values);
		count++;
	}
	return list;
}

	/*{
		// the engine will always return a vector
		// the variables are then plugged into a hashtable
		// which is plugged into an vector and then converts the vector into a json
		Vector output = new Vector();
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();
		sjsw.getVariables();
		
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement stmt = sjsw.next();
			output.addElement(stmt.getPropHash());
		}
		// return the output
		return gson.toJson(output);
	}*/	
	
	public static String getGraphAsJSON(String query, IEngine engine, Hashtable baseFilterHash)
	{
		// the engine will always return a vector
		// the variables are then plugged into a hashtable
		// which is plugged into an vector and then converts the vector into a json
		System.err.println("Called the new routine ");
		Vector output = new Vector();
		SesameJenaSelectCheater sjsw = new SesameJenaSelectCheater();
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.execute();
		//sjsw.getVariables();
		StringBuilder stringOutput = new StringBuilder();
		//stringOutput.append("[");
		
		while(sjsw.hasNext())
		{
			ArrayList tempVector = new ArrayList();
			SesameJenaConstructStatement sct = sjsw.next();
			if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
			{
				// add this guy as source and target
				stringOutput.append(Utility.getInstanceName(sct.getSubject()+"")+ "," + Utility.getInstanceName(sct.getObject()+"")+ ",1 newline");
				//stringOutput.app
				//output.add("{" + sct.getSubject()+ "," + sct.getObject()+",1}");
			}
		}
		//stringOutput.append("]");
		// return the output
		return stringOutput.toString();//gson.toJson(output);
	}	

	
	public static String getGraphAsJSON(IEngine engine, Hashtable baseFilterHash)
	{
		// pretty much gets the graph from a given repository connection
		// 
		String predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
		  //"VALUES ?Subject {"  + subjects + "}"+
		  //"VALUES ?Object {"  + subjects + "}"+
		  //"VALUES ?Object {"  + objects + "}" +
		  //"VALUES ?Predicate {"  + predicates + "}" +
		  "{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
		  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
		  "{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
		  "{?Subject ?Predicate ?Object.}" +
		  "}";

		return getGraphAsJSON(predicateSelectQuery, engine, baseFilterHash);
	}
	
}
