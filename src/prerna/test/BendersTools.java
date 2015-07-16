package prerna.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class BendersTools {

	public BTreeDataFrame createBTree(String[] columnHeaders, ArrayList<Object[]> data){
		BTreeDataFrame result = new BTreeDataFrame(columnHeaders);
		for(int l = 0; l < data.size();l++){
			HashMap<String, Object> tempHash = new HashMap<String, Object>();
			for(int i = 0; i < columnHeaders.length; i++){
				for(int k = 0; k < data.get(l).length; k++){
					tempHash.put(columnHeaders[i], data.get(l)[k]);
				}
			}
			result.addRow(tempHash, tempHash);
		}
		return result;
	}
	
	public BigDataEngine loadEngine(String engineLocation){
		BigDataEngine engine = new BigDataEngine();
		FileInputStream fileIn;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(engineLocation);
			prop.load(fileIn);
			//SEP
			try {
				String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

				String engineName = prop.getProperty(Constants.ENGINE);
				String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
				//TEMPORARY
				// TODO: remove this
				if(engineClass.equals("prerna.rdf.engine.impl.RDBMSNativeEngine")){
					engineClass = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
				}
				else if(engineClass.startsWith("prerna.rdf.engine.impl.")){
					engineClass = engineClass.replace("prerna.rdf.engine.impl.", "prerna.engine.impl.rdf.");
				}
				engine = (BigDataEngine)Class.forName(engineClass).newInstance();
				engine.setEngineName(engineName);
				if(prop.getProperty("MAP") != null) {
					engine.addProperty("MAP", prop.getProperty("MAP"));
				}
				engine.openDB(engineLocation);
				engine.setDreamer(prop.getProperty(Constants.DREAMER));
				engine.setOntology(prop.getProperty(Constants.ONTOLOGY));
				
				// set the core prop
				if(prop.containsKey(Constants.DREAMER))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
				if(prop.containsKey(Constants.ONTOLOGY))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
				if(prop.containsKey(Constants.OWL)) {
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
					engine.setOWL(prop.getProperty(Constants.OWL));
				}
				
				// set the engine finally
				engines = engines + ";" + engineName;
				DIHelper.getInstance().setLocalProperty(engineName, engine);
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			//SEP
			fileIn.close();
			prop.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return engine;
	}
	
	public boolean checkQuery(BigDataEngine engine, String query) throws MalformedQueryException{
		Boolean result = false;
		Object check = engine.execQuery(query);
		
		if(check instanceof Boolean){
			//DO NOTHING
		} else {
			//Limit the results
			if(query.contains("BINDINGS")){
				query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
			}
			if(query.contains("LIMIT") || query.contains("limit")){
				//DO Nothing
			} else {query += " LIMIT 3";}
		}
		
		System.out.println(query);
		
		if(check instanceof GraphQueryResult){
			try {
				GraphQueryResult res = (GraphQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else if(check instanceof Boolean) {
			try {
				result = (Boolean) check;
			} catch (NullPointerException e){
				//Nothing
			}
		} else if(check instanceof TupleQueryResult){
			try {
				TupleQueryResult res = (TupleQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else {
			/*DBUG*/System.out.println("NOT A THING!");
			result = true;
		}
		System.out.println("...");
		return result;
	}

	public boolean checkQuery(String engineLocation, String query) throws MalformedQueryException{
		BigDataEngine engine = loadEngine(engineLocation);
		/*DBUG*/System.out.println("CHECKING "+engine.getEngineName());

		Boolean result = false;
		Object check = engine.execQuery(query);
		
		if(check instanceof Boolean){
			//DO NOTHING
		} else {
			//Limit the results
			if(query.contains("BINDINGS")){
				query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
			}
			if(query.contains("LIMIT") || query.contains("limit")){
				//DO Nothing
			} else {query += " LIMIT 3";}
		}
		
		System.out.println(query);
		
		if(check instanceof GraphQueryResult){
			try {
				GraphQueryResult res = (GraphQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else if(check instanceof Boolean) {
			try {
				result = (Boolean) check;
			} catch (NullPointerException e){
				//Nothing
			}
		} else if(check instanceof TupleQueryResult){
			try {
				TupleQueryResult res = (TupleQueryResult) check;
				if(res != null){result = true;}
				res.close();
			} catch (NullPointerException | QueryEvaluationException e){
				//Nothing
			}
		} else {
			result = true;
		}
		System.out.println("...");
		closeEngine(engine);
		return result;
	}
	
	public void closeEngine(BigDataEngine engine){
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
	}
}
