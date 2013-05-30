package prerna.rdf.engine.impl;

import java.util.Vector;

import prerna.rdf.engine.api.IEngine;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

public class InMemoryJenaEngine implements IEngine {

	// for now this class just
	Model jenaModel = null;

	@Override
	public void openDB(String propFile) {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeDB() {
		// do nothing
	}

	@Override
	public Object execGraphQuery(String query) {
		Model model = null;
		try{
			com.hp.hpl.jena.query.Query q2 = QueryFactory.create(query); 
			QueryExecution qex = QueryExecutionFactory.create(q2, jenaModel);
			model = qex.execConstruct();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return model;
	}

	@Override
	public Object execSelectQuery(String query) {
		ResultSet rs = null;
		try{
			com.hp.hpl.jena.query.Query q2 = QueryFactory.create(query); 
			QueryExecution qex = QueryExecutionFactory.create(q2, jenaModel);
			rs = qex.execSelect();
		}catch (Exception e){
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public void execInsertQuery(String query) {
		// TODO Auto-generated method stub

	}

	public void setModel(Model jenaModel) {
		this.jenaModel = jenaModel;

	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<String> getEntityOfType(String sparqlQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Boolean execAskQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

}
