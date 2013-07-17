package prerna.rdf.engine.impl;

import java.util.Vector;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;

public abstract class AbstractEngine implements IEngine {

	String engineName = null;
	@Override
	public void openDB(String propFile) {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeDB() {
		// TODO Auto-generated method stub

	}

	@Override
	public Object execGraphQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object execSelectQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execInsertQuery(String query) throws SailException,
			UpdateExecutionException, RepositoryException,
			MalformedQueryException {
		// TODO Auto-generated method stub

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

	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return engineName;
	}

}
