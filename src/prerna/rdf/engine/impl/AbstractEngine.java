package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;

public abstract class AbstractEngine implements IEngine {

	String engineName = null;
	String propFile = null;
	Properties prop = null;
	@Override
	public void openDB(String propFile) {
		// TODO Auto-generated method stub
		try {
			if(propFile != null)
			{
				this.propFile = propFile;
				prop = loadProp(propFile);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void closeDB() {
		// TODO Auto-generated method stub

	}

	public Properties loadProp(String fileName) throws Exception
	{
		Properties retProp = new Properties();
		retProp.load(new FileInputStream(fileName));
		System.out.println("Properties >>>>>>>>" + prop);
		return retProp;
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

	@Override
	public void addStatement(String subject, String predicate, Object object, boolean concept)
	{
		
	}
	
	protected String cleanString(String original, boolean replaceForwardSlash){
		String retString = original;
		retString = retString.trim();
		retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
		if(replaceForwardSlash)retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		
		boolean doubleSpace = true;
		while (doubleSpace == true)//remove all double spaces
		{
			doubleSpace = retString.contains("  ");
			retString = retString.replace("  ", " ");
		}
		
		return retString;
	}

	public void commit()
	{
		
	}
	
	public void saveConfiguration()
	{
		
		try {
			System.err.println("Writing to file " + propFile);
			prop.store(new FileOutputStream(propFile), null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addConfiguration(String name, String value)
	{
		prop.put(name, value);
	}
		
}
