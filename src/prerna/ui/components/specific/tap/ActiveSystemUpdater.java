/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class allows a user to traverse freely to only active systems
 * Adds two subclass triples: {ActiveSystem SubclassOf Concept} and {ActiveSystem SubclassOf System}
 * Adds a type triple for each system instance that is active {InstanceActiveSystem typeOf ActiveSystem} 
 */
public class ActiveSystemUpdater {
	
	static final Logger logger = LogManager.getLogger(ActiveSystemUpdater.class.getName());
	
	IEngine engine = null;
	// specific URIs that do not change
	String subclassURI = Constants.SUBCLASS_URI;
	String typeURI = Constants.TYPE_URI;
	String baseSemossSystemURI = "http://semoss.org/ontologies/Concept";
	String activeSystemURI = "http://semoss.org/ontologies/Concept/ActiveSystem";
	private boolean foundQuery = false;
	
	/**
	 * Functions as the main method for the class
	 * Calls methods to determine which systems are active
	 * Calls method to add the actual triples into the database
	 * Requires the engine to be defined
	 */
	public void runUpdateActiveSystems()
	{
		// make sure user has defined the engine
		if (engine != null){
			// get list of all systems in engine
			ArrayList<String> allSystems = getSystems(ConstantsTAP.GET_ALL_SYSTEMS_QUERY);
			// get list of all systems that are decommissioned
			ArrayList<String> decommissionedSystems = getSystems(ConstantsTAP.GET_DECOMMISSIONED_SYSTEMS_QUERY);
			// get list of all active systems
			ArrayList<String> activeSystems = findActiveSystems(allSystems,decommissionedSystems);
			// add necessary triples into the engine
			insertSubclassTriple(activeSystems);
		}
		else
		{
			Utility.showMessage("<html>Warning!<br>You have not chosen an engine</html>");
		}
	}
	/**
	 * Determine which systems are active by comparing all systems to those that are decommissioned
	 * @param allSystems				ArrayList that contains the names of all the systems
	 * @param decommissionedSystems		ArrayList that contains the names of all the decommissioned systems
	 * @return activeSystems			ArrayList that contains the names of all the active systems
	 */
	public ArrayList<String> findActiveSystems(ArrayList<String> allSystems, ArrayList<String> decommissionedSystems)
	{
		String systemName = "";
		ArrayList<String> activeSystems = new ArrayList<String>();
		// iterator to go through all systems
		Iterator<String> iterator = allSystems.iterator();
		while(iterator.hasNext()) 
		{
			systemName = iterator.next();
			// if system is not contained in decommissioned systems list, add to active systems list
			if (!decommissionedSystems.contains(systemName))
			{
				activeSystems.add(systemName);
			}
		}
		return activeSystems;
	}
	
	/**
	 * Calls UpdateProcessor class to add the necessary triples into the engine to allow a user to traverse freely to only active systems
	 * Adds two subclass triples: {ActiveSystem SubclassOf Concept} and {ActiveSystem SubclassOf System}
	 * Adds a type triple for each system instance that is active {InstanceActiveSystem typeOf ActiveSystem} 	 
	 * @param activeSystems		ArrayList containing the list of all active systems
	 */
	public void insertSubclassTriple(ArrayList<String> activeSystems)
	{
		if(this.foundQuery)
		{
			// add semoss base subclass triple
			String insertQuery = "INSERT DATA { <" + activeSystemURI +"> <"+  subclassURI +"> <"+ baseSemossSystemURI + "> .";
			insertQuery = insertQuery + "<" + activeSystemURI +"> <"+  subclassURI +"> <"+ baseSemossSystemURI + "/System> .";
			Iterator<String> iterator = activeSystems.iterator();
			String systemInstanceURI = "";
			// add all the instance typeOf triples
			while(iterator.hasNext())
			{
				systemInstanceURI = "<" + iterator.next() + ">";
				insertQuery = insertQuery + " " + systemInstanceURI +" <" +  typeURI + "> <"+ activeSystemURI + "> .";
			}
			insertQuery = insertQuery + " }";
			logger.info("Inserting: " + insertQuery);
			// process query suing UpdateProcessor class
			UpdateProcessor proc = new UpdateProcessor();
			proc.setEngine(engine);
			proc.setQuery(insertQuery);
			proc.processQuery();
		}
	}
	
	/**
	 * Runs the query from the engines question sheet to get all the systems specified by the query
	 * Requires the query to be defined in the questions sheet
	 * Calls SesameJenaSelectWrapper to execute the query
	 * @param		String query contains the query to retrieve the list of systems
	 * @return		ArrayList containing the systems
	 */
	public ArrayList<String> getSystems(String query)
	{
		ArrayList<String> systemList = new ArrayList<String>();
		// load query from questions sheet
		String queryData = DIHelper.getInstance().getProperty(query);
		if(queryData != null)
		{
			this.foundQuery = true;
			logger.info("Running: " + queryData);
			System.out.println(queryData);
			SesameJenaSelectWrapper wrapperAllSystems = new SesameJenaSelectWrapper();
			wrapperAllSystems.setEngine(engine);
			wrapperAllSystems.setQuery(queryData);
			wrapperAllSystems.executeQuery();
		
			// perform so next() method works
			wrapperAllSystems.getVariables();
		
			while(wrapperAllSystems.hasNext()){
				SesameJenaSelectStatement sjss = wrapperAllSystems.next();
				systemList.add(sjss.getRawVar("system").toString());
			}
		}
		else{
			this.foundQuery = false;
		}
		return systemList;
	}
	
	
	/**
	 * Insert the base relationships created due to the subclassing into the OWL file
	 * Adds two subclass triples: {ActiveSystem SubclassOf Concept} and {ActiveSystem SubclassOf System}
	 * @param engineName				String containing the name of the engine
	 * @throws RepositoryException
	 * @throws SailException
	 */
	public void addToOWL(String engineName) throws RepositoryException, RDFHandlerException 
	{
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		
		// get the path to the owlFile
		String owlFileLocation = DIHelper.getInstance().getProperty(engineName +"_"+Constants.OWL);
		
		AbstractEngine baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
		RDFFileSesameEngine existingEngine = (RDFFileSesameEngine) baseRelEngine;
		existingEngine.addStatement(activeSystemURI, subclassURI, baseSemossSystemURI, true);
		existingEngine.addStatement(activeSystemURI, subclassURI, baseSemossSystemURI + "/System", true);
		RepositoryConnection exportRC = existingEngine.getRc();
		
		FileWriter fWrite = null;
		try{
			fWrite = new FileWriter(owlFileLocation);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			exportRC.export(owlWriter);
			fWrite.close();
			owlWriter.close();	
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
			Utility.showMessage("<html>Error!<br>Existing OWL file not found</html>");
		}finally{
			try{
				if(fWrite!=null)
					fWrite.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Set the engine for the entire class
	 * @param engineName	String containing the name of the engine
	 */
	public void setEngine(String engineName){
		this.engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
	}
	
	/**
	 * Sends the boolean to UpdateActiveSystems to determine if the correct queries are found in the question sheet
	 * @return	Boolean foundQuery which is true if the query is found in the question sheet
	 */
	public boolean getFoundQuery(){
		return this.foundQuery;
	}
}
