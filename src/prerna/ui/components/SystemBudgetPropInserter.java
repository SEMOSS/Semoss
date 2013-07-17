package prerna.ui.components;

import java.util.ArrayList;

import javax.swing.JList;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SystemBudgetPropInserter {

	Logger logger = Logger.getLogger(getClass());
	String query;
	String propName = "SustainmentBudget";
	SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
	String tapEngineName = "TAP_Core_Data";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	//this function will run the query
	//step through the results of the query and put it into an insert query
	//run the insert query
	public void runInsert(){
		logger.info("Beginning sustainment budget insert functionality");
		runQuery();
		logger.info("Got budget numbers");
		logger.info("Begin to put together insert query");
		String insertQuery = prepareInsertQuery();
		logger.info("Got insert query");
		logger.info("Begin to run insert");
		
		//run the insert
		UpdateProcessor proc = new UpdateProcessor();
		proc.setEngine((IEngine) DIHelper.getInstance().getLocalProp(tapEngineName));
		proc.setQuery(insertQuery);
		proc.processQuery();
		logger.info("Insert complete");
	}
	
	private String prepareInsertQuery(){
		//Set up the insert query
		//this is the pred URI that will be used to relate each system to its budget
		//First need to set up the pred URI as a property etc.
		String predUri = "<http://health.mil/ontologies/dbcm/Relation/Contains/"+propName+">";
		//start with type triple
		String insertQuery = "INSERT DATA { " +predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://health.mil/ontologies/dbcm/Relation/Contains>. ";
		//add other type triple
		insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
		//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
		insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
				predUri + ". ";

		// get the bindings from it
		String [] names = wrapper.getVariables();
		int count = 0;
		ArrayList <Object []> list = new ArrayList();
		
		while(wrapper.hasNext()){

			SesameJenaSelectStatement sjss = wrapper.next();
			
			Object [] values = new Object[names.length];
			for(int colIndex = 0;colIndex < names.length;colIndex++)
			{
				values[colIndex] = sjss.getVar(names[colIndex]);
				logger.debug("Binding Name " + names[colIndex]);
				logger.debug("Binding Value " + values[colIndex]);
			}
			
			//add to insert query
			String subjectUri = "<http://health.mil/ontologies/dbcm/Concept/System/"+values[0] +">";
			String objectUri = "\"" + values[1] + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";
			insertQuery = insertQuery + subjectUri + " " + predUri + " " + objectUri + ". ";
			
			logger.debug("Creating new Value " + values);
			list.add(count, values);
			count++;
		}
		//close the insert query
		insertQuery = insertQuery + "}";
		
		return insertQuery;
	}
	
	private void runQuery(){
		//get the selected repositories
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		
		//for each selected repository, run the query
		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			//get specific engine
			IEngine selectedEngine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			logger.info("Selecting repository " + repos[repoIndex]);
			
			//create the update wrapper, set the variables, and let it run
			wrapper.setEngine(selectedEngine);
			wrapper.setQuery(query);
			wrapper.executeQuery();
			
		}
	}
	
	public void setQuery(String q){
		query = q;
	}

}
