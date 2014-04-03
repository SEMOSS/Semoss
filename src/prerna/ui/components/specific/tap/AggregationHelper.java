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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Set;

import javax.swing.JList;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Functions needed for data aggregation procedures.
 */
public class AggregationHelper {
	
	Logger logger = Logger.getLogger(getClass());
	
	private String semossConceptBaseURI = "http://semoss.org/ontologies/Concept/";
	private String semossRelationBaseURI = "http://semoss.org/ontologies/Relation/";
	private String semossPropertyBaseURI = "http://semoss.org/ontologies/Relation/Contains/";

	private static Hashtable<String, Hashtable<String, Object>> dataHash = new Hashtable<String, Hashtable<String, Object>>();
	
	//private Hashtable<String, Set<String>> allRelations = new Hashtable<String, Set<String>>();
	//private Hashtable<String, Set<String>> allConcepts = new Hashtable<String, Set<String>>();

	public String errorMessage = "";
	
//QUERY DATA HELPERS****************************************************************************************************************
	//run the query
	public SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}
	
	public void deleteData(IEngine engine, Hashtable<String, Hashtable<String, Object>> data)
	{
		StringBuilder deleteQuery = new StringBuilder("DELETE DATA { ");
		boolean notEmpty = false;
		for ( String sub : data.keySet())
		{
			for (String pred : data.get(sub).keySet())
			{
				Object obj = data.get(sub).get(pred);
				if(!sub.equals("") && !pred.equals("") && !obj.equals(""))
				{
					notEmpty = true;
				}
				deleteQuery.append(sub + " " + pred + " " + obj + ". ");
			}
		}
		deleteQuery.append(" }");
		logger.info("DELETE QUERY: " + deleteQuery.toString());
		if(notEmpty)
		{
			UpdateProcessor proc = new UpdateProcessor();
			proc.setEngine(engine);
			proc.setQuery(deleteQuery.toString());
			proc.processQuery();
		}
	}

			
//INSERT DATA HELPERS****************************************************************************************************************

	public void processData(IEngine coreDB, Hashtable<String, Hashtable<String, Object>> data) {
		for (String sub : data.keySet()) {
			for (String pred : data.get(sub).keySet()) {
				Object obj = data.get(sub).get(pred);
				boolean concept_triple = true;
				if (pred.contains("Relation/Contains")) {
					concept_triple = false;
				}
				((BigDataEngine) coreDB).addStatement(sub, pred, obj, concept_triple);
				logger.info("ADDING INTO " + coreDB.toString() + ": " + sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
		}
	}
	
	/* (TODO - set up) public void processNewConcepts(Hashtable<String, Set<String>> data) {
		String pred = RDF.TYPE.toString();
		String concept = "http://semoss.org/ontologies/Concept";
		String subclassOf = RDFS.SUBCLASSOF.toString();
		for (String obj : data.keySet()) {
			for (String sub : data.get(obj)) {
				((BigDataEngine) coreDB).addStatement(sub, pred, obj, true);
				logger.info("ADDING INSTANCE TYPEOF CONCEPT TRIPLE: " + sub
						+ ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
			// add concepts that are not already in db
			if (!conceptList.contains(obj)) {
				((BigDataEngine) coreDB).addStatement(obj, subclassOf, concept,
						true);
				logger.info("ADDING NEW CONCEPT TRIPLE: " + obj + ">>>>>"
						+ subclassOf + ">>>>>" + concept + ">>>>>");
			}
		}
	}*/

	public void processNewRelationships(IEngine coreDB, Hashtable<String, Set<String>> data) {
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
			for (String obj : data.keySet()) {
				for (String sub : data.get(obj)) {
					((BigDataEngine) coreDB).addStatement(sub, subpropertyOf, obj, true);
					logger.info("ADDING RELATIONSHIP INSTANCE SUBPROPERTY TRIPLE: " + sub + ">>>>>" + subpropertyOf + ">>>>>" + obj	+ ">>>>>");
				}			
			}	
		}
	
	public Hashtable<String, Hashtable<String, Object>> addToHash(Hashtable<String, Hashtable<String, Object>> currentHash, Object[] returnTriple) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put(returnTriple[1].toString(), returnTriple[2]);
			if(currentHash.containsKey(returnTriple[0].toString())) {
				currentHash.get(returnTriple[0].toString()).putAll(innerHash);
			}
			else {
				currentHash.put(returnTriple[0].toString(), innerHash);
			}
		return currentHash;
	}
	
	/* (TODO - set up) public void addToDeleteHash(Object[] returnTriple)
	{
		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		innerHash.put(returnTriple[1].toString(), returnTriple[2]);
		if(removeDataHash.containsKey(returnTriple[0].toString()))
		{
			removeDataHash.get(returnTriple[0]).putAll(innerHash);
		}
		else
		{
			removeDataHash.put(returnTriple[0].toString(), innerHash);
		}
	}*/

	/* (TODO - set up) public void addToAllConcepts(String uri)
	{
		String conceptBaseURI = semossConceptBaseURI + Utility.getClassName(uri);
		if(allConcepts.containsKey(conceptBaseURI)) {
			allConcepts.get(conceptBaseURI).add(uri);
		}
		else
		{
			allConcepts.put(conceptBaseURI, new HashSet<String>());
			allConcepts.get(conceptBaseURI).add(uri);
		}		
	}*/

	public Hashtable<String, Set<String>> addNewRelationships(Hashtable<String, Set<String>> currentRelHash, String uri)
	{
		String relationBaseURI = semossRelationBaseURI + Utility.getClassName(uri);
		if(currentRelHash.containsKey(relationBaseURI))
		{
			currentRelHash.get(relationBaseURI).add(uri);
		}
		else
		{
			currentRelHash.put(relationBaseURI, new HashSet<String>());
			currentRelHash.get(relationBaseURI).add(uri);
		}
		return currentRelHash;
	}

	/* (TODO - set up) public void deleteData(Hashtable<String, Hashtable<String, Object>> data)	{
		StringBuilder deleteQuery = new StringBuilder("DELETE DATA { ");
		boolean notEmpty = false;
		for ( String sub : data.keySet())
		{
			for (String pred : data.get(sub).keySet())
			{
				Object obj = data.get(sub).get(pred);
				if(!sub.equals("") && !pred.equals("") && !obj.equals(""))
				{
					notEmpty = true;
				}
				deleteQuery.append(sub + " " + pred + " " + obj + ". ");
			}
		}
		deleteQuery.append(" }");
		logger.info("DELETE QUERY: " + deleteQuery.toString());
		if(notEmpty)
		{
			UpdateProcessor proc = new UpdateProcessor();
			proc.setEngine(coreDB);
			proc.setQuery(deleteQuery.toString());
			proc.processQuery();
		}
	}*/

// (TODO - set up) GENERAL METHODS FOR PROPERTIES************************************************************************************************************************

	/*public Object[] processSumValues(String sub, String prop, Object value)
	{
		try
		{
			((Literal) value).doubleValue();
		}
		catch(NumberFormatException e)
		{
			e.printStackTrace();
			this.errorMessage = this.errorMessage + "Error Processing Max/Min Double. Please check value of Double. \n" 
					+ "Error occured processing: " + sub + ">>>>" + prop + ">>>>" + value + "\n";	
			return new String[]{""};
		}

		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			logger.debug("ADDING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Double addValue = ( (Literal) value).doubleValue();
			Double currentValue = (Double) innerHash.get(prop);
			value = addValue + currentValue;
			logger.debug("ADJUSTING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}

	public Object[] processConcatString(String sub, String prop, Object value, String user) 
	{
		// replace any tags for properties that are loaded as other data types but should be strings
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#double","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#decimal","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#integer","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#float","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#boolean","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#dateTime","");

		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			if(!user.equals(""))
			{
				value = "\"" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString().substring(1);
			}
			logger.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			if(!user.equals(""))
			{
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + getTextAfterFinalDelimeter(user, "/") + ":" + value.toString().substring(1);
			}
			else
			{
				value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + value.toString().substring(1);
			}
			logger.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}

	public Object[] processMaxMinDouble(String sub, String prop, Object value, boolean max)
	{
		try
		{
			((Literal) value).doubleValue();
		}
		catch(NumberFormatException e)
		{
			e.printStackTrace();
			this.errorMessage = this.errorMessage + "Error Processing Max/Min Double. Please check value of Double. \n" 
					+ "Error occured processing: " + sub + ">>>>" + prop + ">>>>" + value + "\n";	
			return new String[]{""};
		}

		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			logger.debug("ADDING DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Double oldDouble = (Double) innerHash.get(prop);
			Double newDouble = ((Literal) value).doubleValue();
			if(!max)
			{
				if(newDouble < oldDouble)
				{
					// return the value being passed in
					value = ((Literal) value).doubleValue();
					logger.debug("ADJUSTING MIN DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
			else
			{
				if(newDouble > oldDouble)
				{
					// return the value being passed in
					value = ((Literal) value).doubleValue();
					logger.debug("ADJUSTING MAX DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
		}
		return new Object[]{sub, prop, value};
	}

	public Object[] processMinMaxDate(String sub, String prop, Object value, Boolean latest) 
	{
		try
		{
			((Literal) value).calendarValue();
		}
		catch(IllegalArgumentException e)
		{
			e.printStackTrace();
			this.errorMessage = this.errorMessage + "Error Processing Max/Min Date. Please check value of Date. \n" 
					+ "Error occured processing: " + sub + ">>>>" + prop + ">>>>" + value + "\n";	
			return new String[]{""};
		}

		Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).calendarValue();
			logger.debug("ADDING DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			XMLGregorianCalendar oldDate = (XMLGregorianCalendar) innerHash.get(prop);
			XMLGregorianCalendar newDate = ((Literal) value).calendarValue();
			if(!latest)
			{
				if(newDate.toGregorianCalendar().getTime().before(oldDate.toGregorianCalendar().getTime()))
				{
					// return the value being passed in
					value = ((Literal) value).calendarValue();
					logger.debug("ADJUSTING MIN DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
			else
			{
				if(newDate.toGregorianCalendar().getTime().after(oldDate.toGregorianCalendar().getTime()))
				{
					// return the value being passed in
					value = ((Literal) value).calendarValue();
					logger.debug("ADJUSTING MAX DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
		}
		return new Object[]{sub, prop, value};
	}
*/
	
// ADDITIONAL PROCESSING METHODS ************************************************************************************************************************	
	
	public ArrayList<ArrayList<Object>> arrayListResultProcessor(SesameJenaSelectWrapper sjsw) {
		String[] names = sjsw.getVariables();
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		try {
			while (sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				ArrayList<Object> values = new ArrayList<Object>();
				for (int colIndex = 0; colIndex < names.length; colIndex++) {
					if (sjss.getRawVar(names[colIndex]) != null) {
						if (sjss.getRawVar(names[colIndex]) instanceof Double) {
							values.add(colIndex, (Double) sjss.getRawVar(names[colIndex]));
						}
						else values.add(colIndex, sjss.getRawVar(names[colIndex]));						
					}
				}
				list.add(values);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public Hashtable hashTableResultProcessor(SesameJenaSelectWrapper sjsw) {
		//Hashtable dataHash = new Hashtable();
		/*if(truedataobject) {
			Hashtable<String, Hashtable<String, Set<String>>> aggregatedData = new Hashtable<String, Hashtable<String, Set<String>>>();
			String[] vars = sjsw.getVariables();
			while (sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();			
				String sub = sjss.getRawVar(vars[0]).toString();
				String node = sjss.getRawVar(vars[1]).toString();
				Set<String> prop = null;
				prop.add(sjss.getRawVar(vars[2]).toString());
				Hashtable<String, Set<String>> subHash = new Hashtable<String, Set<String>>();
				subHash.put(node, prop);
				if (aggregatedData.contains(node)) {
					
				}
								
				aggregatedData.put(sub, subHash);
			}
			dataHash.put("data", aggregatedData);
		}
		else {*/
			Hashtable<String, Set<String>> aggregatedData = new Hashtable<String, Set<String>>();
			String[] vars = sjsw.getVariables();
			while (sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();			
				String sub = sjss.getRawVar(vars[0]).toString();
				Set<String> pred = new HashSet<String>();
				pred.add(sjss.getRawVar(vars[1]).toString());
				if (!aggregatedData.containsKey(sub))
					{aggregatedData.put(sub, pred);}
				else {aggregatedData.get(sub).add(sjss.getRawVar(vars[1]).toString());}				
			}		
		//}
				
		return aggregatedData;
	}
		
// UTILITY METHODS ************************************************************************************************************************
	
	public String getTextAfterFinalDelimeter(String uri, String delimeter) {
		if(!uri.equals(""))
		{
			uri = uri.substring(uri.lastIndexOf(delimeter)+1);
		}
		return uri;
	}

	@SuppressWarnings("unused")
	private String getBaseURI(String uri) {
		return uri.substring(0, uri.substring(0, uri.substring(0, uri.lastIndexOf("/")).lastIndexOf("/")).lastIndexOf("/"));
	}

// GETTERS & SETTERS************************************************************************************************************************

	public String getSemossConceptBaseURI() {
		return semossConceptBaseURI;
	}

	public String getSemossRelationBaseURI() {
		return semossRelationBaseURI;
	}

	public String getSemossPropertyBaseURI() {
		return semossPropertyBaseURI;
	}
	
}
