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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Functions needed for data aggregation procedures.
 */
public class AggregationHelper implements IAggregationHelper {

	private static final Logger LOGGER = LogManager.getLogger(AggregationHelper.class.getName());

	public String errorMessage = "";

	// Fundamental Methods
	public void processData(IEngine engine, HashMap<String, HashMap<String, Object>> data)
	{
		for( String sub : data.keySet())
		{
			for ( String pred : data.get(sub).keySet())
			{
				Object obj = data.get(sub).get(pred);
				boolean concept_triple = true;
				if( pred.contains("Relation/Contains"))
				{
					concept_triple = false;
				}
				( (BigDataEngine) engine).addStatement(sub, pred, obj, concept_triple);
				LOGGER.info("ADDING INTO " + engine.getEngineName() + ": " + sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
		}
	}
	
	public void deleteData(IEngine engine, HashMap<String, HashMap<String, Object>> data)
	{
		for( String sub : data.keySet())
		{
			for ( String pred : data.get(sub).keySet())
			{
				Object obj = data.get(sub).get(pred);
				boolean concept_triple = true;
				if( pred.contains("Relation/Contains"))
				{
					concept_triple = false;
				}
				( (BigDataEngine) engine).removeStatement(sub, pred, obj, concept_triple);
				LOGGER.info("REMOVING FROM " + engine.getEngineName() + ": " + sub + ">>>>>" + pred + ">>>>>" + obj + ">>>>>");
			}
		}		
		//		StringBuilder deleteQuery = new StringBuilder("DELETE DATA { ");
		//		boolean notEmpty = false;
		//		for ( String sub : data.keySet())
		//		{
		//			for (String pred : data.get(sub).keySet())
		//			{
		//				Object obj = data.get(sub).get(pred);
		//				if(!sub.equals("") && !pred.equals("") && !obj.equals(""))
		//				{
		//					notEmpty = true;
		//				}
		//				deleteQuery.append(sub + " " + pred + " " + obj + ". ");
		//			}
		//		}
		//		deleteQuery.append(" }");
		//		logger.info("DELETE QUERY: " + deleteQuery.toString());
		//		if(notEmpty)
		//		{
		//			UpdateProcessor proc = new UpdateProcessor();
		//			proc.setEngine(coreDB);
		//			proc.setQuery(deleteQuery.toString());
		//			proc.processQuery();
		//		}
	}
	
	public void processNewSubclass(IEngine engine, String parentType, String childType)
	{
		String subclassOf = RDFS.SUBCLASSOF.toString();
		( (BigDataEngine) engine).addStatement(childType, subclassOf, parentType, true);
		LOGGER.info("ADDING NEW SUBCLASS TRIPLE: " + childType + ">>>>>" + subclassOf + ">>>>>" + parentType + ">>>>>");
	}


	public void processNewConcepts(IEngine engine, String newConceptType)
	{
		String concept = "http://semoss.org/ontologies/Concept";
		String subclassOf = RDFS.SUBCLASSOF.toString();
		
		( (BigDataEngine) engine).addStatement(newConceptType, subclassOf, concept, true);
		RDFFileSesameEngine existingBaseEngine = (RDFFileSesameEngine) ( (AbstractEngine) engine).getBaseDataEngine();
		existingBaseEngine.addStatement(newConceptType, subclassOf, concept, true);
		LOGGER.info("ADDING NEW CONCEPT TRIPLE: " + newConceptType + ">>>>>" + subclassOf + ">>>>>" + concept + ">>>>>");
	}

	public void processNewRelationships(IEngine engine, String newRelationshipType) 
	{
		String relation = "http://semoss.org/ontologies/Relation";
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
		
		( (BigDataEngine) engine).addStatement(newRelationshipType, subpropertyOf, relation, true);
		RDFFileSesameEngine existingBaseEngine = (RDFFileSesameEngine) ( (AbstractEngine) engine).getBaseDataEngine();
		existingBaseEngine.addStatement(newRelationshipType, subpropertyOf, relation, true);
		LOGGER.info("ADDING NEW RELATIONSHIP TRIPLE: " + newRelationshipType + ">>>>>" + subpropertyOf + ">>>>>" + relation + ">>>>>");
	}
	
	public void processNewConceptsAtInstanceLevel(IEngine engine, String subject, String object)
	{
		String pred = RDF.TYPE.toString();
		((BigDataEngine) engine).addStatement(subject, pred, object, true);
		LOGGER.info("ADDING CONCEPT INSTANCE TYPE TRIPLE: " + subject + ">>>>>" + pred + ">>>>>" + object	+ ">>>>>");				
	}

	public void processNewRelationshipsAtInstanceLevel(IEngine engine, String subject, String object) 
	{
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
		((BigDataEngine) engine).addStatement(subject, subpropertyOf, object, true);
		LOGGER.info("ADDING RELATIONSHIP INSTANCE SUBPROPERTY TRIPLE: " + subject + ">>>>>" + subpropertyOf + ">>>>>" + object	+ ">>>>>");
	}
	
	public void addToDataHash(Object[] returnTriple) 
	{
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		innerHash.put(returnTriple[1].toString(), returnTriple[2]);
		if(dataHash.containsKey(returnTriple[0].toString()))
		{
			dataHash.get(returnTriple[0].toString()).putAll(innerHash);
		}
		else
		{
			dataHash.put(returnTriple[0].toString(), innerHash);
		}
	}
	
	public void addToDeleteHash(Object[] returnTriple)
	{
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		innerHash.put(returnTriple[1].toString(), returnTriple[2]);
		if(removeDataHash.containsKey(returnTriple[0].toString()))
		{
			removeDataHash.get(returnTriple[0]).putAll(innerHash);
		}
		else
		{
			removeDataHash.put(returnTriple[0].toString(), innerHash);
		}
	}

	public void addToAllConcepts(String uri)
	{
		String conceptBaseURI = semossConceptBaseURI + Utility.getClassName(uri);
		if(allConcepts.containsKey(conceptBaseURI))
		{
			allConcepts.get(conceptBaseURI).add(uri);
		}
		else
		{
			allConcepts.put(conceptBaseURI, new HashSet<String>());
			allConcepts.get(conceptBaseURI).add(uri);
		}		
	}

	public void addToAllRelationships(String uri)
	{
		String relationBaseURI = semossRelationBaseURI + Utility.getClassName(uri);
		if(allRelations.containsKey(relationBaseURI))
		{
			allRelations.get(relationBaseURI).add(uri);
		}
		else
		{
			allRelations.put(relationBaseURI, new HashSet<String>());
			allRelations.get(relationBaseURI).add(uri);
		}
	}
	
	public void writeToOWL(IEngine engine)
	{
		// get the path to the owlFile
		String owlFileLocation = DIHelper.getInstance().getProperty(engine.getEngineName() +"_" + Constants.OWL); 

		RDFFileSesameEngine existingBaseEngine = (RDFFileSesameEngine) ( (AbstractEngine) engine).getBaseDataEngine();
		RepositoryConnection exportRC = existingBaseEngine.getRc();
		FileWriter fWrite = null;
		try{
			fWrite = new FileWriter(owlFileLocation);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			exportRC.export(owlWriter);
			fWrite.flush();
			owlWriter.close();	
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}finally{
			try{
				if(fWrite!=null)
					fWrite.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// general methods for properties
	public Object[] processSumValues(String sub, String prop, Object value)
	{
		try
		{
			((Literal) value).doubleValue();
		}
		catch(NumberFormatException e)
		{
			//e.printStackTrace();
			this.errorMessage = "Error Processing Sum Double. Please check value of Double. " 
					+ "Error occured processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			LOGGER.debug("ADDING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Double addValue = ( (Literal) value).doubleValue();
			Double currentValue = (Double) innerHash.get(prop);
			value = addValue + currentValue;
			LOGGER.debug("ADJUSTING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		return new Object[]{sub, prop, value};
	}

	public Object[] processConcatString(String sub, String prop, Object value) 
	{
		// replace any tags for properties that are loaded as other data types but should be strings
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#double","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#decimal","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#integer","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#float","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#boolean","");
		value = value.toString().replaceAll("^^<http:--www.w3.org-2001-XMLSchema#dateTime","");
		if(value.toString().startsWith("\"") || value.toString().endsWith("\""))
		{
			value = value.toString().substring(1, value.toString().length()-1); // remove annoying 
		}
		
		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			LOGGER.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + value.toString();
			LOGGER.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
			//e.printStackTrace();
			this.errorMessage = "Error Processing Max/Min Double. Please check value of Double. " 
					+ "Error occured processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			LOGGER.debug("ADDING DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					LOGGER.debug("ADJUSTING MIN DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					LOGGER.debug("ADJUSTING MAX DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
			//e.printStackTrace();
			this.errorMessage = "Error Processing Max/Min Date. Please check value of Date. " 
					+ "Error occured processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{	
			value = (Date) ((XMLGregorianCalendar) ((Literal) value).calendarValue()).toGregorianCalendar().getTime();
			LOGGER.debug("ADDING DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Date oldDate = (Date) innerHash.get(prop);
			Date newDate = (Date) ((XMLGregorianCalendar) ((Literal) value).calendarValue()).toGregorianCalendar().getTime();
			if(!latest)
			{
				if(newDate.before(oldDate))
				{
					// return the value being passed in
					value = newDate;
					LOGGER.debug("ADJUSTING MIN DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
				}
				// if the new value is not to be used, return the originally value already in dataHash
				else
				{
					value = innerHash.get(prop);
				}
			}
			else
			{
				if(newDate.after(oldDate))
				{
					// return the value being passed in
					value = newDate;
					LOGGER.debug("ADJUSTING MAX DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
	
	// UTILITY METHODS ************************************************************************************************************************

	public String getTextAfterFinalDelimeter(String uri, String delimeter)
	{
		if(!uri.equals(""))
		{
			uri = uri.substring(uri.lastIndexOf(delimeter)+1);
		}
		return uri;
	}

	public String getBaseURI(String uri)
	{
		return uri.substring(0, uri.substring(0, uri.substring(0, uri.lastIndexOf("/")).lastIndexOf("/")).lastIndexOf("/"));
	}
}
