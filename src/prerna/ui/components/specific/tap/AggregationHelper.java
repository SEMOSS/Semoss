/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Functions needed for data aggregation procedures.
 */
public class AggregationHelper implements IAggregationHelper {

	private static final Logger classLogger = LogManager.getLogger(AggregationHelper.class.getName());

	public HashMap<String, HashMap<String, Object>> dataHash = new HashMap<String, HashMap<String, Object>>();
	public HashMap<String, HashMap<String, Object>> removeDataHash = new HashMap<String, HashMap<String, Object>>();
	public HashMap<String, Set<String>> allRelations = new HashMap<String, Set<String>>();
	public HashMap<String, Set<String>> allConcepts = new HashMap<String, Set<String>>();
	public HashMap<String, String> allLabels = new HashMap<String, String>();
	
	public String errorMessage = "";

	// Fundamental Methods
	public void processData(IDatabaseEngine engine, HashMap<String, HashMap<String, Object>> data)
	{
		for( String sub : data.keySet())
		{
			for ( String pred : data.get(sub).keySet())
			{
				if(pred.contains("/Concept/")) {
					System.out.println(":error");
				}
				
				Object obj = data.get(sub).get(pred);
				boolean conceptTriple = true;
				if( pred.contains("Relation/Contains"))
				{
					conceptTriple = false;
				}
				( (BigDataEngine) engine).addStatement(new Object[]{sub, pred, obj, conceptTriple});
				classLogger.info("ADDING INTO " + Utility.cleanLogString(engine.getEngineId()) + ": " + Utility.cleanLogString(sub) + ">>>>>" + Utility.cleanLogString(pred) + ">>>>>" + obj + ">>>>>");
			}
		}
	}
	
	public void deleteData(IDatabaseEngine engine, HashMap<String, HashMap<String, Object>> data)
	{
		for( String sub : data.keySet())
		{
			for ( String pred : data.get(sub).keySet())
			{
				Object obj = data.get(sub).get(pred);
				boolean conceptTriple = true;
				if( pred.contains("Relation/Contains"))
				{
					conceptTriple = false;
				}
				( (BigDataEngine) engine).removeStatement(new Object[]{sub, pred, obj, conceptTriple});
				classLogger.info("REMOVING FROM " + Utility.cleanLogString(engine.getEngineId()) + ": " + Utility.cleanLogString(sub) + ">>>>>" + Utility.cleanLogString(pred) + ">>>>>" + obj + ">>>>>");
			}
		}		
	}
	
	public void processInstanceDataRelations(List<Object[]> data, HashMap<String, HashMap<String, Set<String>>> baseRelations) {
		for(Object[] triple: data){
			createBaseRelationsHash(triple, baseRelations);
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
			addToAllRelationships(triple[1].toString());
			addToAllConcepts(triple[2].toString());
		}
	}
	
	public void processInstancePropOnRelationshipData(List<Object[]> data, IDatabaseEngine engine){
		Set<String> storePropURI = new HashSet<String>();
		for(Object[] triple: data){
			storePropURI.add(triple[1].toString());
			addToDataHash(triple);
			addToAllRelationships(triple[0].toString());
		}
		//add http://semoss.org/ontology/Relation/Contains/PropName -> RDF:TYPE -> http://semoss.org/ontology/Relation/Contains
		for(String propURI: storePropURI) {
			processNewConceptsAtInstanceLevel(engine, propURI, semossPropertyBaseURI.substring(0, semossPropertyBaseURI.length()-1));
		}
	}
	
	public void processInstancePropOnNodeData(List<Object[]> data, IDatabaseEngine engine){
		Set<String> storePropURI = new HashSet<String>();
		for(Object[] triple: data){
			storePropURI.add(triple[1].toString());
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
		}
		//add http://semoss.org/ontology/Relation/Contains/PropName -> RDF:TYPE -> http://semoss.org/ontology/Relation/Contains
		for(String propURI: storePropURI) {
			processNewConceptsAtInstanceLevel(engine, propURI, semossPropertyBaseURI.substring(0, semossPropertyBaseURI.length()-1));
		}
	}

	public void processNewSubclass(IDatabaseEngine engine, String parentType, String childType)
	{
		String subclassOf = RDFS.SUBCLASSOF.toString();
		( (BigDataEngine) engine).addStatement(new Object[]{childType, subclassOf, parentType, true});
		classLogger.info("ADDING NEW SUBCLASS TRIPLE: " + childType + ">>>>>" + subclassOf + ">>>>>" + parentType + ">>>>>");
	}
	
	public void processActiveSystemSubclassing(IDatabaseEngine engine, Set<String> data){
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/System", "http://semoss.org/ontologies/Concept/ActiveSystem");
		for(String sysURI : data) {
			processNewConceptsAtInstanceLevel(engine, sysURI, "http://semoss.org/ontologies/Concept/ActiveSystem");
		}
	}

	public void processNewConcepts(IDatabaseEngine engine, String newConceptType)
	{
		String concept = "http://semoss.org/ontologies/Concept";
		String subclassOf = RDFS.SUBCLASSOF.toString();
		
		( (BigDataEngine) engine).addStatement(new Object[]{newConceptType, subclassOf, concept, true});
		try(WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.addToBaseEngine(new Object[]{newConceptType, subclassOf, concept, true});
			classLogger.info(Utility.cleanLogString("ADDING NEW CONCEPT TRIPLE: " + newConceptType + ">>>>>" + subclassOf + ">>>>>" + concept + ">>>>>"));
		} catch (IOException | InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public void processNewRelationships(IDatabaseEngine engine, String newRelationshipType) 
	{
		String relation = "http://semoss.org/ontologies/Relation";
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
		
		( (BigDataEngine) engine).addStatement(new Object[]{newRelationshipType, subpropertyOf, relation, true});
		try(WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.addToBaseEngine(new Object[]{newRelationshipType, subpropertyOf, relation, true});
			classLogger.info(Utility.cleanLogString("ADDING NEW RELATIONSHIP TRIPLE: " + newRelationshipType + ">>>>>" + subpropertyOf + ">>>>>" + relation + ">>>>>"));
		} catch (IOException | InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void processNewConceptsAtInstanceLevel(IDatabaseEngine engine, String subject, String object)
	{
		String pred = RDF.TYPE.toString();
		((BigDataEngine) engine).addStatement(new Object[]{subject, pred, object, true});
		classLogger.info(Utility.cleanLogString("ADDING CONCEPT INSTANCE TYPE TRIPLE: " + subject + ">>>>>" + pred + ">>>>>" + object	+ ">>>>>"));				
	}

	public void processNewRelationshipsAtInstanceLevel(IDatabaseEngine engine, String subject, String object) 
	{
		String subpropertyOf = RDFS.SUBPROPERTYOF.toString();
		((BigDataEngine) engine).addStatement(new Object[]{subject, subpropertyOf, object, true});
		classLogger.info(Utility.cleanLogString("ADDING RELATIONSHIP INSTANCE SUBPROPERTY TRIPLE: " + subject + ">>>>>" + subpropertyOf + ">>>>>" + object	+ ">>>>>"));
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
	
	public void processAllConceptTypeTriples(IDatabaseEngine engine)
	{
		for(String newConcept : allConcepts.keySet()) {
			processNewConcepts(engine, newConcept);
			Set<String> instanceSet = allConcepts.get(newConcept);
			for(String newInstance : instanceSet) {
				processNewConceptsAtInstanceLevel(engine, newInstance, newConcept);
			}
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
	
	public void processAllRelationshipSubpropTriples(IDatabaseEngine engine)
	{
		for(String newRelationship : allRelations.keySet()) {
			processNewRelationships(engine, newRelationship);
			Set<String> instanceSet = allRelations.get(newRelationship);
			for(String newRelInstance : instanceSet) {
				processNewRelationshipsAtInstanceLevel(engine, newRelInstance, newRelationship);
			}
		}
	}
	
	public void addToAllLabel(String uri) {
		if(!allLabels.containsKey(uri)) {
			String instanceName = Utility.getInstanceName(uri);
			allLabels.put(uri, instanceName);
		}
	}
	
	public void processLabel(IDatabaseEngine engine) {
		String label = RDFS.LABEL.toString();
		for(String instanceNodeURI : allLabels.keySet()) {
			((BigDataEngine) engine).addStatement(new Object[]{instanceNodeURI, label, allLabels.get(instanceNodeURI), false});
		}
	}
	
	public void writeToOWL(IDatabaseEngine engine)
	{
		try(WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.export();
		} catch (IOException | InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void createBaseRelationsHash(Object[] triple, HashMap<String, HashMap<String, Set<String>>> baseRelations) {
		String subjectBaseURI = semossConceptBaseURI + Utility.getClassName(triple[0].toString());
		String predicateBaseURI = semossRelationBaseURI + Utility.getClassName(triple[1].toString());
		String objectBaseURI = semossConceptBaseURI + Utility.getClassName(triple[2].toString());
		if(baseRelations.containsKey(subjectBaseURI)) {
			HashMap<String, Set<String>> innerHash = baseRelations.get(subjectBaseURI);
			if(innerHash.containsKey(predicateBaseURI)) {
				innerHash.get(predicateBaseURI).add(objectBaseURI);
			} else {
				Set<String> list = new HashSet<String>();
				list.add(objectBaseURI);
				innerHash.put(predicateBaseURI, list);
			}
		} else {
			Set<String> list = new HashSet<String>();
			list.add(objectBaseURI);
			HashMap<String, Set<String>> innerHash = new HashMap<String, Set<String>>();
			innerHash.put(predicateBaseURI, list);
			baseRelations.put(subjectBaseURI, innerHash);
		}
	}
	
	public void writeToOWL(IDatabaseEngine engine, HashMap<String, HashMap<String, Set<String>>> baseRelations) throws RepositoryException, RDFHandlerException 
	{
		// get the path to the owlFile
		String owlFileLocation = DIHelper.getInstance().getProperty(engine.getEngineId() +"_" + Constants.OWL); 

		try(WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			for(String subjectURI : baseRelations.keySet()) 
			{
				HashMap<String, Set<String>> predicateURIHash = baseRelations.get(subjectURI);
				for(String predicateURI : predicateURIHash.keySet()) 
				{
					Set<String> objectURIList = predicateURIHash.get(predicateURI);
					for(String objectURI : objectURIList) 
					{
						owlEngine.addToBaseEngine(new Object[]{subjectURI, predicateURI, objectURI, true});
					}
				}
			}
			owlEngine.export();
		} catch (IOException | InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
					+ "Error occurred processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			classLogger.debug("ADDING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Double addValue = ( (Literal) value).doubleValue();
			Double currentValue = (Double) innerHash.get(prop);
			value = addValue + currentValue;
			classLogger.debug("ADJUSTING SUM:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
			classLogger.debug("ADDING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
		}
		else
		{
			innerHash = dataHash.get(sub);
			Object currentString = innerHash.get(prop);
			value = currentString.toString().substring(0, currentString.toString().length()-1) + ";" + value.toString();
			classLogger.debug("ADJUSTING STRING:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					+ "Error occurred processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{
			value = ((Literal) value).doubleValue();
			classLogger.debug("ADDING DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					classLogger.debug("ADJUSTING MIN DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					classLogger.debug("ADJUSTING MAX DOUBLE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					+ "Error occurred processing: " + sub + " >>>> " + prop + " >>>> " + value;	
			return new String[]{""};
		}

		HashMap<String, Object> innerHash = new HashMap<String, Object>();
		if(!dataHash.containsKey(sub) || !dataHash.get(sub).containsKey(prop))
		{	
			value = (Date) ((XMLGregorianCalendar) ((Literal) value).calendarValue()).toGregorianCalendar().getTime();
			classLogger.debug("ADDING DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					classLogger.debug("ADJUSTING MIN DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
					classLogger.debug("ADJUSTING MAX DATE:     " + sub + " -----> {" + prop + " --- " + value + "}");
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
