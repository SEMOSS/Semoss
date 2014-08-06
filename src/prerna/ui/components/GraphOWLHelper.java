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
package prerna.ui.components;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;

import prerna.om.GraphDataModel;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * This class is responsible for handling various components related to the engine for the OWL file.
 * It loads concepts from the engine into the specified sesame engine.
 */
public class GraphOWLHelper {

	static final Logger logger = LogManager.getLogger(GraphOWLHelper.class.getName());

	/**
	 * Loads the hierarchy of concepts (subjects).
	 * @param rc 		Repository connection- interface for updating data in and performing queries on a Sesame repository.
	 * @param subjects 	String
	 * @param objects 	String
	 * @param ps 		Graph playsheet.
	 */
	public static void loadConceptHierarchy(RepositoryConnection rc, String subjects, String objects, GraphDataModel ps)
	{
		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		String conceptHierarchyForSubject = "SELECT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
			"{?Subject ?Predicate ?Object}" + 
			"} BINDINGS ?Subject { " + subjects + objects + " } " +
			"";
		
		System.err.println("Predicates are " + conceptHierarchyForSubject);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(conceptHierarchyForSubject);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			SesameJenaConstructStatement st = sjsc.next();
			PropertySpecData psd = ps.getPredicateData();
			psd.addConcept(st.getObject()+"", st.getSubject()+"");

		}
	}


	/**
	 * Loads the hierarchy of relations (predicates).
	 * @param rc Repository connection- interface for updating data in and performing queries on a Sesame repository.
	 * @param predicates 	Type of predicates.
	 * @param ps GraphPlaySheet
	 */
	public static void loadRelationHierarchy(RepositoryConnection rc, String predicates, GraphDataModel ps)
	{
		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		// same concept as the subject, but only for relations
		String relationHierarchy = "SELECT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
			"} BINDINGS ?Subject { " + predicates + " } " +
			"";// relation hierarchy		
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model			
			SesameJenaConstructStatement st = sjsc.next();
			PropertySpecData psd = ps.getPredicateData();
			psd.addPredicate2(st.getObject()+"", st.getSubject());
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
		}
	}	

	/**
	 * Loads the hierarchy of properties.
	 * @param rc Repository connection- interface for updating data in and performing queries on a Sesame repository.
	 * @param predicates 		Predicates.
	 * @param containsRelation 	Type of relation.
	 * @param ps GraphPlaySheet
	 */
	public static void loadPropertyHierarchy(RepositoryConnection rc, String predicates, String containsRelation, GraphDataModel ps)
	{
		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		// same concept as the subject, but only for relations
		String relationHierarchy = "SELECT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
			"} BINDINGS ?Subject { " + predicates + " } " +
			"";// relation hierarchy
		

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model

			SesameJenaConstructStatement st = sjsc.next();
			PropertySpecData psd = ps.getPredicateData();
			psd.addPredicate2(st.getObject()+"", st.getSubject());
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
		}
	}

}
