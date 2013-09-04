package prerna.ui.components;

import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;

import prerna.om.DBCMVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;

public class RDFEngineHelper {

	Logger logger = Logger.getLogger(getClass());
	// responsible for handling various engine related stuff
	// loads the concepts from the engine into the specified sesame
	public static void loadConceptHierarchy(IEngine fromEngine, String subjects, String objects, GraphPlaySheet ps)
	{
		String conceptHierarchyForSubject = "" ;

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{			
			conceptHierarchyForSubject = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
			"{?Subject ?Predicate ?Object}" + 
			"} BINDINGS ?Subject { " + subjects + objects + " } " +
			"";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			conceptHierarchyForSubject = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{VALUES ?Subject {" + subjects + objects + "}" +
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
			"{?Subject ?Predicate ?Object}" + 
			"}";
		}

		System.err.println("Predicates are " + conceptHierarchyForSubject);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(conceptHierarchyForSubject);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			SesameJenaConstructStatement st = sjsc.next();
			ps.predData.addConcept(st.getObject()+"", st.getSubject()+"");

			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
			//addToJenaModel(st);
			ps.addToSesame(st, false, false);
		}
	}


	public static void loadRelationHierarchy(IEngine fromEngine, String predicates, GraphPlaySheet ps)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "";

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{
			relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
			"} BINDINGS ?Subject { " + predicates + " } " +
			"";// relation hierarchy		
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{ VALUES ?Subject {" + predicates + "}" + 
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
			"}";// relation hierarchy					
		}

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model			
			SesameJenaConstructStatement st = sjsc.next();
			ps.predData.addPredicate2(st.getObject()+"", st.getSubject());
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
			//addToJenaModel(st);
			ps.addToSesame(st, false, false);
		}
	}	

	public static void loadPropertyHierarchy(IEngine fromEngine, String predicates, String containsRelation, GraphPlaySheet ps)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "";

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{
			relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{" +
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
			"} BINDINGS ?Subject { " + predicates + " } " +
			"";// relation hierarchy
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
			"{ VALUES ?Subject {" + predicates + "}" + 
			"{?Subject ?Predicate ?Object}" + 
			"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
			"}";// relation hierarchy					
		}

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model

			SesameJenaConstructStatement st = sjsc.next();
			ps.predData.addPredicate2(st.getObject()+"", st.getSubject());
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype

			//addToJenaModel(st);
			ps.addToSesame(st, false, false);
		}
	}

	public static void genPropertiesRemote(IEngine fromEngine, String subjects, String objects, String predicates, String containsRelation, GraphPlaySheet ps)
	{

		String propertyQuery = "";
		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{
			propertyQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
			"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
			//"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
			"{?Subject ?Predicate ?Object}}" +
			"BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects + " }";
		}
		else
		{
			propertyQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
			"VALUES ?Subject {" + subjects + " " + predicates + " " + objects + "}" +
			"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
			//"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
			"{?Subject ?Predicate ?Object}}";			
		}


		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(fromEngine);
		sjsc.setQuery(propertyQuery);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement sct = sjsc.next();

			String subject = sct.getSubject();
			String predicate = sct.getPredicate();
			Object obj = sct.getObject();

			// add the property
			ps.predData.addProperty(predicate, predicate);

			// try to see if the predicate here has been designated to be a relation
			// if so then add it as a relation
			// if the object is a simple type
			// then it would be Predicate/SimpleType
			ps.addProperty(subject, obj, predicate);
			ps.addToSesame(sct, false, false);
		}
	}

	public static void genPropertiesLocal(RepositoryConnection rc, String containsRelation, GraphPlaySheet ps)
	{

		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		String propertyQuery =  "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
		"{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
		//"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
		"{?Subject ?Predicate ?Object}}";


		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(sesameEngine);
		sjsc.setQuery(propertyQuery);
		sjsc.execute();

		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement sct = sjsc.next();

			String subject = sct.getSubject();
			String predicate = sct.getPredicate();
			Object obj = sct.getObject();

			// add the property
			ps.predData.addProperty(predicate, predicate);

			// try to see if the predicate here has been designated to be a relation
			// if so then add it as a relation
			// if the object is a simple type
			// then it would be Predicate/SimpleType
			ps.addProperty(subject, obj, predicate);
			//System.err.println("LOCAL Props " + subject + obj + predicate);
			ps.addToSesame(sct, false, false);
		}
	}
	
	public static void loadLabels(IEngine fromEngine, String subjects, GraphPlaySheet ps)
	{
		// loads all of the labels
		// http://www.w3.org/2000/01/rdf-schema#label
		String labelQuery = "";
		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{			
			labelQuery = "SELECT DISTINCT ?Subject ?Label WHERE " +
			"{" +
			"{?Subject <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
			"} BINDINGS ?Subject { " + subjects + " } " +
			"";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			labelQuery = "SELECT DISTINCT ?Subject ?Label WHERE " +
			"{VALUES ?Subject {" + subjects + "}" +
			"{?Subject <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
			"}";
		}
		System.err.println("Query is " + labelQuery);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(fromEngine);
		sjsw.setQuery(labelQuery);
		sjsw.executeQuery();
		sjsw.getVariables();
		
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement st = sjsw.next();
			String subject = st.getRawVar("Subject") + "";
			String label = st.getVar("Label") + "";
			
			DBCMVertex vert = ps.vertStore.get(subject);
			vert.setProperty(Constants.VERTEX_NAME, label);
		}

	}


}
