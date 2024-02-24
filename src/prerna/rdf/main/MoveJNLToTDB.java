package prerna.rdf.main;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.tdb2.TDB2Factory;

import prerna.engine.impl.rdf.BigDataEngine;
import prerna.test.TestUtilityMethods;

public class MoveJNLToTDB {

	public static void main(String[] args) throws Exception {
		createTDB();
//		readOnly();
	}
	
	private static void createTDB() throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		
		BigDataEngine engine = new BigDataEngine();
		engine.open("C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss");
		
		String allTriples = "select ?s ?p ?o where { ?s ?p ?o } ";
		org.openrdf.query.TupleQueryResult tqr = (org.openrdf.query.TupleQueryResult) engine.execQuery(allTriples);
		
		Dataset dataset = TDB2Factory.connectDataset("C:\\workspace\\Semoss_Dev\\TEST_CREATION");
		// Start a transaction
        dataset.begin(ReadWrite.WRITE);
		Model model = dataset.getDefaultModel();
		
		List<String> ignoredTriples = new ArrayList<String>();
		
		long start = System.currentTimeMillis();
		
		long numTriples = 0;
		while(tqr.hasNext()) {
			org.openrdf.query.BindingSet bs = tqr.next();
			org.openrdf.model.Value subject = bs.getValue("s");
			org.openrdf.model.Value predicate = bs.getValue("p");
			org.openrdf.model.Value object = bs.getValue("o");
			numTriples++;
			
			Resource newSubject = model.createResource(subject.stringValue());
			Property newPredicate = model.createProperty(predicate.stringValue());
			RDFNode newObject = null;
			if(object instanceof org.openrdf.model.Literal) {
				org.openrdf.model.Literal literal = (org.openrdf.model.Literal) object;
				org.openrdf.model.URI dataType = literal.getDatatype();
				if(dataType == null) {
					newObject = model.createLiteral(literal.getLabel());
				} else {
					if(org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil.isStringLiteral(literal)) {
						newObject = model.createLiteral(literal.getLabel());
					} else if(dataType.getLocalName().equals("double")) {
				        newObject = ResourceFactory.createTypedLiteral(((org.openrdf.model.Literal) object).doubleValue());
					} else if(dataType.getLocalName().equals("float")) {
				        newObject = ResourceFactory.createTypedLiteral(((org.openrdf.model.Literal) object).floatValue());
					} else if(dataType.getLocalName().equalsIgnoreCase("boolean")) {
				        newObject = ResourceFactory.createTypedLiteral(((org.openrdf.model.Literal) object).booleanValue());
					} else if(dataType.getLocalName().equalsIgnoreCase("dateTime")) {
				        newObject = ResourceFactory.createTypedLiteral(((org.openrdf.model.Literal) object).calendarValue().toString(), XSDDatatype.XSDdateTime);
					} else if(dataType.getLocalName().equalsIgnoreCase("date")) {
				        newObject = ResourceFactory.createTypedLiteral(((org.openrdf.model.Literal) object).calendarValue().toString(), XSDDatatype.XSDdate);
					}
					else {
						System.out.println("ignoring literal for now " + literal);
						ignoredTriples.add(subject + " : " + predicate + " : " + object);
						continue;
					}
				}
			} else {
				newObject = model.createResource(object.stringValue());
			}
			
			model.add(newSubject, newPredicate, newObject);
		}
		// Commit the transaction
        dataset.commit();
        dataset.end();
        System.out.println("Total triples added = " + numTriples);
        
		long end = System.currentTimeMillis();
		// print what we loaded in
		
		long startPrint = System.currentTimeMillis();
		printModel(dataset);
		long endPrint = System.currentTimeMillis();
		
		if(ignoredTriples.isEmpty()) {
			System.out.println("All triples accounted for");
		} else {
			for(String s : ignoredTriples) {
				System.out.println("IGNORED TRIPLE >>> " + s);
			}
		}
		
		System.out.println("Time to create the model  = " + (end-start)+"ms");
		System.out.println("Time to print the model  = " + (endPrint-startPrint)+"ms");

		engine.close();
	}
	
	private static void readOnly() {
		Dataset dataset = TDB2Factory.connectDataset("C:\\workspace\\Semoss_Dev\\TEST_CREATION");
		long startPrint = System.currentTimeMillis();
		printModel(dataset);
		long endPrint = System.currentTimeMillis();
		System.out.println("Time to print the model  = " + (endPrint-startPrint)+"ms");
	}
	
	/**
	 * 
	 * @param model
	 */
	private static void printModel(Dataset dataset) {
        dataset.begin(ReadWrite.READ);
		Model model = dataset.getDefaultModel();
		// see what is in the model
		StmtIterator iter = model.listStatements();
		// print out the predicate, subject and object of each statement
		long numTriples = 0;
		while (iter.hasNext()) {
		    Statement stmt      = iter.nextStatement();  // get next statement
		    Resource  subject   = stmt.getSubject();     // get the subject
		    Property  predicate = stmt.getPredicate();   // get the predicate
		    RDFNode   object    = stmt.getObject();      // get the object

//		    System.out.println(subject + " : " + predicate + " : " + object);
		    numTriples++;
		}
		System.out.println("Total triples in TDB = " + numTriples);
		dataset.end();
	}
	
}
