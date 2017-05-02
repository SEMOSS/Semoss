package prerna.rdf.main;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class NodeRenamer {
	
	public NodeRenamer() {

	}

	public void addUpTriples(List<Object[]> triples, IEngine engine, String newUri, String newName) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] data = triples.get(i);
			// we replace the subject with the new uri we are adding
			String subject = newUri;
			String predicate = data[1].toString();
			Object object = data[2];
			boolean concept = (boolean) data[3];
			
			//handles if the instance of the object is a date. The doAction (used for add and delete, needs a specific date object for dates)
			try {
				object = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").parse((String) data[2]);
			} catch (Exception e) {

			}
		
			//TODO: Discuss if this can be handled in the storevalues() instead
			if(predicate.equals(RDFS.LABEL.stringValue())) {
				object = newName;
				concept = false;
			}
			
//			if (predicate.equals(RDFS.LABEL.stringValue())) {
				System.out.println("Add Up Subject: " + subject);
				System.out.println("Add Up Predicate: " + predicate);
				System.out.println("Add Up Object: " + object.toString());
				System.out.println("Is Concept: " + concept);
				System.out.println("");
//			}

			addData(subject, predicate, object, concept, engine);
		}
	}
	
	public void addDownTriples(List<Object[]> triples, IEngine engine, String newUri) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] data = triples.get(i);
			String subject = data[0].toString();
			String predicate = data[1].toString();
			// replace the object with the new uri we are adding
			Object object = newUri;
			boolean concept = (boolean) data[3];
			
			//handles if the new istance being added is a date. The doAction (used for add and delete, needs a specific date object for dates)
			try {
				object = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").parse((String) data[2]);
			} catch (Exception e) {

			}
			
//			if (predicate.equals(RDFS.LABEL.stringValue())) {
				System.out.println("Add Down Subject: " + subject);
				System.out.println("Add Down Predicate: " + predicate);
				System.out.println("Add Down Object: " + object.toString());
				System.out.println("Is Concept: " + concept);
				System.out.println("");
//			}
			
			addData(subject, predicate, object, concept, engine);
		}
	}

	public void deleteTriples(List<Object[]> triples, IEngine engine) {
		int size = triples.size();
		for(int i = 0; i < size; i++) {
			Object[] data = triples.get(i);
			String subject = data[0].toString();
			String predicate = data[1].toString();
			Object object = data[2];
			boolean concept = (boolean) data[3];
			
			//handles if the instance of the object is a date. The doAction (used for add and delete, needs a specific date object for dates)
			try {
				object = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'").parse((String) data[2]);
			} catch (Exception e) {

			}

			
//			if (predicate.equals(RDFS.LABEL.stringValue())) {
				System.out.println("Delete Subject: " + subject);
				System.out.println("Delete Predicate: " + predicate);
				System.out.println("Delete Object: " + object.toString());
				System.out.println("Is Concept: " + concept);
				System.out.println("");
//			}
			deleteData(subject, predicate, object, concept, engine);
		}
	}

	/**
	 * Go through the iterator and add it to the list
	 * @param it
	 * @param listToAdd
	 */
	public void storeValues(IRawSelectWrapper it, List<Object[]> listToAdd) {
		while(it.hasNext()) {
			IHeadersDataRow datarow = it.next();
			//instance uri
			Object[] rawTriple = datarow.getRawValues();
			//only instance name
			Object[] cleanTriple = datarow.getValues();
//			System.out.println("FOUND TRIPLE ::: " + Arrays.toString(rawTriple));

			// we need to also consider
			// if the last thing is a literal
			// or if it is a uri
			Object[] adjustedTriple = new Object[4];
			for(int i = 0; i < 3; i++) {
				adjustedTriple[i] = rawTriple[i];
			}
			// if its a literal
			// use the clean value
			// checks the relationship to see if this a property or an RDF label
			// NOTE: There is a "/" after contains, because some concepts have the relationship: contains, so need to make sure just properties are grabbed
			if(adjustedTriple[1].toString().startsWith("http://semoss.org/ontologies/Relation/Contains/") 
					|| adjustedTriple[1].toString().equals(RDFS.LABEL.toString()) ) {
				adjustedTriple[2] = cleanTriple[2];
				adjustedTriple[3] = false;
			} else {
				adjustedTriple[3] = true;
			}
			
			listToAdd.add(adjustedTriple);
		}
	}

	/**
	 * Insert the triple into the local master database
	 * @param subject                   The subject URI
	 * @param predicate                       The predicate URI
	 * @param object                    The object (either URI or Literal)
	 * @param concept                   Boolean true if object is concept and false is object is literal
	 * @param engine                    The local master engine to insert into
	 */
	public void addData(String subject, String predicate, Object object, boolean concept, IEngine engine)
	{
		Object [] statement = new Object[4];
		statement[0] = subject;
		statement[1] = predicate;
		statement[2] = object;
		statement[3] = new Boolean(concept);

		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, statement);
	}

	/**
	 * Insert the triple into the local master database
	 * @param subject                   The subject URI
	 * @param predicate                       The predicate URI
	 * @param object                    The object (either URI or Literal)
	 * @param concept                   Boolean true if object is concept and false is object is literal
	 * @param engine                    The local master engine to insert into
	 */
	public void deleteData(String subject, String predicate, Object object, boolean concept, IEngine engine)
	{
		Object [] statement = new Object[4];
		statement[0] = subject;
		statement[1] = predicate;
		statement[2] = object;
		statement[3] = new Boolean(concept);

		engine.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, statement);
	}



}
