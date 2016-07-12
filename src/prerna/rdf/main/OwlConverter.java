package prerna.rdf.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class OwlConverter {

	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		
		try {
			TestUtilityMethods.loadDIHelper();
	
			//TODO: put in correct path for your database!!!
			//TODO: put in correct path for your database!!!
			//TODO: put in correct path for your database!!!
			//TODO: put in correct path for your database!!!
			//TODO: put in correct path for your database!!!
			String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDF.smss";
			
			// generically create the engine based on the smss
			Properties prop = new Properties();
			FileInputStream fileIn = new FileInputStream(engineProp);
			prop.load(fileIn);
			String engineName = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
			fileIn.close();
			
			AbstractEngine coreEngine = (AbstractEngine)Class.forName(engineClass).newInstance();
			coreEngine.setEngineName(engineName);
			coreEngine.openDB(engineProp);
			DIHelper.getInstance().setLocalProperty(engineName, coreEngine);
			
			// and so we begin...
			
			/*
			 * Steps to convert
			 * 1) check if engine owl is converted. if not, we continue
			 * 2) get all current concepts
			 * 3) get all props for each concept
			 * 4) get all relationships between 2 concepts
			 * 5) get the engine type
			 * Utilizing the OWLER
			 * 6) add all the concepts
			 * 7) add all the properties to the concepts
			 * 8) add all the relationships
			 */
			
			RDFFileSesameEngine owlEng = coreEngine.getBaseDataEngine();
			
			// 1) make sure it is in face an old owl
			boolean converted = false;
			
			// the minus is in case we have a relationship called conceptual between 2 nodes
			// since the "conceptual" node is not a rdfs:subClassOf semoss:Concept
			String testNeedsConversion = "SELECT DISTINCT ?conceptual WHERE { "
					+ "{?concept rdfs:subClassOf <http://semoss.org/ontologies/Concept>} "
					+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual} "
					+ "MINUS { ?conceptual rdfs:subClassOf <http://semoss.org/ontologies/Concept> }"
					+ "}";
			
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(owlEng, testNeedsConversion);
			if(wrapper.hasNext()) {
				converted = true;
			}
			
			System.out.println("Is engine converted? " + converted);
			
			if(!converted) {
				System.out.println("Alright, lets convert it");
				
				// actually, in case we f this up
				// we should probably save the old owl file
				System.out.println("Writing OLD OWL file into new file in case we destroy everything");
				String owlPath = coreEngine.getOWL();
				File oldFile = new File(owlPath);
				FileChannel oldFileChannel = new FileInputStream(oldFile).getChannel();
				String saveFileName = oldFile.getName().replace(".OWL", "") + "_OLD.OWL";
				File saveFile = new File(oldFile.getParentFile() + "/" + saveFileName);
				FileChannel saveFileChannel = new FileOutputStream(saveFile).getChannel();
				saveFileChannel.transferFrom(oldFileChannel, 0, oldFileChannel.size());
				System.out.println("Old OWL is now sitting in file located at: " + saveFile.getAbsolutePath());
				oldFileChannel.close();
				saveFileChannel.close();
				
				System.out.println("okay, now we actually convert it");

				// would be more efficient to write out specific queries
				// but less work to use the defined functions on the engine
				
				// 2) get all the concepts
				Vector<String> concepts = coreEngine.getConcepts2(false);
				System.out.println("Found following concepts... " + concepts);
				
				// 3) get all props for concepts
				Map<String, List<String>> conceptProps = new Hashtable<String, List<String>>();
				for(String concept : concepts) {
					List<String> properties = coreEngine.getProperties4Concept2(concept, false);
					conceptProps.put(concept, properties);
				}
				System.out.println("Found following properties for concepts... " + conceptProps);
	
				// 4) get all the relationships
				// only need to capture one direction
				List<String> relationshipInfo = new Vector<String>();
				String relationshipQuery = "SELECT DISTINCT ?fromConcept ?rel ?toConcept WHERE {"
						+ "{?fromConcept rdfs:subClassOf <http://semoss.org/ontologies/Concept>} "
						+ "{?toConcept rdfs:subClassOf <http://semoss.org/ontologies/Concept>} "
						+ "{?rel rdfs:subPropertyOf <http://semoss.org/ontologies/Relation>} "
						+ "{?fromConcept ?rel ?toConcept} "
						+ "FILTER ( ?rel != <http://semoss.org/ontologies/Relation> )"
						+ "}";
				
				wrapper = WrapperManager.getInstance().getRawWrapper(owlEng, relationshipQuery);
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getRawValues();
					relationshipInfo.add(row[0] + "");
					relationshipInfo.add(row[1] + "");
					relationshipInfo.add(row[2] + "");
				}
				
				System.out.println("Found following relationships... " + relationshipInfo);
	
				// we now have all the information we need...
	
				// create the owler to make this process easier
				OWLER owler = new OWLER(coreEngine, owlPath);
				// need to delete the current OWL
				File file = new File(owlPath);
				file.delete();
				
				// 5) get the engine type
				// how we add will depend on if the engine is rdbms vs rdf
				ENGINE_TYPE eType = coreEngine.getEngineType();
				System.out.println("Engine Type is: " + eType);
				
				if(eType.equals(IEngine.ENGINE_TYPE.RDBMS)) {
					// 6) add the concepts
					for(String concept : concepts) {
						if(concept.equals("http://semoss.org/ontologies/Concept")) {
							continue;
						}
						
						String tableName = Utility.getInstanceName(concept);
						String columnName = Utility.getClassName(concept);
						String dataType = coreEngine.getDataTypes(concept).replace("TYPE:", "");
						System.out.println("Adding concept with: tableName=" + tableName + ", columnName=" + columnName
								+ ", dataType=" + dataType);
						owler.addConcept(tableName, columnName, dataType);
					}
					
					// 7) add the properties
					for(String concept : conceptProps.keySet()) {
						if(concept.equals("http://semoss.org/ontologies/Concept")) {
							continue;
						}
						String tableName = Utility.getInstanceName(concept);
						String columnName = Utility.getClassName(concept);
						
						List<String> props = conceptProps.get(concept);
						for(String property : props) {
							String propColumn = Utility.getInstanceName(property);
							String propDataType = coreEngine.getDataTypes(property).replace("TYPE:", "");;
							
							System.out.println("Adding concept property with: tableName=" + tableName + ", propColumn=" + propColumn
								+ ", dataType=" + propDataType);
							owler.addProp(tableName, columnName, propColumn, propDataType);
						}
					}
					
					// 8) add the relationships
					for(int relIndex = 0 ; relIndex < relationshipInfo.size(); relIndex+=3) {
						String fromConcept = relationshipInfo.get(relIndex);
						String relationship = relationshipInfo.get(relIndex+1);
						String toConcept = relationshipInfo.get(relIndex+2);
						
						String fromTableName = Utility.getInstanceName(fromConcept);
						String fromColumnName = Utility.getClassName(fromConcept);
						
						String predicate = Utility.getInstanceName(relationship);
						
						String toTableName = Utility.getInstanceName(toConcept);
						String toColumnName = Utility.getClassName(toConcept);
						
						System.out.println("Adding relationship between " + fromTableName + "." + fromColumnName + " to " + 
								toTableName + "." + toColumnName + " using the predicate " + predicate);
						owler.addRelation(fromTableName, fromColumnName, toTableName, toColumnName, predicate);
					}
				} else {
					// 6) add the concepts
					for(String concept : concepts) {
						if(concept.equals("http://semoss.org/ontologies/Concept")) {
							continue;
						}
						String conceptName = Utility.getInstanceName(concept);
						String dataType = coreEngine.getDataTypes(concept).replace("TYPE:", "");;
						System.out.println("Adding concept=" + conceptName + " with dataType=" + dataType);
						owler.addConcept(conceptName, dataType);
					}
					
					// 7) add the properties
					for(String concept : conceptProps.keySet()) {
						if(concept.equals("http://semoss.org/ontologies/Concept")) {
							continue;
						}
						String conceptName = Utility.getInstanceName(concept);
						
						List<String> props = conceptProps.get(concept);
						for(String property : props) {
							String propertyName = Utility.getInstanceName(property);
							String propDataType = coreEngine.getDataTypes(property).replace("TYPE:", "");;
							
							System.out.println("Adding property to concept=" + conceptName + ", prop=" + propertyName + 
									" , dataType=" + propDataType);
							owler.addProp(conceptName, propertyName, propDataType);
						}
					}
					
					// 8) add the relationships
					for(int relIndex = 0 ; relIndex < relationshipInfo.size(); relIndex+=3) {
						String fromConcept = relationshipInfo.get(relIndex);
						String relationship = relationshipInfo.get(relIndex);
						String toConcept = relationshipInfo.get(relIndex);
						
						String fromConceptName = Utility.getInstanceName(fromConcept);
						String predicate = Utility.getInstanceName(relationship);
						String toConceptName = Utility.getInstanceName(toConcept);
						
						System.out.println("Adding relationship between " + fromConceptName + " to " + 
								toConceptName + " using the predicate " + predicate);
						owler.addRelation(fromConceptName, toConceptName, predicate);
					}
				}
				
				// write the owl out
				owler.export();
				System.out.println("Successfully created new OWL");
				// if not in this main, we would need to reset the engine's owl
				// without shutting it down
//				coreEngine.setOWL(owler.getOwlPath());
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
