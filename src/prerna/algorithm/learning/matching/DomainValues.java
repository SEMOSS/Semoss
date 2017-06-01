package prerna.algorithm.learning.matching;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.openrdf.model.vocabulary.RDF;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

/**
 * This class is used to print out the unique instance values of every
 * concept/property of an engine to a .txt file
 */
public class DomainValues {

	// Used for separating engine from concept or property names
	public static final String ENGINE_CONCEPT_PROPERTY_DELIMETER = ";";


	/**
	 * This method gets the list of concepts uri 
	 * @param engine
	 * @return concept uri
	 */
	public List<String> getConceptList(IEngine engine){
		return engine.getConcepts(false);
	}
	
	
	/**
	 * Returns the physical uri 
	 * @param conceptName
	 * @param engine
	 * @return
	 */
	public static String getConceptURI(String conceptName, IEngine engine, boolean conceptual) {
		List<String> concepts = engine.getConcepts(conceptual);
		String conceptURI = "";
		for (String concept : concepts) {
			if (determineCleanConceptName(concept, engine).equals(conceptName)) {
				conceptURI = concept;
			}
		}
		return conceptURI;
	}
	
	// TODO getConcepts with false, should return physical - then we pass into getProps, which assumes conceptual
	public static String getPropertyURI(String propertyName, String conceptName, IEngine engine, boolean conceptual) {
		String conceptURI = getConceptURI(conceptName, engine, conceptual);
		List<String> properties = engine.getProperties4Concept(conceptURI, conceptual);
		String propertyURI = "";
		for (String property : properties) {
			if (determineCleanPropertyName(property, engine).equals(propertyName)) {
				propertyURI = property;
			}
		}
		return propertyURI;
	}
	
	/**
	 * Gets the list of properties for a concept
	 * @param engine
	 * @param concept uri
	 * @return properties
	 */
	public List<String> getPropertyList(IEngine engine, String concept) {
		return engine.getProperties4Concept(concept, false);
	}
	
	/**
	 * Export concept/concept property instance values from an engine to the
	 * specified output folder as engine;concept;property.txt for each concept
	 * or property
	 * 
	 * @param engine
	 * @parm outputFolder
	 * @param compareProperties
	 *            add or remove concept properties
	 */
	public void exportInstanceValues(IEngine engine, String outputFolder, boolean exportProperty, String instanceCountFile) {
		String engineName = engine.getEngineName();
		
		// Grab all the concepts that exist in the database
		// Process each concept
		List<String> concepts = getConceptList(engine);
		
		//Used to write out instance count to instanceCountFile
		ArrayList<String> instanceCount = new ArrayList<String>();
		for (String concept : concepts) {

			// Ignore the default concept node...
			if (concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}

			// Grab the unique values for the concept
			HashSet<String> uniqueConceptValues = retrieveConceptUniqueValues(concept, engine);
			List<Object> conceptValues = retrieveConceptValues(concept, engine);
			
			// Sometimes this list is empty when users create databases with
			// empty fields that are meant to filled in via forms
			if (uniqueConceptValues.isEmpty()) {
				continue;
			}

			// Write the unique instances to a file
			String cleanConcept = determineCleanConceptName(concept, engine);
			String conceptId = engineName + ENGINE_CONCEPT_PROPERTY_DELIMETER + cleanConcept
					+ ENGINE_CONCEPT_PROPERTY_DELIMETER;
			writeToFile(outputFolder + "\\" + conceptId + ".txt", uniqueConceptValues);
			
			//build instanceCountFile
			String metadataRow = conceptId + "," + conceptValues.size(); 
			boolean hasProperties = false;

			// If the user wants to compare properties,
			// then proceed to the concept's properties
			if (exportProperty) {

				// Grab all the properties that exist for the concept

				List<String> properties = getPropertyList(engine, concept);

				// If there are no properties, go onto the next concept
				if (properties.isEmpty()) {
					metadataRow += "," + 0;
					instanceCount.add(metadataRow);
					continue;
				}

				// Process each property uri
				for (String property : properties) {
					HashSet<String> uniquePropertyValues = retrievePropertyUniqueValues(concept, property, engine);
					if (!uniquePropertyValues.isEmpty()) {
						hasProperties = true;
						String cleanProperty = determineCleanPropertyName(property, engine);
						String propertyId = conceptId + cleanProperty;
						writeToFile(outputFolder + "\\" + propertyId + ".txt", uniquePropertyValues);
					}
				}
			}
			if(hasProperties) {
				metadataRow += "," + 1;
			}
			instanceCount.add(metadataRow);


		}
		
		//write out instance countFile

	    try {
			FileWriter fw = new FileWriter(instanceCountFile,true);
			for(int i =0; i < instanceCount.size(); i++) {
				fw.write(instanceCount.get(i));
				if(i < instanceCount.size() - 1) {
					fw.write("\n");
				}
			}
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void writeToCSV(String path, Object[] headers, ArrayList<Object[]> data) {
		// write source to file
		StringBuilder sv = new StringBuilder();
		for (int i = 0; i < headers.length; i++) {
			sv.append(headers[i].toString());
			if (i < headers.length - 1) {
				sv.append(",");

			}

		}
		sv.append("\n");

		for (int j = 0; j < data.size(); j++) {
			Object[] rowValue = data.get(j);
			for (int i = 0; i < rowValue.length; i++) {
				Object value = rowValue[i];
				if (value != null) {
					sv.append(rowValue[i].toString());
				} else {
					sv.append("");
				}
				if (i < rowValue.length - 1) {
					sv.append(",");
				}
			}
			if (j < data.size() - 1) {
				sv.append("\n");
			}
		}

		PrintWriter printSource;
		try {
			printSource = new PrintWriter(new File(path));
			printSource.write(sv.toString());
			printSource.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	private static void writeToFile(String filePath, HashSet<String> domainValues) {
		try {
			FileWriter fileWriter = new FileWriter(filePath);
			for (String s : domainValues) {

				// Replace everything that is not a letter, number, or space
				// with an underscore
				// I am not sure which characters are used to delimit instances
				// in the R package
				// But I do know that underscores are not used
				if (s != null) {
					fileWriter.write(s.replaceAll("[^A-Za-z0-9 ]", "_") + " ");
				}
			}
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * This method gets unique concept instance values
	 *  
	 * @param uri
	 * @param engine
	 * @return
	 */
	public static HashSet<String> retrieveConceptUniqueValues(String uri, IEngine engine) {
		// This works for concepts for both RDF and RDBMS
		return getUniqueEntityOfType(uri, engine);
	}
	
	public static List<Object> retrieveConceptValues(String uri, IEngine engine) {
		return engine.getEntityOfType(uri);
	}
	
	public static List<Object> retrieveCleanPropertyValues(String conceptURI, String propertyURI, IEngine engine) {
		List<Object> allPropValues = new Vector<Object>();
		if (engine instanceof BigDataEngine) {
			String query = "SELECT DISTINCT ?property WHERE { {?x <" + RDF.TYPE + "> <" + conceptURI + "> } { ?x <"
					+ propertyURI + "> ?property} }";
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			while (wrapper.hasNext()) {
				ISelectStatement selectStatement = wrapper.next();
				//clean property
				String propertyUri = selectStatement.getVar("property").toString();
//				propertyUri =  determineCleanPropertyName(propertyUri, engine);
				allPropValues.add(propertyUri);
			}
		} else {
			allPropValues = engine.getEntityOfType(propertyURI);
		}
		return allPropValues;
	}
	public static List<Object> retrievePropertyValues(String conceptURI, String propertyURI, IEngine engine) {
		List<Object> allPropValues = new Vector<Object>();
		if (engine instanceof BigDataEngine) {
			String query = "SELECT DISTINCT ?property WHERE { {?x <" + RDF.TYPE + "> <" + conceptURI + "> } { ?x <"
					+ propertyURI + "> ?property} }";
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			while (wrapper.hasNext()) {
				ISelectStatement selectStatement = wrapper.next();
				allPropValues.add(selectStatement.getVar("property").toString());
			}
		} else {
			allPropValues = engine.getEntityOfType(propertyURI);
		}
		return allPropValues;
	}

	

	public static HashSet<String> retrievePropertyUniqueValues(String conceptURI, String propertyURI, IEngine engine) {
		HashSet<String> uniquePropertyValues = new HashSet<String>();

		// There is no way to get the list of properties for a
		// specific concept in RDF through the interface
		// Create a query using the concept and the property name
		// TODO fix this through the interface
		if (engine instanceof BigDataEngine) {
			String query = "SELECT DISTINCT ?property WHERE { {?x <" + RDF.TYPE + "> <" + conceptURI + "> } { ?x <"
					+ propertyURI + "> ?property} }";
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			while (wrapper.hasNext()) {
				ISelectStatement selectStatement = wrapper.next();
				uniquePropertyValues.add(selectStatement.getVar("property").toString());
			}
		} else {
			uniquePropertyValues = getUniqueEntityOfType(propertyURI, engine);
		}
		return uniquePropertyValues;
	}

	public static HashSet<String> getUniqueEntityOfType(String type, IEngine engine) {

		// Get all the instances for the concept
		List<Object> allValues = engine.getEntityOfType(type);
		// Push all the instances into a set,
		// because we are only interested in unique values
		HashSet<String> uniqueValues = new HashSet<String>();
		for (Object value : allValues) {
			if (value != null) {
				uniqueValues.add(Utility.getInstanceName(value.toString()));
			}
		}
		return uniqueValues;
	}

	/**
	 * This method cleans the concept uri
	 * @param uri concept uri
	 * @param engine
	 * @return clean concept
	 */
	public static String determineCleanConceptName(String uri, IEngine engine) {
		String conceptualURI = engine.getConceptualUriFromPhysicalUri(uri);
		return conceptualURI.substring(conceptualURI.lastIndexOf("/") + 1);
	}

	/**
	 * This method cleans the property uri
	 * @param uri concept uri
	 * @param engine
	 * @return clean property
	 */
	public static String determineCleanPropertyName(String uri, IEngine engine) {
		String conceptualURI = engine.getConceptualUriFromPhysicalUri(uri);
		String withoutConcept = conceptualURI.substring(0, conceptualURI.lastIndexOf("/"));
		return withoutConcept.substring(withoutConcept.lastIndexOf("/") + 1);
	}

	public static Vector<Object> retrieveCleanConceptValues(String uri, IEngine engine) {
		Vector<Object> conceptValues = engine.getEntityOfType(uri);
		Vector<Object> cleanConceptValues = new Vector<Object>();
		for(Object concept : conceptValues) {
			String[] splitConcept = ((String)concept).split("/");
			String cleanConcept = splitConcept[splitConcept.length-1];
			cleanConceptValues.add(cleanConcept);
		}
		
		return cleanConceptValues;
	} 
	// Test case
	public static void main(String[] args) {

		// Load the RDF map for testing purposes
		DIHelper.getInstance().loadCoreProp(System.getProperty("user.dir") + "/RDF_Map.prop");

		// Load the engines that will be tested and the local master database
		String[] engines = new String[] { Constants.LOCAL_MASTER_DB_NAME, "TAP_Core_Data", "Movie_DB" };

		// Load the engines for this test case
		String dbDirectory = "C:\\Users\\tbanach\\Workspace\\SemossDev\\db\\";
		for (String engineName : engines) {
			String smssPath = dbDirectory + engineName + ".smss";
			try (FileInputStream fis = new FileInputStream(new File(smssPath))) {
				Properties prop = new Properties();
				prop.load(fis);
				Utility.loadEngine(smssPath, prop);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// For debugging, use both an RDF and a RDBMS
		DomainValues dv = new DomainValues();

		// Test out the export
//		try {
//			dv.exportDomainValues();
//		} catch (IOException e) {
		// e.printStackTrace();
		// }

	}

	/**
	 * 
	 * @param engine
	 * @param outputFolder
	 */
	public void exportRelationInstanceValues(IEngine engine, String outputFolder) {
		String engineName = engine.getEngineName();
		QueryStruct engineQS = engine.getDatabaseQueryStruct();
		Map<String, Map<String, List>> relations = engineQS.getRelations();

		for (String fromConcept : relations.keySet()) {
			Map<String, List> joins = relations.get(fromConcept);
			for (String join : joins.keySet()) {
				Vector<String> concepts = (Vector) joins.get(join);
				for (String endConcept : concepts) {
					HashSet<String> uniqueConceptValues = new HashSet<String>();
					List<Object[]> allSourceInstances = null;
					Insight insightSource = InsightUtility.createInsight(engineName);
					StringBuilder pkqlCommand = new StringBuilder();
					pkqlCommand.append("data.frame('grid'); ");
					pkqlCommand.append("data.import ( api: " + engineName + " ");
					// pkqlCommand.append(". query ( [ c: "+ fromConcept + " ,
					// c:" + endConcept + "], " );
					pkqlCommand.append(". query ( [ c:" + endConcept + "], ");
					pkqlCommand.append("([ c: " + fromConcept + " , " + join + " , c:" + endConcept + " ])));");
					InsightUtility.runPkql(insightSource, pkqlCommand.toString());
					ITableDataFrame data = (ITableDataFrame) insightSource.getDataMaker();
					allSourceInstances = data.getData();
					for (int i = 0; i < allSourceInstances.size(); i++) {
						Object[] rowValues = allSourceInstances.get(i);
						String rowOutput = rowValues[0] + " ";
						uniqueConceptValues.add(rowOutput);
					}

					String conceptId = engineName + ENGINE_CONCEPT_PROPERTY_DELIMETER + fromConcept + "%%%"
							+ endConcept;

					writeToFile(outputFolder + "\\" + conceptId + ".txt", uniqueConceptValues);
				}
			}
		}
	}
}
