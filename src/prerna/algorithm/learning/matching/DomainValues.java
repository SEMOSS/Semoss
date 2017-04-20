package prerna.algorithm.learning.matching;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.openrdf.model.vocabulary.RDF;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to print out the unique instance values of every
 * concept/property of an engine to a .txt file
 */
public class DomainValues {

	private String baseFolder;
	private String outputFolder;
	private String[] engineNames;
	private boolean compareProperties;

	// Used for separating engine from concept or property names
	public static final String ENGINE_CONCEPT_PROPERTY_DELIMETER = ";";

	/**
	 * @param engineNames
	 *            The engines to extract unique values from
	 * @param compareProperties
	 *            Whether to extract for properties as well as concepts
	 */
	public DomainValues(String[] engineNames, boolean compareProperties) {
		this.baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		this.outputFolder = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\" + Constants.R_MATCHING_FOLDER + "\\" + Constants.R_TEMP_FOLDER + "\\" + Constants.R_MATCHING_REPO_FOLDER;
		this.engineNames = engineNames;
		this.compareProperties = compareProperties;
	}

	/**
	 * Export domain values as text files.
	 * 
	 * @throws IOException
	 */
	public void exportDomainValues() throws IOException {

		// Wipe out the old files
		FileUtils.cleanDirectory(new File(outputFolder));

		// Refresh the corpus
		for (String engineName : engineNames) {
			IEngine engine = (IEngine) Utility.getEngine(engineName);
			process(engine);
		}
	}

	private void process(IEngine engineToAdd) {
		String engineName = engineToAdd.getEngineName();

		// Grab all the concepts that exist in the database
		// Process each concept
		List<String> concepts = engineToAdd.getConcepts(false);
		for (String concept : concepts) {

			// Ignore the default concept node...
			if (concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}

			// Grab the unique values for the concept
			HashSet<String> uniqueConceptValues = retrieveConceptUniqueValues(concept, engineToAdd);

			// Sometimes this list is empty when users create databases with
			// empty fields that are meant to filled in via forms
			if (uniqueConceptValues.isEmpty()) {
				continue;
			}

			// Write the unique instances to a file
			String cleanConcept = determineCleanConceptName(concept, engineToAdd);
			String conceptId = engineName + ENGINE_CONCEPT_PROPERTY_DELIMETER + cleanConcept
					+ ENGINE_CONCEPT_PROPERTY_DELIMETER;
			writeToFile(outputFolder + "\\" + conceptId + ".txt", uniqueConceptValues);

			// If the user wants to compare properties,
			// then proceed to the concept's properties
			if (compareProperties) {

				// Grab all the properties that exist for the concept
				List<String> properties = engineToAdd.getProperties4Concept(concept, false);

				// If there are no properties, go onto the next concept
				if (properties.isEmpty()) {
					continue;
				}

				// Process each property
				for (String property : properties) {
					HashSet<String> uniquePropertyValues = retrievePropertyUniqueValues(concept, property, engineToAdd);
					if (!uniquePropertyValues.isEmpty()) {
						String cleanProperty = determineCleanPropertyName(property, engineToAdd);
						String propertyId = conceptId + cleanProperty;
						writeToFile(outputFolder + "\\" + propertyId + ".txt", uniquePropertyValues);
					}
				}
			}
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

	public static String determineCleanConceptName(String uri, IEngine engine) {
		String conceptualURI = engine.getConceptualUriFromPhysicalUri(uri);
		return conceptualURI.substring(conceptualURI.lastIndexOf("/") + 1);
	}

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
		DomainValues dv = new DomainValues(new String[] { "TAP_Core_Data", "Movie_DB" }, true);

		// Test out the export
		try {
			dv.exportDomainValues();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
