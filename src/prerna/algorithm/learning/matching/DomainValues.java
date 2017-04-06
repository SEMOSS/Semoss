package prerna.algorithm.learning.matching;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;

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
		this.outputFolder = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\" + Constants.R_MATCHING_REPO_FOLDER;
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
			HashSet<String> uniqueConceptValues = retrieveUniqueValues(engineToAdd, concept);

			// Sometimes this list is empty when users create databases with
			// empty fields that are meant to filled in via forms
			if (uniqueConceptValues.isEmpty()) {
				continue;
			}

			// Write the unique instances to a file
			String cleanConcept = concept.substring(concept.lastIndexOf("/") + 1);
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
					HashSet<String> uniquePropertyValues = new HashSet<String>();

					// There is no way to get the list of properties for a
					// specific concept in RDF through the interface
					// Create a query using the concept and the property name
					if (engineToAdd instanceof BigDataEngine) {

						// TODO simplify query using RDF.TYPE
						String query = "SELECT DISTINCT ?property WHERE { {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"
								+ concept + "> } { ?x <" + property + "> ?property} }";
						ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engineToAdd, query);
						while (wrapper.hasNext()) {
							ISelectStatement selectStatement = wrapper.next();

							// TODO test this out for various data types in RDF
							uniquePropertyValues.add(selectStatement.getVar("property").toString());
						}
					} else {

						// Grab the unique values for the property
						uniquePropertyValues = retrieveUniqueValues(engineToAdd, property);
					}
					if (!uniquePropertyValues.isEmpty()) {
						String cleanProperty = property.substring(
								property.substring(0, property.lastIndexOf("/")).lastIndexOf("/") + 1,
								property.lastIndexOf("/"));
						String propertyId = conceptId + cleanProperty;
						writeToFile(outputFolder + "\\" + propertyId + ".txt", uniquePropertyValues);
					}
				}
			}
		}
	}

	private static HashSet<String> retrieveUniqueValues(IEngine engine, String type) {

		// Get all the instances for the concept
		List<Object> allValues = engine.getEntityOfType(type);

		// Push all the instances into a set,
		// because we are only interested in unique values
		HashSet<String> uniqueValues = new HashSet<String>();
		for (Object value : allValues) {
			uniqueValues.add(Utility.getInstanceName(value + ""));
		}
		return uniqueValues;
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
}
