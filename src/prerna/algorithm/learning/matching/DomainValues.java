package prerna.algorithm.learning.matching;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
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

	// used for separating engine from concept or property names
	public static final String ENGINE_VALUE_DELIMETER = ";";

	public DomainValues(String[] engineNames, boolean compareProperties) {
		this.baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		this.outputFolder = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\" + Constants.R_MATCHING_REPO_FOLDER + "\\";
		this.engineNames = engineNames;
		this.compareProperties = compareProperties;
	}

	/**
	 * Export domain values as text files.
	 * @throws IOException 
	 */
	public void exportDomainValues() throws IOException {
		
		// Wipe out the old files
		FileUtils.cleanDirectory(new File(outputFolder));
		
		// Refresh the corpus
		for (String engineName : engineNames) {
			IEngine engine = (IEngine) Utility.getEngine(engineName);
			printInstanceValues(engine);
		}
	}

	private void printInstanceValues(IEngine engineToAdd) {
		String engineName = engineToAdd.getEngineName();
		Writer fileWriter;

		// grab all concepts that exist in the database
		List<String> conceptList = engineToAdd.getConcepts(false);
		for (String concept : conceptList) {
			String cleanConcept = concept.substring(concept.lastIndexOf("/") + 1);

			// ignore the default concept node...
			if (concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}

			// get all the instances for the concept

			List<Object> instances = engineToAdd.getEntityOfType(concept);
			if (instances.isEmpty()) {
				// sometimes this list is empty when users create databases
				// with empty fields that are
				// meant to filled in via forms
				continue;
			}

			String newId = engineName + ENGINE_VALUE_DELIMETER + cleanConcept;

			HashSet<String> instancesList = new HashSet<String>();
			for (Object instance : instances) {
				instancesList.add(Utility.getInstanceName(instance + ""));
			}

			try {
				// TODO this is what the output for concept files will be named
				fileWriter = new FileWriter(outputFolder + newId + ".txt");
				for (String s : instancesList) {
					
					// Replace everything that is not a letter, number, or space with an underscore
					// I am not sure which characters are used to delimit instances in the R package
					// But I do know that underscores are not used
					fileWriter.write(s.replaceAll("[^A-Za-z0-9 ]", "_") + " ");
				}
				fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// see if the concept has properties
			if (compareProperties) {
				List<String> propName = engineToAdd.getProperties4Concept(concept, false);
				if (propName.isEmpty()) {
					// if no properties, go onto the next concept
					continue;
				}

				for (String prop : propName) {
					// there is no way to get the list of properties for a
					// specific concept in RDF through the interface
					// create a query using the concept and the property
					// name
					String propQuery = "SELECT DISTINCT ?property WHERE { {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"
							+ concept + "> } { ?x <" + prop + "> ?property} }";
					ISelectWrapper propWrapper = WrapperManager.getInstance().getSWrapper(engineToAdd, propQuery);
					HashSet<Object> propertiesList = new HashSet<Object>();
					while (propWrapper.hasNext()) {
						ISelectStatement propSS = propWrapper.next();
						Object property = propSS.getVar("property");
						if (property instanceof String && !Utility.isStringDate((String) property)) {
							// replace property underscores with space
							property = property.toString();
							propertiesList.add(property);
						}
					}

					if (!propertiesList.isEmpty()) {
						String cleanProp = prop.substring(prop.lastIndexOf("/") + 1);
						String propId = newId + ENGINE_VALUE_DELIMETER + cleanProp;

						propertiesList.add(Utility.getInstanceName(prop));

						// write properties
						try {
							// TODO this is what the output for property files
							// will be named
							fileWriter = new FileWriter(outputFolder + "PROP" + propId + ".txt");
							for (Object o : propertiesList) {
								fileWriter.write(o.toString().replaceAll("[^A-Za-z0-9 ]", "_") + " ");
							}
							fileWriter.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
}
