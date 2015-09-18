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
package prerna.poi.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.SQLQueryUtil;

/**
 * Creates a folder in user.dir/db that contains the files required for the engine The custom map, smss, and question sheet are all named based on the
 * name of the engine This class required user.dir/db/Default folder to contain a custom map, smss and question sheet
 */
public class PropFileWriter {

	private String engineDirectoryName;
	private String defaultDBPropName;
	private String defaultQuestionProp;
	private String defaultOntologyProp;
	private String baseDirectory;

	public String propFileName;
	public String engineName;
	public String questionFileName;
	public String ontologyFileName;
	public File engineDirectory;
	public String owlFile;
	public String defaultEngine = "prerna.engine.impl.rdf.BigDataEngine";
	public String defaultRDBMSEngine = "prerna.engine.impl.rdbms.RDBMSNativeEngine";//
	public boolean hasMap = false;

	private SQLQueryUtil.DB_TYPE dbDriverType = SQLQueryUtil.DB_TYPE.H2_DB;

	public PropFileWriter() {
		defaultDBPropName = "db/Default/Default.properties";
		defaultQuestionProp = "db/Default/Default_Questions.XML";
		defaultOntologyProp = "db/Default/Default_Custom_Map.prop";
	}

	public void setDefaultQuestionSheet(String defaultQuestionSheet) {
		this.defaultQuestionProp = defaultQuestionSheet;
	}

	public void setBaseDir(String baseDir) {
		baseDirectory = baseDir;
	}

	public void setRDBMSType(SQLQueryUtil.DB_TYPE dbDriverType){
		if(dbDriverType != null)
			this.dbDriverType = dbDriverType;
	}

	// TODO Change variable names, should we change default.properties to default.smss?
	/**
	 * Uses the name of a new database to create the custom map, smss, and question sheet files for the engine If user does not specify specific
	 * files, the default files in db/Default will be used This also creates the path to save the OWL file
	 * 
	 * @param dbName
	 *            String that contains the name of the engine
	 * @param ontologyName
	 *            String that contains the path to a user specified custom map file
	 * @param dbPropFile
	 *            String that contains the path to a user specified smss file
	 * @param questionFile
	 *            String that contains the path to a user specified question sheet
	 * @throws FileReaderException
	 * @throws EngineException
	 */
	public void runWriter(String dbName, String ontologyName, String dbPropFile, String questionFile, ImportDataProcessor.DB_TYPE dbType)
			throws FileReaderException, EngineException {
		if (dbName == null) {
			throw new EngineException("Database name is invalid.");
		}
		this.engineName = dbName;
		engineDirectoryName = "db" + System.getProperty("file.separator") + dbName;
		engineDirectory = new File(baseDirectory + System.getProperty("file.separator") + engineDirectoryName);
		try {
			// make the new folder to store everything in
			engineDirectory.mkdir();
			// define the owlFile location
			this.owlFile = "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + engineName + "_OWL.OWL";
			// if owlFile is null (which it is upon loading a new engine, create a new one
			File owlF = new File(baseDirectory + System.getProperty("file.separator") + owlFile);
			if(!owlF.exists()) { 
				PrintWriter writer = null;
				//input default parameters s.t. it is loaded and doesn't produce errors
				try {
					owlF.createNewFile();
					writer = new PrintWriter(baseDirectory + System.getProperty("file.separator") + owlFile);
					writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
					writer.println("<rdf:RDF");
					writer.println("\txmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"");
					writer.println("\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
					writer.println("</rdf:RDF>");
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if(writer != null) {
						writer.close();
					}
				}
			}
			// if question sheet was not specified, we need to make a copy of the default questions
			if ((questionFile == null || questionFile.equals(""))) {
				questionFileName = defaultQuestionProp.replaceAll("Default", dbName);
				// need to create XML file from scratch
				if (dbType == ImportDataProcessor.DB_TYPE.RDF)
					copyFile(baseDirectory + System.getProperty("file.separator") + questionFileName, baseDirectory
							+ System.getProperty("file.separator") + defaultQuestionProp);
				else if (dbType == ImportDataProcessor.DB_TYPE.RDBMS)
					questionFileName = questionFileName.replace("XML", "properties");
				// this needs to be completed
			}
			// if it was specified, get it in the format for the map file and move the file to the new directory
			else {
				if (dbType == ImportDataProcessor.DB_TYPE.RDF)
					FileUtils.copyFileToDirectory(new File(questionFile), engineDirectory, true);
				questionFileName = engineDirectoryName + questionFile.substring(questionFile.lastIndexOf("\\"));
				if (questionFileName.contains("\\"))
					questionFileName = questionFileName.replaceAll("\\\\", "/");
			}

			// if the map was not specified, copy default map. All augmentation of the map must be done after poi reader though
			if (ontologyName == null || ontologyName.equals("")) {
				ontologyFileName = defaultOntologyProp.replace("Default", dbName);
				copyFile(baseDirectory + System.getProperty("file.separator") + ontologyFileName, baseDirectory
						+ System.getProperty("file.separator") + defaultOntologyProp);
			}
			// if it was specified, don't copy default---will augment after running reader
			else {
				// if a default file was selected, just copy and replace default
				if (ontologyName.contains("Default")) {
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					String newOntologyFile = ontologyName.replace("Default", dbName);
					ontologyFileName = engineDirectoryName + newOntologyFile.substring(ontologyName.lastIndexOf("\\"));
					if (ontologyFileName.contains("\\")) {
						ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");
					}
					copyFile(ontologyFileName, ontologyName);
				}
				// if a truly custom map file was selected, just get in the format for rdf map
				else {
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					ontologyFileName = engineDirectoryName + ontologyName.substring(ontologyName.lastIndexOf("\\"));
					if (ontologyFileName.contains("\\")) {
						ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");
					}
				}
			}

			// Now we have all of the different file required for an engine taken care of, update the map file
			if (dbPropFile == null || dbPropFile.equals("")) {
				propFileName = baseDirectory + System.getProperty("file.separator") + defaultDBPropName;
				writeCustomDBProp(propFileName, dbName, dbType);
			} else {
				FileUtils.copyFileToDirectory(new File(dbPropFile), engineDirectory, true);
				propFileName = engineDirectoryName + dbPropFile.substring(dbPropFile.lastIndexOf("\\"));
				if (propFileName.contains("\\")) {
					propFileName = propFileName.replaceAll("\\\\", "/");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find default database files");
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Error copying default database files to new database");
		}

	}

	/**
	 * Copies all the contents of one file and sends it to another file
	 * 
	 * @param newFilePath
	 *            String containing the path to the new file
	 * @param oldFilePath
	 *            String containing the path to the old file which is going to be copied
	 * @throws FileReaderException
	 * @throws EngineException
	 */
	private void copyFile(String newFilePath, String oldFilePath) throws FileReaderException, EngineException {
		try {
			if (!oldFilePath.contains("_Questions.XML")) {
				Path newPath = Paths.get(newFilePath);
				Path oldPath = Paths.get(oldFilePath);
				Files.copy(oldPath, newPath);
			} else {
				createXMLQuestionFile(newFilePath, oldFilePath);
			}
		} catch (FileAlreadyExistsException ex) {
			ex.printStackTrace();
			throw new EngineException("Database name already exists. Please load using a different database name.");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new FileReaderException("Error copying default database files to new database");
		}
	}

	/**
	 * Creates the contents of the SMSS file in a temp file for the engine Adds file locations of the database, custom map, questions, and OWl files
	 * for the engine Adds the file locations to the contents of the default SMSS file which contains constant information about the database
	 * 
	 * @param defaultName
	 *            String containing the path to the Default SMSS file
	 * @param dbname
	 *            String containing the name of the new database
	 * @throws FileReaderException
	 */
	private void writeCustomDBProp(String defaultName, String dbname, ImportDataProcessor.DB_TYPE dbType) throws FileReaderException {
		String jnlName = engineDirectoryName + System.getProperty("file.separator") + dbname + ".jnl";
		// move it outside the default directory
		propFileName = defaultName.replace("Default/", "") + "1";
		// change the name of the file from default to engine name
		propFileName = propFileName.replace("Default", dbname);
		propFileName = propFileName.replace("properties1", "temp");

		// also write the base properties
		// ie ONTOLOGY, DREAMER, ENGINE, ENGINE CLASS
		FileWriter pw = null;
		BufferedReader read = null;
		Reader fileRead = null;
		try {
			File newFile = new File(propFileName);
			pw = new FileWriter(newFile);
			pw.write("Base Properties \n");
			pw.write(Constants.ONTOLOGY + "\t" + ontologyFileName + "\n");
			pw.write(Constants.OWL + "\t" + owlFile + "\n");
			// pw.write(name+"_PROP" + "\t"+ propFileName + "\n");
			pw.write(Constants.ENGINE + "\t" + dbname + "\n");
			if (dbType == ImportDataProcessor.DB_TYPE.RDF) {
				pw.write(Constants.ENGINE_TYPE + "\t" + this.defaultEngine + "\n");
				pw.write(Constants.INSIGHTS + "\t" + questionFileName + "\n\n\n");
			}
			if (dbType == ImportDataProcessor.DB_TYPE.RDBMS) {
				SQLQueryUtil queryUtil = SQLQueryUtil.initialize(dbDriverType);
				pw.write(Constants.ENGINE_TYPE + "\t" + this.defaultRDBMSEngine + "\n");
				pw.write(Constants.RDBMS_TYPE + "\t" + dbDriverType.toString() + "\n");
				pw.write(Constants.DREAMER + "\t" + questionFileName + "\n\n\n");
				pw.write(Constants.DRIVER + "\t" + queryUtil.getDatabaseDriverClassName() + "\n");
				pw.write(Constants.USERNAME + "\t" + queryUtil.getDefaultDBUserName() + "\n");
				pw.write(Constants.PASSWORD + "\t" + queryUtil.getDefaultDBPassword() + "\n");
				// pw.write("TEMP"+ "\t" + "true" + "\n");
				String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
				System.out.println("Base Folder...  " + baseFolder);
				pw.write(Constants.CONNECTION_URL + "\t" + queryUtil.getConnectionURL(baseFolder,dbname) + "\n");
				//pw.write(Constants.RDBMS_QUERY_CLASS + "\t" +  H2QueryUtil.class.getCanonicalName() + "\n");
				pw.write(Constants.USE_OUTER_JOINS + "\t" + queryUtil.getDefaultOuterJoins()+ "\n");
				//commenting out this item below by default
				pw.write("# " + Constants.USE_CONNECTION_POOLING + "\t" + queryUtil.getDefaultConnectionPooling());
//				pw.write(Constants.USE_CONNECTION_POOLING + "\t" + queryUtil.getDefaultConnectionPooling());

			}
			if (this.hasMap) {
				pw.write("MAP" + "\t" + "db/" + dbname + "/" + dbname + "_Mapping.ttl" + "\n");
			}
			if (dbType == ImportDataProcessor.DB_TYPE.RDF) {
				fileRead = new FileReader(defaultName);
				read = new BufferedReader(fileRead);
				String currentLine;
				while ((currentLine = read.readLine()) != null) {
					if (currentLine.contains("@FileName@")) {
						currentLine = currentLine.replace("@FileName@", jnlName);
					}
					pw.write(currentLine + "\n");
				}
			}
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			throw new FileReaderException("Could not find default database smss file");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new FileReaderException("Could not read default database smss file");
		}
		try {
			if (fileRead != null)
				fileRead.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if (read != null)
				read.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if (pw != null)
				pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the XML file containing all of the questions/insights. This takes the default XML file and replace the content with the engine name.
	 * 
	 * @param newFilePath
	 *            String containing the path to the new database
	 * @param oldFilePath
	 *            String containing the path to the Default folder
	 * @throws ParserConfigurationException 
	 * @throws SAXException 
	 */
	public void createXMLQuestionFile(String newFilePath, String oldFilePath) {
		BufferedReader reader = null;
		BufferedWriter writer = null;

		try {
			File oldFile = new File(oldFilePath);
			File newFile = new File(newFilePath);

			newFile.createNewFile();

			reader = new BufferedReader(new FileReader(oldFile));
			writer = new BufferedWriter(new FileWriter(newFile));

			StringBuffer xmlStringBugger = new StringBuffer();
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.contains("Default")) {
					line = line.replace("Default", StringEscapeUtils.escapeXml10(engineName));
				}
				xmlStringBugger.append(line);
				xmlStringBugger.append("\n");
			}

			// ensure encoding occurs properly for XML
			Document doc = stringToXML(xmlStringBugger.toString());
		    Source source = new DOMSource(doc);
		    Result result = new StreamResult(newFile);
		    Transformer xformer = TransformerFactory.newInstance().newTransformer();
		    xformer.transform(source, result);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public Document stringToXML(String xmlSource) throws SAXException, ParserConfigurationException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		return builder.parse(new InputSource(new StringReader(xmlSource)));
	}

}
