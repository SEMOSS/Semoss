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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.commons.io.FileUtils;

import prerna.poi.main.helper.ImportOptions;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.SQLQueryUtil;

/**
 * Creates a folder in user.dir/db that contains the files required for the engine The custom map, smss, and question sheet are all named based on the
 * name of the engine This class required user.dir/db/Default folder to contain a custom map, smss and question sheet
 */
public class PropFileWriter {

	private String defaultDBPropName = "db/Default/Default.properties";
	private String engineDirectoryName;
	private String baseDirectory;

	public String propFileName;
	public String owlFile;

	private String engineName;
	private File engineDirectory;
	private String defaultEngine = "prerna.engine.impl.rdf.BigDataEngine";
	private String defaultRDBMSEngine = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
	private String defaultTinkerEngine = "prerna.engine.impl.tinker.TinkerEngine";
	private boolean hasMap = false;

	private SQLQueryUtil.DB_TYPE dbDriverType = SQLQueryUtil.DB_TYPE.H2_DB;
	SQLQueryUtil queryUtil;
	
	private ImportOptions.TINKER_DRIVER tinkerDriverType = ImportOptions.TINKER_DRIVER.TG; //default 

	// TODO Change variable names, should we change default.properties to default.smss?
	public void runWriter(String dbName, String dbPropFile, String questionFile, ImportOptions.DB_TYPE dbType) throws IllegalArgumentException, FileNotFoundException, IOException 
	{
		runWriter(dbName, dbPropFile, questionFile, dbType, null); // overloaded method to satisfy the file csv
	}

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
	 * @throws FileNotFoundException
	 * @throws IOException 
	 */
	public void runWriter(String dbName, String dbPropFile, String questionFile, ImportOptions.DB_TYPE dbType, String fileName)
			throws IllegalArgumentException, FileNotFoundException, IOException {
		if (dbName == null) {
			throw new IllegalArgumentException("Database name is invalid.");
		}
		this.engineName = dbName;
		engineDirectoryName = "db" + System.getProperty("file.separator") + dbName;
		engineDirectory = new File(baseDirectory + System.getProperty("file.separator") + engineDirectoryName);
		try {
			// make the new folder to store everything in
			if(!engineDirectory.exists())
				engineDirectory.mkdir();
			// define the owlFile location
			this.owlFile = "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + engineName + "_OWL.OWL";
			if(owlFile.contains("\\")) {
				owlFile = owlFile.replaceAll("\\\\", "/");
			}
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

			// Now we have all of the different file required for an engine taken care of, update the map file
			if (dbPropFile == null || dbPropFile.equals("")) {
				propFileName = baseDirectory + System.getProperty("file.separator") + defaultDBPropName;
				writeCustomDBProp(propFileName, dbName, dbType, fileName);
			} else {
				if(((new File(dbPropFile)).getPath().contains(engineDirectory.getPath()))) {
					FileUtils.copyFileToDirectory(new File(dbPropFile), engineDirectory, true);
				}
				propFileName = engineDirectoryName + dbPropFile.substring(dbPropFile.lastIndexOf("\\"));
				if (propFileName.contains("\\")) {
					propFileName = propFileName.replaceAll("\\\\", "/");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileNotFoundException(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
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
	private void writeCustomDBProp(String defaultName, String dbname, ImportOptions.DB_TYPE dbType) throws IOException {
		writeCustomDBProp(defaultName,dbname, dbType, null);
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
	private void writeCustomDBProp(String defaultName, String dbname, ImportOptions.DB_TYPE dbType, String fileName) throws IOException {
		
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
			pw.write(Constants.ENGINE + "\t" + dbname + "\n");
			pw.write(Constants.OWL + "\t" + this.owlFile + "\n");
			if (dbType == ImportOptions.DB_TYPE.RDF) {
				pw.write(Constants.ENGINE_TYPE + "\t" + this.defaultEngine + "\n");
			}
			pw.write(Constants.RDBMS_INSIGHTS + "\tdb" + System.getProperty("file.separator") + dbname + System.getProperty("file.separator") + "insights_database" + "\n");
			pw.write(Constants.SOLR_RELOAD + "\tfalse\n");
			pw.write(Constants.HIDDEN_DATABASE + "\tfalse\n");
			if (dbType == ImportOptions.DB_TYPE.RDBMS) {
				if(this.queryUtil == null) {
					this.queryUtil = SQLQueryUtil.initialize(dbDriverType);
				}
				pw.write(Constants.ENGINE_TYPE + "\t" + this.defaultRDBMSEngine + "\n");
				pw.write(Constants.RDBMS_TYPE + "\t" + queryUtil.getDatabaseType().toString() + "\n");
				pw.write(Constants.DRIVER + "\t" + queryUtil.getDatabaseDriverClassName() + "\n");
				pw.write(Constants.USERNAME + "\t" + queryUtil.getDefaultDBUserName() + "\n");
				pw.write(Constants.PASSWORD + "\t" + queryUtil.getDefaultDBPassword() + "\n");
				String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
				if(queryUtil.getDatabaseType().equals(SQLQueryUtil.DB_TYPE.H2_DB)) {
					if(fileName == null)
						pw.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL() + "\n");
					if(fileName != null)
					{
						pw.write("USE_FILE" + "\ttrue\n");
						fileName = fileName.replace(baseFolder, "@BaseFolder@");
						fileName = fileName.replace(dbname, "@ENGINE@");
						// strip the stupid ;
						fileName = fileName.replace(";", "");
						pw.write("DATA_FILE" + "\t" + fileName+"\n");
						pw.write(Constants.CONNECTION_URL + "\t" + RDBMSUtility.getH2BaseConnectionURL2() + "\n");
					}
				} else {
					pw.write(Constants.CONNECTION_URL + "\t" + queryUtil.getConnectionURL(baseFolder,dbname) + "\n");
				}
				pw.write(Constants.USE_OUTER_JOINS + "\t" + queryUtil.getDefaultOuterJoins()+ "\n");
				//commenting out this item below by default
				pw.write("# " + Constants.USE_CONNECTION_POOLING + "\t" + queryUtil.getDefaultConnectionPooling());
			}
			if (this.hasMap) {
				pw.write("MAP" + "\t" + "db/" + dbname + "/" + dbname + "_Mapping.ttl" + "\n");
			}
			if (dbType == ImportOptions.DB_TYPE.RDF) {
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
			if(dbType == ImportOptions.DB_TYPE.TINKER) {
				//tinker-specific properties
				pw.write(Constants.TINKER_FILE + " @BaseFolder@/db/@ENGINE@/@ENGINE@.tg" + "\n");
				pw.write(Constants.ENGINE_TYPE + "\t" + this.defaultTinkerEngine + "\n");
				pw.write(Constants.TINKER_DRIVER + "\t" + this.tinkerDriverType + "\n");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not find default database smss file");
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
	
	public void setSQLQueryUtil(SQLQueryUtil queryUtil) {
		this.queryUtil = queryUtil;
	}
	
	public void setBaseDir(String baseDir) {
		this.baseDirectory = baseDir;
	}

	public void setRDBMSType(SQLQueryUtil.DB_TYPE dbDriverType){
		this.dbDriverType = dbDriverType;
	}
	
	public void setTinkerType(ImportOptions.TINKER_DRIVER tinkerDriverType) {
		this.tinkerDriverType = tinkerDriverType;
	}
}
