/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.poi.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Creates a folder in user.dir/db that contains the files required for the engine
 * The custom map, smss, and question sheet are all named based on the name of the engine
 * This class required user.dir/db/Default folder to contain a custom map, smss and question sheet
 */
public class PropFileWriter {

	Logger logger = Logger.getLogger(getClass());

	private String baseDirectory;
	public String propFileName;
	public String engineName;
	String questionFileName;
	public String ontologyFileName;
	public File engineDirectory;
	String engineDirectoryName;
	public String owlFile;

	public PropFileWriter (){
		
	}
	
	public void setBaseDir(String baseDir){
		baseDirectory = baseDir;
	}
	
	//TODO Change variable names, should we change default.properties to default.smss?	
	/**
	 * Uses the name of a new database to create the custom map, smss, and question sheet files for the engine
	 * If user does not specify specific files, the default files in db/Default will be used 
	 * This also creates the path to save the OWL file
	 * @param dbName 			String that contains the name of the engine
	 * @param ontologyName 		String that contains the path to a user specified custom map file
	 * @param dbPropFile 		String that contains the path to a user specified smss file
	 * @param questionFile 		String that contains the path to a user specified question sheet
	 */
	public void runWriter(String dbName, String ontologyName, String dbPropFile, String questionFile){
		String defaultDBPropName = "db/Default/Default.properties";
		String defaultQuestionProp = "db/Default/Default_Questions.properties";
		String defaultOntologyProp = "db/Default/Default_Custom_Map.prop";
		this.engineName = dbName;
		engineDirectoryName = "db/" +dbName;
		engineDirectory = new File(baseDirectory +"/"+ engineDirectoryName);
		try {
			// make the new folder to store everything in
			engineDirectory.mkdir();
			// define the owlFile location
			this.owlFile = "db/"+engineName+"/"+engineName+"_OWL.OWL";
			// if question sheet was not specified, we need to make a copy of the default questions
			if(questionFile.equals("")){
				questionFileName = defaultQuestionProp.replaceAll("Default", dbName);
				copyFile(baseDirectory +"/"+ questionFileName, baseDirectory +"/"+ defaultQuestionProp);
			}
			// if it was specified, get it in the format for the map file and move the file to the new directory
			else {
				FileUtils.copyFileToDirectory(new File(questionFile), engineDirectory, true);
				questionFileName = engineDirectoryName + questionFile.substring(questionFile.lastIndexOf("\\"));
				if(questionFileName.contains("\\"))
					questionFileName = questionFileName.replaceAll("\\\\", "/");
			}

			// if the map was not specified, copy default map.  All augmentation of the map must be done after poi reader though
			if(ontologyName.equals("")) {
				ontologyFileName = defaultOntologyProp.replace("Default", dbName);
				copyFile(baseDirectory +"/"+ ontologyFileName, baseDirectory +"/"+ defaultOntologyProp);
			}
			// if it was specified, don't copy default---will augment after running reader
			else {
				// if a default file was selected, just copy and replace default
				if(ontologyName.contains("Default")){
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					String newOntologyFile =ontologyName.replace("Default", dbName);
					ontologyFileName = engineDirectoryName + newOntologyFile.substring(ontologyName.lastIndexOf("\\"));
					if(ontologyFileName.contains("\\")) ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");	
					copyFile(ontologyFileName, ontologyName);
				}
				// if a truly custom map file was selected, just get in the format for rdf map
				else{
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					ontologyFileName = engineDirectoryName + ontologyName.substring(ontologyName.lastIndexOf("\\"));
					if(ontologyFileName.contains("\\"))
						ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");					
				}
			}

			// Now we have all of the different file required for an engine taken care of, update the map file
			if(dbPropFile.equals("")){
				propFileName = baseDirectory +"/"+ defaultDBPropName;
				writeCustomDBProp(propFileName, dbName);
			}
			else {
				FileUtils.copyFileToDirectory(new File(dbPropFile), engineDirectory, true);
				propFileName = engineDirectoryName + dbPropFile.substring(dbPropFile.lastIndexOf("\\"));
				if(propFileName.contains("\\")) propFileName = propFileName.replaceAll("\\\\", "/");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Copies all the contents of one file and sends it to another file 
	 * @param newFilePath 		String containing the path to the new file
	 * @param oldFilePath 		String containing the path to the old file which is going to be copied
	 */
	private void copyFile(String newFilePath, String oldFilePath){
		try{
			Path newPath = Paths.get(newFilePath);
			Path oldPath = Paths.get(oldFilePath);
			Files.copy(oldPath, newPath);
		}
		catch(IOException ex){
			ex.printStackTrace();
			Utility.showMessage("Warning! \n Check engine file paths");
		}
	}

	/**
	 * Creates the contents of the SMSS file in a temp file for the engine
	 * Adds file locations of the database, custom map, questions, and OWl files for the engine
	 * Adds the file locations to the contents of the default SMSS file which contains constant information about the database
	 * @param defaultName 		String containing the path to the Default SMSS file
	 * @param dbname 			String containing the name of the new database
	 */
	private void writeCustomDBProp(String defaultName, String dbname) throws IOException{
		String jnlName = engineDirectoryName +"/"+ dbname+".jnl";
		//move it outside the default directory
		propFileName = defaultName.replace("Default/", "") + "1";
		//change the name of the file from default to engine name
		propFileName = propFileName.replace("Default", dbname);
		propFileName = propFileName.replace("properties1", "temp");

		File newFile = new File(propFileName);
		FileWriter pw = new FileWriter(newFile);

		BufferedReader read = new BufferedReader(new FileReader(defaultName));
		String currentLine;

		// also write the base properties
		// ie ONTOLOGY, DREAMER, ENGINE, ENGINE CLASS

		pw.write("Base Properties \n");
		//pw.write(name+"_PROP" + "\t"+ propFileName + "\n");
		pw.write(Constants.ENGINE + "\t" + dbname + "\n");
		pw.write(Constants.ENGINE_TYPE + "\t" + "prerna.rdf.engine.impl.BigDataEngine" + "\n");
		pw.write(Constants.ONTOLOGY + "\t"+  ontologyFileName + "\n");
		pw.write(Constants.OWL + "\t" + owlFile + "\n");
		pw.write(Constants.DREAMER + "\t" + questionFileName + "\n\n\n");

		while((currentLine = read.readLine()) != null){
			if(currentLine.contains("@FileName@")){
				currentLine = currentLine.replace("@FileName@", jnlName);
			}
			pw.write(currentLine + "\n");
		}
		read.close();
		pw.close();
	}
}
