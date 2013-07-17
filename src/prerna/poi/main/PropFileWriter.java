package prerna.poi.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.commons.io.FileUtils;

import prerna.util.Constants;

public class PropFileWriter {

	String inputName = "TestName";
	public String propFileName;
	public String engineName;
	String questionFileName;
	public String ontologyFileName;
	public File engineDirectory;
	String engineDirectoryName;
	public Hashtable newProp= new Hashtable();
	
	
	/*This function takes in the name of a new database engine to create, the name of the ontology map file to use (can be ""), 
	 * the name of the prop file that specifies engine properties (can be "") and the name of the question file to associate with
	 * the new engine (also can be "")
	 * 
	 * For any of those that are "", the default will be copied and used.  For those that are not "", the files will not be 
	 * modified but the names will be used for updating the RDF_Map.prop
	 * 
	 * In total, this function adds the new engine to the map, and creates any files that are needed for an engine but not 
	 * specified on the intake.
	 * 
	 * This function creates the ontology map from the default for the new engine to use, but does not populate it.
	 * 
	 */
	public void runWriter(String dbName, String ontologyName, String dbPropFile, String questionFile){
		String workingDir = System.getProperty("user.dir");
		String defaultDBPropName = "db/Default/Default.properties";
		String defaultQuestionProp = "db/Default/Default_Questions.properties";
		String defaultOntologyProp = "db/Default/Default_Custom_Map.prop";
		this.engineName = dbName;
		engineDirectoryName = "db/" +dbName;
		engineDirectory = new File(workingDir +"/"+ engineDirectoryName);
		try {
			//make the new folder to store everything in
			engineDirectory.mkdir();
			//if question sheet was not specified, we need to make a copy of the default questions
			if(questionFile.equals("")){
				questionFileName = defaultQuestionProp.replaceAll("Default", dbName);
				copyFileWithDefaultReplaced(questionFileName, defaultQuestionProp);
			}
			//if it was specified, get it in the format for the map file and move the file to the new directory
			else {
				FileUtils.copyFileToDirectory(new File(questionFile), engineDirectory, true);
				questionFileName = engineDirectoryName + questionFile.substring(questionFile.lastIndexOf("\\"));
				if(questionFileName.contains("\\")) questionFileName = questionFileName.replaceAll("\\\\", "/");
			}

			//if the map was not specified, copy default map.  All augmentation of the map must be done after poi reader though
			if(ontologyName.equals("")) {
				ontologyFileName = defaultOntologyProp.replace("Default", dbName);
				copyFileWithDefaultReplaced(ontologyFileName, defaultOntologyProp);
			}
			//If it was specified, don't copy default---will augment after running reader
			else {
				//if a default file was selected, just copy and replace default
				if(ontologyName.contains("Default")){
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					String newOntologyFile =ontologyName.replace("Default", dbName);
					ontologyFileName = engineDirectoryName + newOntologyFile.substring(ontologyName.lastIndexOf("\\"));
					if(ontologyFileName.contains("\\")) ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");	
					copyFileWithDefaultReplaced(ontologyFileName, ontologyName);
				}
				//if a truly custom map file was selected, just get in the format for rdf map
				else{
					FileUtils.copyFileToDirectory(new File(ontologyName), engineDirectory, true);
					ontologyFileName = engineDirectoryName + ontologyName.substring(ontologyName.lastIndexOf("\\"));
					if(ontologyFileName.contains("\\"))
						ontologyFileName = ontologyFileName.replaceAll("\\\\", "/");					
				}
			}
			
			//Now we have all of the different file required for an engine taken care of, update the map file
			if(dbPropFile.equals(""))
				writeCustomDBProp(defaultDBPropName, dbName);
			else {
				FileUtils.copyFileToDirectory(new File(dbPropFile), engineDirectory, true);
				propFileName = engineDirectoryName + dbPropFile.substring(dbPropFile.lastIndexOf("\\"));
				if(propFileName.contains("\\")) propFileName = propFileName.replaceAll("\\\\", "/");
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//this merely makes a copy of a file and changes the file name without changing anything in the file
	private File copyFileWithDefaultReplaced(String fileName, String defaultName) throws IOException{

		File newFile = new File(fileName);
		FileWriter pw = new FileWriter(newFile);
		
		BufferedReader read = new BufferedReader(new FileReader(defaultName));
		String currentLine;
		while((currentLine = read.readLine()) != null){
			pw.write(currentLine + "\n");
		}
		read.close();
		pw.close();
		return newFile;
	}
	
	//this makes a copy of a file with changing the name of the file
	//also replaces "FileName" with engine name so that a the prop file is pointing at a unique jnl file
	private File writeCustomDBProp(String defaultName, String name) throws IOException{
		
		String jnlName = engineDirectoryName +"/"+ name+".jnl";
		//move it outside the default directory
		propFileName = defaultName.replace("Default/", "") + "1";
		//change the name of the file from default to engine name
		propFileName = propFileName.replace("Default", name);
		propFileName = propFileName.replace("properties1", "temp");
		
		File newFile = new File(propFileName);
		FileWriter pw = new FileWriter(newFile);
		
		BufferedReader read = new BufferedReader(new FileReader(defaultName));
		String currentLine;
		
		// also write the base properties
		// ie ONTOLOGY, DREAMER, ENGINE, ENGINE CLASS
		
		pw.write("Base Properties \n");
		//pw.write(name+"_PROP" + "\t"+ propFileName + "\n");
		pw.write(Constants.ENGINE + "\t" + name + "\n");
		pw.write(Constants.ENGINE_TYPE + "\t" + "prerna.rdf.engine.impl.BigDataEngine" + "\n");
		pw.write(Constants.ONTOLOGY + "\t"+  ontologyFileName + "\n");
		pw.write(Constants.DREAMER + "\t" + questionFileName + "\n\n\n");

		
		while((currentLine = read.readLine()) != null){
			if(currentLine.contains("@FileName@")){
				currentLine = currentLine.replace("@FileName@", jnlName);
			}
			System.out.println(currentLine);
			pw.write(currentLine + "\n");
		}
		read.close();
		pw.close();
		
		return newFile;
	}
	

}
