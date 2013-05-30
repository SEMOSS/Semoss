package prerna.poi.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import prerna.util.Constants;

public class PropFileWriter {

	String inputName = "TestName";
	public String propFileName;
	public String engineName;
	String questionFileName;
	public String ontologyFileName;
	public Hashtable newProp= new Hashtable();;
	
	
	public void runWriter(String dbName, String ontologyName, String dbPropFile, String questionFile){
		String workingDir = System.getProperty("user.dir");
		String defaultDBPropName = "db/DefaultBigDataProp.properties";
		String defaultQuestionProp = "questions/Default_Questions.properties";
		String defaultOntologyProp = "load_maps/Default_Custom_Map.prop";
		try {
			if(dbPropFile.equals(""))
				writeCustomDBProp(defaultDBPropName, dbName);
			else {
				propFileName = dbPropFile.substring(workingDir.length()+1);
				if(propFileName.contains("\\"))
					propFileName = propFileName.replaceAll("\\\\", "/");
			}

			//if question sheet was not specified, we need to make a copy of the default questions
			if(questionFile.equals("")){
				questionFileName = defaultQuestionProp.replace("Default", dbName);
				copyFileWithDefaultReplaced(questionFileName, defaultQuestionProp);
			}
			else {
				questionFileName = questionFile.substring(workingDir.length()+1);
				if(questionFileName.contains("\\"))
						questionFileName = questionFileName.replaceAll("\\\\", "/");
			}

			//map may already be specified.  If it is specified, don't copy default---will augment after running reader
			if(ontologyName.equals("")) {
				ontologyFileName = defaultOntologyProp.replace("Default", dbName);
				copyFileWithDefaultReplaced(ontologyFileName, defaultOntologyProp);
			}
			else {
				if(ontologyName.contains("Default")){
					ontologyFileName = ontologyName.replace("Default", dbName).substring(workingDir.length()+1).replaceAll("\\\\", "/");
					copyFileWithDefaultReplaced(ontologyFileName, ontologyName);
				}
				else{
					ontologyFileName = ontologyName.substring(workingDir.length()+1);
					if(ontologyFileName.contains("\\"))
						ontologyFileName.replaceAll("\\\\", "/");					
				}
			}

			updateDBCMmap(dbName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void updateDBCMmap(String name) throws IOException{
		String DBCMmapString = "RDF_Map.prop";
		File mapFile = new File(DBCMmapString);
		engineName = name;
		String engineClass = "prerna.rdf.engine.impl.BigDataEngine";

		File newFile = new File("Temp_" + DBCMmapString);
		FileWriter pw = new FileWriter(newFile);
		
		BufferedReader read = new BufferedReader(new FileReader(mapFile));
		String currentLine;
		boolean insertEngine= false;
		while((currentLine = read.readLine()) != null){
			if(currentLine.length()>7){
				if(currentLine.substring(0, 7).equals("ENGINES")){
					if(currentLine.endsWith(";")) {
						currentLine = currentLine + engineName;
						String split[] = currentLine.split("	");
						newProp.put(split[0], split[split.length-1]);
					}
					else  {
						currentLine = currentLine + ";" + engineName;
						String split[] = currentLine.split("	");
						newProp.put(split[0], split[split.length-1]);
					}
					insertEngine = true;
				}
			}
			System.out.println(currentLine);
			pw.write(currentLine + "\n");
			
			if(insertEngine){
				pw.write("\n");
				pw.write(engineName+" " + engineClass +"\n");
				pw.write(engineName+"_PROP " + propFileName +"\n");
				pw.write(engineName+"_"+Constants.ONTOLOGY + " " + ontologyFileName +"\n");
				pw.write(engineName+"_"+Constants.DREAMER + " " + questionFileName+"\n");
				
				newProp.put(engineName, engineClass);
				newProp.put(engineName+"_PROP", propFileName);
				newProp.put(engineName+"_"+Constants.ONTOLOGY, ontologyFileName);
				newProp.put(engineName+"_"+Constants.DREAMER, questionFileName);
				insertEngine = false;
			}
		}
		read.close();
		pw.close();
		mapFile.delete();
		newFile.renameTo(mapFile);
		
	}
	
	private void copyFileWithDefaultReplaced(String fileName, String defaultName) throws IOException{

		File newFile = new File(fileName);
		FileWriter pw = new FileWriter(newFile);
		
		BufferedReader read = new BufferedReader(new FileReader(defaultName));
		String currentLine;
		while((currentLine = read.readLine()) != null){
			pw.write(currentLine + "\n");
		}
		read.close();
		pw.close();
	}
	
	private File writeCustomDBProp(String defaultName, String name) throws IOException{
		
		String jnlName = name+".jnl";
		propFileName = defaultName.replace("Default", name);
		
		File newFile = new File(propFileName);
		FileWriter pw = new FileWriter(newFile);
		
		BufferedReader read = new BufferedReader(new FileReader(defaultName));
		String currentLine;
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
