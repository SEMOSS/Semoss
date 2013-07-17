package prerna.poi.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import prerna.util.Constants;

public class OntologyFileWriter {

	public String fileName;
	File file;
	File tempFile;
	private String tempFileName;
	public String engineName;
	String questionFileName;
	public Hashtable newProp= new Hashtable();
	FileWriter pw;
	Logger logger = Logger.getLogger(getClass());
	
	//this will take an ontology file along with two hashtables and propURI and predicateURI
	//find where to add new uri values (objects and relations) and add those
	//get current base relations if there are any and add to those
	//see if map contains necessary properties like POP_URI and PREDICATE_URI--if no, add those
	public void runAugment(String ontologyFileName, Hashtable newURIvalues, String newBaseRelations, Hashtable newRelURIvalues, String newRelBaseRelations,
			String propURI, String predicateURI){
		fileName = ontologyFileName;
		if(ontologyFileName.contains("\\"))
			tempFileName = ontologyFileName.substring(0, ontologyFileName.lastIndexOf("\\")) + "\\TEMP_" + ontologyFileName.substring(ontologyFileName.lastIndexOf("\\")+1);
		else
			tempFileName = ontologyFileName.substring(0, ontologyFileName.lastIndexOf("/")) + "/TEMP_" + ontologyFileName.substring(ontologyFileName.lastIndexOf("/")+1);
	
		openFile();
		
		try{
			insertValues(newURIvalues, newBaseRelations, newRelURIvalues, newRelBaseRelations, propURI, predicateURI);
		
		}catch(Exception e){
			logger.error(e);
		}
		
		closeFile();
	}
	
	private void insertValues(Hashtable newURIvalues, String baseRelations, Hashtable relURIvalues, String relBaseRelations,
			String propURI, String predicateURI) throws IOException{
		BufferedReader read = new BufferedReader(new FileReader(fileName));
		String currentLine;
		boolean containsBaseObjects = false;
		boolean containsBasePredicates = false;
		boolean containsPropURI = false;
		boolean containsPredURI = false;
		boolean containsBaseData = false;
		boolean containsBaseMetaProperties = false;
		boolean containsBaseMetaRelationships = false;
		while((currentLine = read.readLine()) != null){
			boolean lineAlreadyHandled = false;
			boolean printLn = true;
			//add base objects
			if(currentLine.length()>=16){
				if(currentLine.substring(0, 16).equals("##Base Objects##")){
					System.out.println(currentLine);
					pw.write(currentLine + "\n");
					pw.write("\n");
					Iterator objectKeyIt = newURIvalues.keySet().iterator();
					while(objectKeyIt.hasNext()){
						String excelName = (String) objectKeyIt.next();
						String uri = (String) newURIvalues.get(excelName);
						pw.write(excelName + " " + uri + "\n");
						System.out.println(excelName + " " + uri);
					}
					logger.info("Done augmenting base objects");
					lineAlreadyHandled = true;
					printLn = false;
					containsBaseObjects = true;
				}
			}
			//add base predicates
			if (!lineAlreadyHandled && currentLine.length()>=19){
				if (currentLine.substring(0, 19).equals("##Base Predicates##")){
					System.out.println(currentLine);
					pw.write(currentLine + "\n");
					pw.write("\n");
					Iterator relationKeyIt = relURIvalues.keySet().iterator();
					while(relationKeyIt.hasNext()){
						String excelName = (String) relationKeyIt.next();
						String uri = (String) relURIvalues.get(excelName);
						pw.write(excelName + " " + uri + "\n");
						System.out.println(excelName + " " + uri);
					}
					logger.info("Done augmenting base predicates");
					lineAlreadyHandled = true;
					printLn = false;
					containsBasePredicates = true;
				}
				
			}
			//keep track of predicateURI.  if it never is reached, append at the end
			if (!lineAlreadyHandled && currentLine.length()>=Constants.PREDICATE_URI.length()){
				if (currentLine.substring(0, Constants.PREDICATE_URI.length()).equals(Constants.PREDICATE_URI)){
					containsPredURI = true;
					lineAlreadyHandled = true;
					logger.info("Map contains predicate_URI");
				}
				
			}
			//keep track of propURI.  if never reached, append at the end
			if (!lineAlreadyHandled && currentLine.length()>=Constants.PROP_URI.length()){
				if (currentLine.substring(0, Constants.PROP_URI.length()).equals(Constants.PROP_URI)){
					containsPropURI = true;
					lineAlreadyHandled = true;
					logger.info("Map contains prop_URI");
				}
				
			}
			//keep track of base data line
			if (!lineAlreadyHandled && currentLine.length()>=8){
				if (currentLine.substring(0, 8).equals("BaseData")){
					if(!currentLine.contains("=")) currentLine = currentLine + "=";
					if(!currentLine.contains("BaseMetaRelationships"))currentLine = currentLine + ";BaseMetaRelationships";
					if(!currentLine.contains("BaseMetaProperties"))currentLine = currentLine + ";BaseMetaProperties";
					containsBaseData = true;
					lineAlreadyHandled = true;
					logger.info("Map contains BaseData");
				}
				
			}
			//keep track of BaseMetaRelationships line
			if (!lineAlreadyHandled && currentLine.length()>=21){
				if (currentLine.substring(0, 21).equals("BaseMetaRelationships")){
					if(!currentLine.contains("=")) currentLine = currentLine + "=";
					//add any base meta relationships to what is already here
					currentLine = currentLine + ";"+ baseRelations;
					containsBaseMetaRelationships = true;
					lineAlreadyHandled = true;
					logger.info("Map contains BaseData");
				}
				
			}
			//keep track of BaseMetaProperties line
			if (!lineAlreadyHandled && currentLine.length()>=18){
				if (currentLine.substring(0, 18).equals("BaseMetaProperties")){
					if(!currentLine.contains("=")) currentLine = currentLine + "=";
					//add any base meta relationships to what is already here
					currentLine = currentLine + ";"+ relBaseRelations;
					containsBaseMetaProperties = true;
					lineAlreadyHandled = true;
					logger.info("Map contains BaseMetaProperties");
				}
				
			}
			if(printLn){
				System.out.println(currentLine);
				pw.write(currentLine + "\n");
			}
		}
		pw.write("\n\n");
		if(!containsBaseObjects){
			writeBaseObjects("##Base Objects##", newURIvalues);
		}
		if(!containsBasePredicates){
			writeBaseObjects("##Base Predicates##", relURIvalues);
		}
		if(!containsPropURI){
			pw.write(Constants.PROP_URI +" " + propURI +"\n");
		}
		if(!containsPredURI){
			pw.write(Constants.PREDICATE_URI +" " + predicateURI+"\n");
		}
		if(!containsBaseData){
			pw.write("BaseData=BaseMetaRelationships;BaseMetaProperties"+"\n");
		}
		if(!containsBaseMetaProperties){
			pw.write("BaseMetaProperties="+baseRelations+"\n");
		}
		if(!containsBaseMetaRelationships){
			pw.write("BaseMetaRelationships="+relBaseRelations+"\n");
		}
		read.close();
	}

	private void writeBaseObjects(String currentLine, Hashtable newURIvalues) throws IOException{
		System.out.println(currentLine);
		pw.write(currentLine + "\n");
		pw.write("\n");
		Iterator objectKeyIt = newURIvalues.keySet().iterator();
		while(objectKeyIt.hasNext()){
			String excelName = (String) objectKeyIt.next();
			String uri = (String) newURIvalues.get(excelName);
			pw.write(excelName + " " + uri + "\n");
			System.out.println(excelName + " " + uri);
		}
		pw.write("\n");
		logger.info("Done augmenting base objects");
	}

	private void writeBasePredicates(String currentLine, Hashtable relURIvalues) throws IOException{
		System.out.println(currentLine);
		pw.write(currentLine + "\n");
		pw.write("\n");
		Iterator relationKeyIt = relURIvalues.keySet().iterator();
		while(relationKeyIt.hasNext()){
			String excelName = (String) relationKeyIt.next();
			String uri = (String) relURIvalues.get(excelName);
			pw.write(excelName + " " + uri + "\n");
			System.out.println(excelName + " " + uri);
		}
		pw.write("\n");
		logger.info("Done augmenting base predicates");
	}
	
	private void openFile(){

		file = new File(fileName);
		tempFile = new File(tempFileName);
		try {
			pw = new FileWriter(tempFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void closeFile(){
		try {
			pw.close();
			file.delete();
			tempFile.renameTo(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
