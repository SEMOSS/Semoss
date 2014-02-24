package prerna.ui.components;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.poi.main.AbstractFileReader;
import prerna.poi.main.CSVReader;
import prerna.poi.main.NLPReader;
import prerna.poi.main.OntologyFileWriter;
import prerna.poi.main.POIReader;
import prerna.poi.main.PropFileWriter;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ImportDataProcessor {

	Logger logger = Logger.getLogger(getClass());

	public enum IMPORT_METHOD {CREATE_NEW, ADD_TO_EXISTING, OVERRIDE};
	public enum IMPORT_TYPE {CSV, NLP, EXCEL};

	String baseDirectory;
	Hashtable<String, String> propHash;

	public void setBaseDirectory(String baseDirectory){
		this.baseDirectory = baseDirectory;
	}

	//This method will take in all possible required information
	//After determining the desired import method and type, process with the subset of information that that processing requires.
	public boolean runProcessor(IMPORT_METHOD importMethod, IMPORT_TYPE importType, String fileNames, String customBaseURI, 
			String newDBname, String mapFile, String dbPropFile, String questionFile, String repoName){
		boolean success = false;

		if(importMethod == IMPORT_METHOD.CREATE_NEW)
			success = processCreateNew(importType, customBaseURI, fileNames, newDBname, mapFile, dbPropFile, questionFile);

		else if(importMethod == IMPORT_METHOD.ADD_TO_EXISTING)
			success = processAddToExisting(importType, customBaseURI, fileNames, repoName);

		else if(importMethod == IMPORT_METHOD.OVERRIDE)
			success = processOverride(importType, customBaseURI, fileNames, repoName);

		return success;
	}

	public boolean processAddToExisting(IMPORT_TYPE importType, String customBaseURI, String fileNames, String repoName){
		boolean success = true;
		//get the engine information
		String mapPath = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.ONTOLOGY);
		String owlPath = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.OWL);
		if(importType == IMPORT_TYPE.EXCEL)
		{
			POIReader reader = new POIReader();
			try {
				//run the reader
				reader.importFileWithConnection(repoName, fileNames, customBaseURI, mapPath, owlPath);

				//run the ontology augmentor

				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(mapPath, reader.conceptURIHash, reader.baseConceptURIHash, 
						reader.relationURIHash,  reader.baseRelationURIHash, 
						reader.basePropURI);
			}
			catch (Exception ex)
			{
				success = false;
				ex.printStackTrace();
			}

		}
		else if (importType == IMPORT_TYPE.CSV)
		{
			CSVReader csvReader = new CSVReader();
			//If propHash has not been set, we are coming from semoss and need to read the propFile
			//If propHash has been set, we are coming from monolith and are pulling propHash from the metamodel builder
			if(propHash != null)
				csvReader.setRdfMap(propHash);
			try {
				//run the reader
				csvReader.importFileWithConnection(repoName, fileNames, customBaseURI, owlPath);

				//run the ontology augmentor
				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(mapPath, csvReader.conceptURIHash, csvReader.baseConceptURIHash, 
						csvReader.relationURIHash,  csvReader.baseRelationURIHash, 
						csvReader.basePropURI);
			}
			catch (Exception ex)
			{
				success = false;
				ex.printStackTrace();
			}
		}
		return success;
	}

	public boolean processCreateNew(IMPORT_TYPE importType, String customBaseURI, String fileNames, String dbName, String mapFile, String dbPropFile, String questionFile){
		boolean success = true;
		POIReader reader = new POIReader();
		NLPReader nlpreader = new NLPReader();
		//first write the prop file for the new engine
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseDirectory);
		if(importType == IMPORT_TYPE.NLP)
			propWriter.setDefaultQuestionSheet("db/Default/Default_NLP_Questions.properties");
		propWriter.runWriter(dbName, mapFile, dbPropFile, questionFile);

		String ontoPath = baseDirectory + "/" + propWriter.ontologyFileName;
		String owlPath = baseDirectory + "/" + propWriter.owlFile;

		//then process based on what type of file
		if(importType == IMPORT_TYPE.EXCEL)
		{
			try{
				reader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, mapFile, owlPath);

				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(ontoPath, reader.conceptURIHash, reader.baseConceptURIHash, 
						reader.relationURIHash, reader.baseRelationURIHash,
						reader.basePropURI);

				File propFile = new File(propWriter.propFileName);
				File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
				FileUtils.copyFile(propFile, newProp);
				propFile.delete();
			}catch(Exception ex)
			{
				success = false;
				try {
					reader.closeDB();
					logger.warn("SC IS OPEN:" + reader.sc.isOpen());
					ex.printStackTrace();
				} catch(Exception exe){
					exe.printStackTrace();
				}
				File propFile = new File(propWriter.propFileName);
				deleteFile(propFile);
				File propFile2 = propWriter.engineDirectory;
				deleteFile(propFile2);//need to use this function because must clear directory before i can delete it
			}
		}
		else if (importType == IMPORT_TYPE.CSV)
		{
			CSVReader csvReader = new CSVReader();
			//If propHash has not been set, we are coming from semoss and need to read the propFile
			//If propHash has been set, we are coming from monolith and are pulling propHash from the metamodel builder
			if(propHash != null)
				csvReader.setRdfMap(propHash);
			try{
				csvReader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, owlPath);

				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(ontoPath, csvReader.conceptURIHash, csvReader.baseConceptURIHash, 
						csvReader.relationURIHash,  csvReader.baseRelationURIHash, 
						csvReader.basePropURI);

				File propFile = new File(propWriter.propFileName);
				File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
				FileUtils.copyFile(propFile, newProp);
				propFile.delete();
			}
			catch(Exception ex)
			{
				success = false;
				ex.printStackTrace();
				try {
					csvReader.closeDB();
					logger.warn("SC IS OPEN:" + csvReader.sc.isOpen());
				} catch(Exception exe){

				}
				File propFile = new File(propWriter.propFileName);
				deleteFile(propFile);
				File propFile2 = propWriter.engineDirectory;
				deleteFile(propFile2);//need to use this function because must clear directory before i can delete it
			}
		}
		else if(importType == IMPORT_TYPE.NLP){
			System.out.println("HERE3");
			try{
				nlpreader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, mapFile, owlPath);

				OntologyFileWriter ontologyWriter = new OntologyFileWriter();
				ontologyWriter.runAugment(ontoPath, nlpreader.conceptURIHash, nlpreader.baseConceptURIHash, 
						nlpreader.relationURIHash, nlpreader.baseRelationURIHash,
						nlpreader.basePropURI);

				File propFile = new File(propWriter.propFileName);
				File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
				FileUtils.copyFile(propFile, newProp);
				propFile.delete();
			}catch(Exception ex)
			{
				success = false;
				try {
					nlpreader.closeDB();
					logger.warn("SC IS OPEN2:" + nlpreader.sc.isOpen());
					ex.printStackTrace();
				} catch(Exception exe){
					exe.printStackTrace();
				}
				File propFile = new File(propWriter.propFileName);
				deleteFile(propFile);
				File propFile2 = propWriter.engineDirectory;
				deleteFile(propFile2);//need to use this function because must clear directory before i can delete it
			}
		}
		return success;
	}


	/**
	 * Method executeDeleteAndLoad.  Executes the deleting and loading of files.
	 * @param repo String
	 * @param fileNames String
	 * @param customBaseURI String
	 * @param mapName String
	 * @param owlFile String
	 */
	public boolean processOverride(IMPORT_TYPE importType, String customBaseURI, String fileNames, String repoName) {
		if(importType == IMPORT_TYPE.EXCEL){
			boolean success = true;
			POIReader reader = new POIReader();
			String[] files = fileNames.split(";");
			for (String file : files) {
				try {
					XSSFWorkbook book = new XSSFWorkbook(new FileInputStream(file));
					XSSFSheet lSheet = book.getSheet("Loader");
					int lastRow = lSheet.getLastRowNum();

					ArrayList<String> sheets = new ArrayList<String>();
					ArrayList<String> nodes = new ArrayList<String>();
					ArrayList<String[]> relationships = new ArrayList<String[]>();
					for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
						XSSFRow sheetNameRow = lSheet.getRow(rIndex);
						XSSFCell cell = sheetNameRow.getCell(0);
						XSSFSheet sheet = book.getSheet(cell.getStringCellValue());

						XSSFRow row = sheet.getRow(0);
						String sheetType = "";
						if (row.getCell(0) != null) {
							sheetType = row.getCell(0).getStringCellValue();
						}
						if ("Node".equalsIgnoreCase(sheetType)) {
							if (row.getCell(1) != null) {
								nodes.add(row.getCell(1).getStringCellValue());
							}
						}
						if ("Relation".equalsIgnoreCase(sheetType)) {
							String subject = "";
							String object = "";
							String relationship = "";
							if (row.getCell(1) != null && row.getCell(2) != null) {
								subject = row.getCell(1).getStringCellValue();
								object = row.getCell(2).getStringCellValue();

								row = sheet.getRow(1);
								if (row.getCell(0) != null) {
									relationship = row.getCell(0)
											.getStringCellValue();
								}

								relationships.add(new String[] { subject,
										relationship, object });
							}
						}
					}
					String deleteQuery = "";
					UpdateProcessor proc = new UpdateProcessor();

					int numberNodes = nodes.size();
					if (numberNodes > 0) {
						for (String node : nodes) {
							deleteQuery = "DELETE {?s ?p ?prop. ?s ?x ?y} WHERE { {";
							deleteQuery += "SELECT ?s ?p ?prop ?x ?y WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
							deleteQuery += node;
							deleteQuery += "> ;} {?s ?x ?y} MINUS {?x <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation> ;} ";
							deleteQuery += "OPTIONAL{ {?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?s ?p ?prop ;} } } } ";
							deleteQuery += "}";

							proc.setQuery(deleteQuery);
							logger.info(deleteQuery);
							proc.processQuery();
						}
					}

					int numberRelationships = relationships.size();
					if (numberRelationships > 0) {
						for (String[] rel : relationships) {
							deleteQuery = "DELETE {?in ?relationship ?out. ?relationship ?contains ?prop} WHERE { {";
							deleteQuery += "SELECT ?in ?relationship ?out ?contains ?prop WHERE { "
									+ "{?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
							deleteQuery += rel[0];
							deleteQuery += "> ;} ";

							deleteQuery += "{?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/";
							deleteQuery += rel[2];
							deleteQuery += "> ;} ";

							deleteQuery += "{?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/";
							deleteQuery += rel[1];
							deleteQuery += "> ;} {?in ?relationship ?out ;} ";
							deleteQuery += "OPTIONAL { {?relationship ?contains ?prop ;} } } } ";
							deleteQuery += "}";

							proc.setQuery(deleteQuery);
							logger.info(deleteQuery);
							proc.processQuery();
						}
					}

					String mapName = DIHelper.getInstance().getProperty(repoName+"_"+Constants.ONTOLOGY);
					String owlFile = DIHelper.getInstance().getProperty(repoName+"_"+Constants.OWL);
					// run the reader
					reader.importFileWithConnection(repoName, file, customBaseURI,
							mapName, owlFile);

					// run the ontology augmentor

					OntologyFileWriter ontologyWriter = new OntologyFileWriter();
					ontologyWriter.runAugment(mapName, reader.conceptURIHash,
							reader.baseConceptURIHash, reader.relationURIHash,
							reader.baseRelationURIHash, reader.basePropURI);
				} catch (Exception ex) {
					success = false;
					ex.printStackTrace();
				}
			}
			return success;
		}
		else{
			//currently only processing override for excel
			return false;
		}
	}

	/**
	 * Method deleteFile.  Deletes a file from the directory.
	 * @param file File
	 */
	public void deleteFile(File file) {
		if(file.isDirectory()) {
			//directory is empty, then delete it
			if(file.list().length==0) {
				file.delete();
				logger.info("Directory is deleted : " + file.getAbsolutePath());
			} else {
				//list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					//construct the file structure
					File fileDelete = new File(file, temp);

					//this delete is only for two levels.  At this point, must be file, so just delete it
					fileDelete.delete();
				}

				//check the directory again, if empty then delete it
				if(file.list().length==0) {
					file.delete();
					logger.info("Directory is deleted : " + file.getAbsolutePath());
				}
			}

		} else {
			//if file, then delete it
			file.delete();
			logger.info("File is deleted : " + file.getAbsolutePath());
		}
	}

	public void setPropHash(Hashtable<String, String> propHash) {
		this.propHash = propHash;
	}

}
