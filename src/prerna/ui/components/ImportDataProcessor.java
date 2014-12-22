/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.error.HeaderClassException;
import prerna.error.InvalidUploadFormatException;
import prerna.error.NLPException;
import prerna.poi.main.CSVReader;
import prerna.poi.main.NLPReader;
import prerna.poi.main.OntologyFileWriter;
import prerna.poi.main.POIReader;
import prerna.poi.main.PropFileWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.main.ImportRDBMSProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImportDataProcessor {

	static final Logger logger = LogManager.getLogger(ImportDataProcessor.class.getName());

	public enum IMPORT_METHOD {CREATE_NEW, ADD_TO_EXISTING, OVERRIDE, RDBMS};
	public enum IMPORT_TYPE {CSV, NLP, EXCEL, OCR};

	private String baseDirectory;
	private Hashtable<String, String>[] propHashArr;

	public void setPropHashArr(Hashtable<String, String>[] propHashArr) {
		this.propHashArr = propHashArr;
	}
	
	public void setBaseDirectory(String baseDirectory){
		this.baseDirectory = baseDirectory;
	}

	//This method will take in all possible required information
	//After determining the desired import method and type, process with the subset of information that that processing requires.
	public void runProcessor(IMPORT_METHOD importMethod, IMPORT_TYPE importType, String fileNames, String customBaseURI, 
			String newDBname, String mapFile, String dbPropFile, String questionFile, String repoName) throws EngineException, FileReaderException, HeaderClassException, FileWriterException, NLPException {
		if(importMethod == null) {
			String errorMessage = "Import method is not supported";
			throw new FileReaderException(errorMessage);
		}
		
		if(importType == null) {
			String errorMessage = "Import type is not supported";
			throw new FileReaderException(errorMessage);
		}
		
		if(importMethod == IMPORT_METHOD.CREATE_NEW) {
			processCreateNew(importType, customBaseURI, fileNames, newDBname, mapFile, dbPropFile, questionFile);
		} else if(importMethod == IMPORT_METHOD.ADD_TO_EXISTING) {
			processAddToExisting(importType, customBaseURI, fileNames, repoName);
		}
		else if(importMethod == IMPORT_METHOD.OVERRIDE) {
			processOverride(importType, customBaseURI, fileNames, repoName);
		}
	}

	public void processAddToExisting(IMPORT_TYPE importType, String customBaseURI, String fileNames, String repoName) throws EngineException, FileReaderException, HeaderClassException, FileWriterException {
		//get the engine information
		String mapPath = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.ONTOLOGY);
		String owlPath = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.OWL);
		if(importType == IMPORT_TYPE.EXCEL)
		{
			POIReader reader = new POIReader();
			//run the reader
			try {
				reader.importFileWithConnection(repoName, fileNames, customBaseURI, mapPath, owlPath);
			} catch (InvalidUploadFormatException ex) {
				ex.printStackTrace();
				throw new FileReaderException(ex.getMessage());
			}
			//run the ontology augmentor
			OntologyFileWriter ontologyWriter = new OntologyFileWriter();
			ontologyWriter.runAugment(mapPath, reader.conceptURIHash, reader.baseConceptURIHash, 
					reader.relationURIHash,  reader.baseRelationURIHash, 
					reader.basePropURI);
		}
		else if (importType == IMPORT_TYPE.CSV)
		{
			CSVReader csvReader = new CSVReader();
			//If propHash has not been set, we are coming from semoss and need to read the propFile
			//If propHash has been set, we are coming from monolith and are pulling propHash from the metamodel builder
			if(propHashArr != null) {
				csvReader.setRdfMapArr(propHashArr);
			}
			//run the reader
			csvReader.importFileWithConnection(repoName, fileNames, customBaseURI, owlPath);
			//run the ontology augmentor
			OntologyFileWriter ontologyWriter = new OntologyFileWriter();
			ontologyWriter.runAugment(mapPath, csvReader.conceptURIHash, csvReader.baseConceptURIHash, 
					csvReader.relationURIHash,  csvReader.baseRelationURIHash, 
					csvReader.basePropURI);
		}
	}

	public void processCreateNew(IMPORT_TYPE importType, String customBaseURI, String fileNames, String dbName, String mapFile, String dbPropFile, String questionFile) throws EngineException, FileWriterException, FileReaderException, HeaderClassException, NLPException {
		//Replace spaces in db name with underscores
		dbName = dbName.replace(" ", "_");
		//first write the prop file for the new engine
		PropFileWriter propWriter = runPropWriter(dbName, mapFile, dbPropFile, questionFile, importType);

		String ontoPath = baseDirectory + "/" + propWriter.ontologyFileName;
		String owlPath = baseDirectory + "/" + propWriter.owlFile;

		//then process based on what type of file
		if(importType == IMPORT_TYPE.EXCEL)
		{
			POIReader reader = new POIReader();
			try {
				reader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, mapFile, owlPath);
			} catch (InvalidUploadFormatException ex) {
				ex.printStackTrace();
				try {
					File failedSMSSFile = new File(baseDirectory + "/db/" + dbName + ".temp");
					failedSMSSFile.delete();
					FileUtils.deleteDirectory(new File(baseDirectory + "/db/" + dbName));
				} catch (IOException e) {
					e.printStackTrace();
					throw new FileReaderException(ex.getMessage() + "\nCould not delete failed directory at: " + baseDirectory + "/db/" + dbName);
				}
				throw new FileReaderException(ex.getMessage());
			}

			OntologyFileWriter ontologyWriter = new OntologyFileWriter();
			ontologyWriter.runAugment(ontoPath, reader.conceptURIHash, reader.baseConceptURIHash, 
					reader.relationURIHash, reader.baseRelationURIHash,
					reader.basePropURI);

			File propFile = new File(propWriter.propFileName);
			File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
			try {
				FileUtils.copyFile(propFile, newProp);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileWriterException("Could not create .smss file for new database");
			}
			propFile.delete();
		}
		//				try {
		//					reader.closeDB();
		//					logger.warn("SC IS OPEN:" + reader.sc.isOpen());
		//					ex.printStackTrace();
		//				} catch(Exception exe){
		//					exe.printStackTrace();
		//				}
		//				File propFile = new File(propWriter.propFileName);
		//				deleteFile(propFile);
		//				File propFile2 = propWriter.engineDirectory;
		//				deleteFile(propFile2);//need to use this function because must clear directory before i can delete it
		//			}
		//		}
		else if (importType == IMPORT_TYPE.CSV)
		{
			CSVReader csvReader = new CSVReader();
			//If propHash has not been set, we are coming from semoss and need to read the propFile
			//If propHash has been set, we are coming from monolith and are pulling propHash from the metamodel builder
			if(propHashArr != null) {
				csvReader.setRdfMapArr(propHashArr);
			}
			csvReader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, owlPath);

			OntologyFileWriter ontologyWriter = new OntologyFileWriter();
			ontologyWriter.runAugment(ontoPath, csvReader.conceptURIHash, csvReader.baseConceptURIHash, 
					csvReader.relationURIHash,  csvReader.baseRelationURIHash, 
					csvReader.basePropURI);

			File propFile = new File(propWriter.propFileName);
			File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
			try {
				FileUtils.copyFile(propFile, newProp);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileWriterException("Could not create .smss file for new database");
			}
			propFile.delete();
		}
		//					csvReader.closeDB();
		//					logger.warn("SC IS OPEN:" + csvReader.sc.isOpen());
		//				} catch(Exception exe){
		//
		//				}
		//				File propFile = new File(propWriter.propFileName);
		//				deleteFile(propFile);
		//				File propFile2 = propWriter.engineDirectory;
		//				deleteFile(propFile2);//need to use this function because must clear directory before i can delete it
		//			}
		//		}
		else if(importType == IMPORT_TYPE.NLP){
			NLPReader nlpreader = new NLPReader();
			nlpreader.importFileWithOutConnection(propWriter.propFileName, fileNames, customBaseURI, mapFile, owlPath);

			OntologyFileWriter ontologyWriter = new OntologyFileWriter();
			ontologyWriter.runAugment(ontoPath, nlpreader.conceptURIHash, nlpreader.baseConceptURIHash, 
					nlpreader.relationURIHash, nlpreader.baseRelationURIHash,
					nlpreader.basePropURI);

			File propFile = new File(propWriter.propFileName);
			File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
			try {
				FileUtils.copyFile(propFile, newProp);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileWriterException("Could not create .smss file for new database");
			}
			propFile.delete();
		} 
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
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(repoName);
			String[] files = fileNames.split(";");
			for (String file : files) {
				FileInputStream fileIn = null;
				try {
					fileIn = new FileInputStream(file);
					XSSFWorkbook book = new XSSFWorkbook(fileIn);
					XSSFSheet lSheet = book.getSheet("Loader");
					int lastRow = lSheet.getLastRowNum();

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
								nodes.add(Utility.cleanString(row.getCell(1).getStringCellValue(),true));
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

								relationships.add(new String[] { Utility.cleanString(subject, true),
										Utility.cleanString(relationship, true), Utility.cleanString(object, true) });
							}
						}
					}
					String deleteQuery = "";
					UpdateProcessor proc = new UpdateProcessor();
					proc.setEngine(engine);

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
							proc.setEngine(engine);
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

					String mapName = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.ONTOLOGY);
					String owlFile = baseDirectory + "/" + DIHelper.getInstance().getProperty(repoName+"_"+Constants.OWL);
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
				}finally{
					try{
						if(fileIn!=null)
							fileIn.close();
					}catch(IOException e) {
						e.printStackTrace();
					}
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

	public boolean processNewRDBMS(String customBaseURI, String fileNames, String repoName, String type, String url, String username, char[] password) throws FileReaderException, EngineException {
		boolean success = false;

		ImportRDBMSProcessor proc = new ImportRDBMSProcessor(customBaseURI, fileNames, repoName, type, url, username, password);
		if(proc.checkConnection(type, url, username, password)) {
			success = proc.setUpRDBMS();
		} else {
			return false;
		}

		File propFile = new File(proc.propWriter.propFileName);
		File newProp = new File(proc.propWriter.propFileName.replace("temp", "smss"));
		try {
			FileUtils.copyFile(propFile, newProp);
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		propFile.delete();

		return success;
	}

	private PropFileWriter runPropWriter(String dbName, String mapFile, String dbPropFile, String questionFile, IMPORT_TYPE importType) throws FileReaderException, EngineException{
		PropFileWriter propWriter = new PropFileWriter();

		if(importType == IMPORT_TYPE.NLP)
			propWriter.setDefaultQuestionSheet("db/Default/Default_NLP_Questions.XML");

		propWriter.setBaseDir(baseDirectory);
		propWriter.runWriter(dbName, mapFile, dbPropFile, questionFile);
		return propWriter;
	}
}
