package prerna.sablecc2.reactor.app.upload.rdf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.sail.SailException;

import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.RDFEngineCreationHelper;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RdfLoaderSheetUploadReactor extends AbstractUploadFileReactor {
	
	public RdfLoaderSheetUploadReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH,
				UploadInputUtility.ADD_TO_EXISTING, 
				UploadInputUtility.CUSTOM_BASE_URI
			};
	}

	public void generateNewApp(User user, String newAppId, String newAppName, String filePath) throws Exception {
		int stepCounter = 1;
		logger.info(stepCounter + ". Start validating app");
		UploadUtilities.validateApp(user, newAppName);
		logger.info(stepCounter + ". Done validating app");
		stepCounter++;

		logger.info(stepCounter + ". Start generating app folder");
		this.appFolder = UploadUtilities.generateAppFolder(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for app...");
		this.tempSmss = UploadUtilities.createTemporaryRdfSmss(newAppId, newAppName, owlFile);
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		this.engine = new BigDataEngine();
		this.engine.setEngineId(newAppId);
		this.engine.setEngineName(newAppName);
		this.engine.openDB(this.tempSmss.getAbsolutePath());
		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String typeOf = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		this.engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { sub, typeOf, obj, true });
		sub = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		obj = Constants.DEFAULT_PROPERTY_URI;
		this.engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { sub, typeOf, obj, true });
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Load Data
		 */
		logger.info(stepCounter + ". Parsing file metadata...");
		String baseUri = UploadInputUtility.getCustomBaseURI(this.store);
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), this.engine.getEngineType());
		owler.addCustomBaseURI(baseUri);
		importFile(this.engine, owler, filePath, baseUri);
		RdfUploadReactorUtility.loadMetadataIntoEngine(this.engine, owler);
		owler.commit();
		owler.export();
		// commit the created engine
		this.engine.setOWL(owler.getOwlPath());
		this.engine.commit();
		((BigDataEngine) this.engine).infer();
		logger.info(stepCounter + ". Complete...");
		RDFEngineCreationHelper.insertNewSelectConceptsAsInsights(this.engine, owler.getConceptualNodes());
		stepCounter++;

		/*
		 * Back to normal upload app stuff
		 */

		// and rename .temp to .smss
		this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(this.tempSmss, this.smssFile);
		this.tempSmss.delete();
		this.engine.setPropFile(this.smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, this.engine, this.smssFile);

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		this.engine.setInsightDatabase(insightDatabase);
		RDFEngineCreationHelper.insertSelectConceptsAsInsights(this.engine, owler.getConceptualNodes());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		UploadUtilities.updateMetadata(newAppId);
		logger.info(stepCounter + ". Complete");
	}

	public void addToExistingApp(String appId, String filePath) throws Exception {
		int stepCounter = 1;
		logger.info(stepCounter + ". Get existing app..");
		if (!(Utility.getEngine(appId) instanceof BigDataEngine)) {
			throw new IllegalArgumentException("Invalid engine type");
		}
		this.engine = (BigDataEngine) Utility.getEngine(appId);
		logger.info(stepCounter + ". Done..");
		stepCounter++;

		logger.setLevel(Level.ERROR);
		OWLER owler = new OWLER(this.engine, this.engine.getOWL());
		importFile(this.engine, owler, filePath, this.engine.getNodeBaseUri());
		RdfUploadReactorUtility.loadMetadataIntoEngine(this.engine, owler);
		owler.commit();
		owler.export();
		// commit the created engine
		this.engine.commit();
		((BigDataEngine) this.engine).infer();
		logger.info(stepCounter + ". Complete");
		RDFEngineCreationHelper.insertSelectConceptsAsInsights(this.engine, owler.getConceptualNodes());
		// commit the created engine
		this.engine.commit();
		((BigDataEngine) this.engine).infer();
	}

	@Override
	public void closeFileHelpers() {

	}
	
	/**
	 * Load the excel workbook, determine which sheets to load in workbook from the Loader tab
	 * @param fileName 					String containing the absolute path to the excel workbook to load
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void importFile(IEngine engine, OWLER owler, String fileName, String baseUri) throws FileNotFoundException, IOException {
		Workbook workbook = null;
		FileInputStream poiReader = null;
		try {
			poiReader = new FileInputStream(fileName);
			workbook = WorkbookFactory.create(poiReader);
			// load the Loader tab to determine which sheets to load
			Sheet lSheet = workbook.getSheet("Loader");
			if (lSheet == null) {
				throw new IOException("Could not find Loader Sheet in Excel file " + fileName);
			}
			// check if user is loading subclassing relationships
			Sheet subclassSheet = workbook.getSheet("Subclass");
			if (subclassSheet != null) {
				createSubClassing(engine, owler, subclassSheet);
			}
			// determine number of sheets to load
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
				Row row = lSheet.getRow(rIndex);
				// check to make sure cell is not null
				if (row != null) {
					Cell sheetNameCell = row.getCell(0);
					Cell sheetTypeCell = row.getCell(1);
					if (sheetNameCell != null && sheetTypeCell != null) {
						// get the name of the sheet
						String sheetToLoad = sheetNameCell.getStringCellValue();
						// determine the type of load
						String loadTypeName = sheetTypeCell.getStringCellValue();
						if (!sheetToLoad.isEmpty() && !loadTypeName.isEmpty()) {
							this.logger.debug("Cell Content is " + sheetToLoad);
							// this is a relationship
							if (loadTypeName.contains("Matrix")) {
								loadMatrixSheet(engine, owler, sheetToLoad, workbook, baseUri);
								engine.commit();
							} else {
								loadSheet(engine, owler, sheetToLoad, workbook, baseUri);
								engine.commit();
							}
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			if (poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new IOException("Could not close Excel file stream");
				}
			}
		}
	}
	
	/**
	 * Load subclassing information into the db and the owl file Requires the data to be in specific excel tab labeled "Subclass", with Parent nodes
	 * in the first column and child nodes in the second column
	 * 
	 * @param subclassSheet
	 *            Excel sheet with the subclassing information
	 * @throws IOException 
	 * @throws EngineException
	 * @throws SailException
	 */
	private void createSubClassing(IEngine engine, OWLER owler, Sheet subclassSheet) throws IOException {
		// URI for subclass
		String pred = Constants.SUBCLASS_URI;
		// check parent and child nodes in correct position
		Row row = subclassSheet.getRow(0);
		String parentNode = row.getCell(0).toString().trim().toLowerCase();
		String childNode = row.getCell(1).toString().trim().toLowerCase();
		// check to make sure parent column is in the correct column
		if (!parentNode.equalsIgnoreCase("parent")) {
			throw new IOException("Error with Subclass Sheet.\nError in parent node column.");
		}
		// check to make sure child column is in the correct column
		if (!childNode.equalsIgnoreCase("child")) {
			throw new IOException("Error with Subclass Sheet.\nError in child node column.");
		}
		// loop through and create all the triples for subclassing
		int lastRow = subclassSheet.getLastRowNum();
		for (int i = 1; i <= lastRow; i++) {
			row = subclassSheet.getRow(i);

			String parentURI = owler.addConcept(Utility.cleanString(row.getCell(0).toString(), true));
			String childURI = owler.addConcept(Utility.cleanString(row.getCell(1).toString(), true));
			// add triples to engine
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { childURI, pred, parentURI, true });
			// add triples to OWL
			owler.addSubclass(childNode, parentNode);
		}
		engine.commit();
		owler.commit();
	}

	/**
	 * Load specific sheet in workbook
	 * @param sheetToLoad			String containing the name of the sheet to load
	 * @param workbook				XSSFWorkbook containing the sheet to load
	 * @throws IOException
	 */
	public void loadSheet(IEngine engine, OWLER owler, String sheetToLoad, Workbook workbook, String baseUri) throws IOException {
		Sheet lSheet = workbook.getSheet(sheetToLoad);
		if (lSheet == null) {
			throw new IOException("Could not find sheet " + sheetToLoad + " in workbook.");
		}
		logger.info("Loading Sheet: " + sheetToLoad);
		System.out.println(">>>>>>>>>>>>>>>>> " + sheetToLoad);
		int lastRow = lSheet.getLastRowNum() + 1;

		// Get the first row to get column names
		Row row = lSheet.getRow(0);

		// initialize variables
		String objectNode = "";
		String relName = "";
		Vector<String> propNames = new Vector<String>();

		// determine if relationship or property sheet
		String sheetType = row.getCell(0).getStringCellValue();
		String subjectNode = row.getCell(1).getStringCellValue();
		int currentColumn = 0;
		if (sheetType.equalsIgnoreCase("Relation")) {
			objectNode = row.getCell(2).getStringCellValue();
			// if relationship, properties start at column 2
			currentColumn++;
		}

		// determine property names for the relationship or node
		// colIndex starts at currentColumn+1 since if relationship, the object
		// node name is in the second column
		int lastColumn = 0;
		for (int colIndex = currentColumn + 1; colIndex < row.getLastCellNum(); colIndex++) {
			// add property name to vector
			if (row.getCell(colIndex) != null) {
				propNames.addElement(row.getCell(colIndex).getStringCellValue());
				lastColumn = colIndex;
			}
		}
		logger.info("Number of Columns: " + (lastColumn + 1));

		// processing starts
		logger.info("Number of Rows: " + lastRow);
		for (int rowIndex = 1; rowIndex < lastRow; rowIndex++) {
			// first cell is the name of relationship
			Row nextRow = lSheet.getRow(rowIndex);

			if (nextRow == null) {
				continue;
			}

			// get the name of the relationship
			if (rowIndex == 1) {
				Cell relCell = nextRow.getCell(0);
				if (relCell != null && !relCell.getStringCellValue().isEmpty()) {
					relName = nextRow.getCell(0).getStringCellValue();
				} else {
					if (sheetType.equalsIgnoreCase("Relation")) {
						throw new IOException("Need to define the relationship on sheet " + sheetToLoad);
					}
					relName = "Ignore";
				}
			}

			// set the name of the subject instance node to be a string
			if (nextRow.getCell(1) != null && nextRow.getCell(1).getCellType() != Cell.CELL_TYPE_BLANK) {
				nextRow.getCell(1).setCellType(Cell.CELL_TYPE_STRING);
			}

			// to prevent errors when java thinks there is a row of data when
			// the row is empty
			Cell instanceSubjectNodeCell = nextRow.getCell(1);
			String instanceSubjectNode = "";
			if (instanceSubjectNodeCell != null && instanceSubjectNodeCell.getCellType() != Cell.CELL_TYPE_BLANK
					&& !instanceSubjectNodeCell.toString().isEmpty()) {
				instanceSubjectNode = nextRow.getCell(1).getStringCellValue();
			} else {
				continue;
			}

			// get the name of the object instance node if relationship
			String instanceObjectNode = "";
			int startCol = 1;
			int offset = 1;
			if (sheetType.equalsIgnoreCase("Relation")) {
				if (nextRow.getCell(2) != null) {
					// make it a string so i can easily parse it
					nextRow.getCell(2).setCellType(Cell.CELL_TYPE_STRING);
					Cell instanceObjectNodeCell = nextRow.getCell(2);
					// if empty, ignore
					if (ExcelParsing.isEmptyCell(instanceObjectNodeCell)) {
						continue;
					}
					instanceObjectNode = nextRow.getCell(2).getStringCellValue();
				}
				startCol++;
				offset++;
			}

			Hashtable<String, Object> propHash = new Hashtable<String, Object>();
			// process properties
			for (int colIndex = (startCol + 1); colIndex < nextRow.getLastCellNum(); colIndex++) {
				if (propNames.size() <= (colIndex - offset)) {
					continue;
				}
				String propName = propNames.elementAt(colIndex - offset).toString();
				// ignore bad data
				if (ExcelParsing.isEmptyCell(nextRow.getCell(colIndex))) {
					continue;
				}

				Object propValue = ExcelParsing.getCell(nextRow.getCell(colIndex));
				if (propValue instanceof SemossDate) {
					propValue = ((SemossDate) propValue).getDate();
				}
				propHash.put(propName, propValue);
			}

			if (sheetType.equalsIgnoreCase("Relation")) {
				// adjust indexing since first row in java starts at 0
				logger.info("Processing Relationship Sheet: " + sheetToLoad + ", Row: " + (rowIndex + 1));
				RdfUploadReactorUtility.createRelationship(engine, owler, baseUri, subjectNode, objectNode, instanceSubjectNode, instanceObjectNode, relName, propHash);
			} else {
				RdfUploadReactorUtility.addNodeProperties(engine, owler, baseUri, subjectNode, instanceSubjectNode, propHash);
			}
			if (rowIndex == (lastRow - 1)) {
				logger.info("Done processing: " + sheetToLoad);
			}
		}
	}
	
	/**
	 * Load excel sheet in matrix format
	 * @param sheetToLoad				String containing the name of the excel sheet to load
	 * @param workbook					XSSFWorkbook containing the name of the excel workbook
	 * @throws EngineException
	 */
	public void loadMatrixSheet(IEngine engine, OWLER owler, String sheetToLoad, Workbook workbook, String baseUri) {
		Sheet lSheet = workbook.getSheet(sheetToLoad);
		int lastRow = lSheet.getLastRowNum();
		logger.info("Number of Rows: " + lastRow);

		// Get the first row to get column names
		Row row = lSheet.getRow(0);
		// initialize variables
		String objectNodeType = "";
		String relName = "";
		boolean propExists = false;

		String sheetType = row.getCell(0).getStringCellValue();
		// Get the string that contains the subject node type, object node type, and properties
		String nodeMap = row.getCell(1).getStringCellValue();

		// check to see if properties exist
		String propertyName = "";
		StringTokenizer tokenProperties = new StringTokenizer(nodeMap, "@");
		String triple = tokenProperties.nextToken();
		if (tokenProperties.hasMoreTokens()) {
			propertyName = tokenProperties.nextToken();
			propExists = true;
		}

		StringTokenizer tokenTriple = new StringTokenizer(triple, "_");
		String subjectNodeType = tokenTriple.nextToken();
		if (sheetType.equalsIgnoreCase("Relation")) {
			relName = tokenTriple.nextToken();
			objectNodeType = tokenTriple.nextToken();
		}

		// determine object instance names for the relationship
		ArrayList<String> objectInstanceArray = new ArrayList<String>();
		int lastColumn = 0;
		for (int colIndex = 2; colIndex < row.getLastCellNum(); colIndex++) {
			objectInstanceArray.add(row.getCell(colIndex).getStringCellValue());
			lastColumn = colIndex;
		}
		// fix number of columns due to data shift in excel sheet
		lastColumn--;
		logger.info("Number of Columns: " + lastColumn);

		try {
			// process all rows (contains subject instances) in the matrix
			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				// boolean to determine if a mapping exists
				boolean mapExists = false;
				Row nextRow = lSheet.getRow(rowIndex);
				// get the name subject instance
				String instanceSubjectName = nextRow.getCell(1).getStringCellValue();
				// see what relationships are mapped between subject instances
				// and object instances
				for (int colIndex = 2; colIndex <= lastColumn; colIndex++) {
					String instanceObjectName = objectInstanceArray.get(colIndex - 2);
					Hashtable<String, Object> propHash = new Hashtable<String, Object>();
					// store value in cell between instance subject and object
					// in current iteration of loop
					Cell matrixContent = nextRow.getCell(colIndex);
					// if any value in cell, there should be a mapping
					if (matrixContent != null) {
						if (propExists) {
							if (matrixContent.getCellType() == Cell.CELL_TYPE_NUMERIC) {
								if (DateUtil.isCellDateFormatted(matrixContent)) {
									propHash.put(propertyName, (Date) matrixContent.getDateCellValue());
									mapExists = true;
								} else {
									propHash.put(propertyName, new Double(matrixContent.getNumericCellValue()));
									mapExists = true;
								}
							} else {
								// if not numeric, assume it is a string and
								// check to make sure it is not empty
								if (!matrixContent.getStringCellValue().isEmpty()) {
									propHash.put(propertyName, matrixContent.getStringCellValue());
									mapExists = true;
								}
							}
						} else {
							mapExists = true;
						}
					}

					if (sheetType.equalsIgnoreCase("Relation") && mapExists) {
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						RdfUploadReactorUtility.createRelationship(engine, owler, baseUri, subjectNodeType, objectNodeType, instanceSubjectName, instanceObjectName, relName, propHash);
					} else {
						logger.info("Processing" + sheetToLoad + " Row " + rowIndex + " Column " + colIndex);
						RdfUploadReactorUtility.addNodeProperties(engine, owler, baseUri, subjectNodeType, instanceSubjectName, propHash);
					}
				}
				logger.info(instanceSubjectName);
			}
		} finally {
			logger.info("Done processing: " + sheetToLoad);
		}
	}

}
