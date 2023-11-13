package prerna.reactor.database.upload.rdbms.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.helper.excel.ExcelParsing;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.database.upload.AbstractUploadFileReactor;
import prerna.reactor.database.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RdbmsLoaderSheetUploadReactor extends AbstractUploadFileReactor {
	
	private static final Logger classLogger = LogManager.getLogger(RdbmsLoaderSheetUploadReactor.class);

	private Map<String, String> sqlHash = new Hashtable<String, String>();
	private Hashtable <String, Hashtable <String, String>> concepts = new Hashtable <String, Hashtable <String, String>>();
	private Hashtable <String, Vector <String>> relations = new Hashtable <String, Vector<String>>();
	private Hashtable <String, String> sheets = new Hashtable <String, String> ();

	private int indexUniqueId = 1;
	private List<String> tempIndexAddedList = new Vector<String>();
	private List<String> tempIndexDropList = new Vector<String>();
	
	public RdbmsLoaderSheetUploadReactor() {
		this.keysToGet = new String[]{ UploadInputUtility.DATABASE, UploadInputUtility.FILE_PATH};
	}

	public void generateNewDatabase(User user, final String newDatabaseName, final String filePath) throws Exception{
		if(!ExcelParsing.isExcelFile(filePath)) {
			NounMetadata error = new NounMetadata("Invalid file. Must be .xlsx, .xlsm or .xls", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException e = new SemossPixelException(error);
			e.setContinueThreadOfExecution(false);
			throw e;
		}
		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, newDatabaseName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.createTemporaryRdbmsSmss(this.databaseId, newDatabaseName, owlFile, RdbmsTypeEnum.H2_DB, null);
		DIHelper.getInstance().setEngineProperty(this.databaseId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		this.database = new RDBMSNativeEngine();
		this.database.setEngineId(this.databaseId);
		this.database.setEngineName(newDatabaseName);
		Properties smssProps = Utility.loadProperties(this.tempSmss.getAbsolutePath());
		smssProps.put("TEMP", true);
		this.database.open(smssProps);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Load Data
		 */
		logger.info(stepCounter + ". Parsing file metadata...");
		WriteOWLEngine owlEngine = this.database.getOWLEngineFactory().getWriteOWL();
		importFileRDBMS((RDBMSNativeEngine) this.database, owlEngine, filePath);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Back to normal database flow
		 */
		logger.info(stepCounter + ". Commit database metadata...");
		owlEngine.commit();
		owlEngine.export();
		owlEngine.close();
		// if(scriptFile != null) {
		// scriptFile.println("-- ********* completed load process ********* ");
		// scriptFile.close();
		// }
		// and rename .temp to .smss
		logger.info(stepCounter + ". Complete...");
		stepCounter++;
	}

	public void addToExistingDatabase(final String filePath) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void closeFileHelpers() {
		// TODO Auto-generated method stub

	}

	//////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/**
	 * Load the excel workbook, determine which sheets to load in workbook from
	 * the Loader tab
	 * 
	 * @param fileName
	 *            String containing the absolute path to the excel workbook to
	 *            load
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void importFileRDBMS(RDBMSNativeEngine database, WriteOWLEngine owlEngine, String fileName) throws FileNotFoundException, IOException {
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
			int lastRow = lSheet.getLastRowNum();
			// first sheet name in second row
			// step one is to go through every sheet and determine what are the
			// concepts within these sheets
			// along with their properties
			// would be kind of cool to guess their type as well

			for (int sIndex = 1; sIndex <= lastRow; sIndex++) {
				Row row = lSheet.getRow(sIndex);
				// check to make sure cell is not null
				if (row != null) {
					int sheetCounter = 0;
					Cell sheetNameCell = row.getCell(0);
					// unlike rdf, we do not have multiple load types for sheets
					// XSSFCell sheetTypeCell = row.getCell(1);
					if (sheetNameCell != null) {
						sheetCounter++;
						// get the name of the sheet
						String sheetToLoad = sheetNameCell.getStringCellValue();
						// determine the type of load
						// String loadTypeName =
						// sheetTypeCell.getStringCellValue();

						assimilateSheet(sheetToLoad, workbook);

						// assimilate this sheet and workbook
						// I need to have 2 hashes
						// hash 1 tells me
						// Concept - and all the given properties of this
						// concept
						// hash 2 tells me
						// given this concept what are its relationships as a
						// vector
					}
					if (sheetCounter == 0) {
						throw new IOException("Loader sheet specified no sheets to upload.\n Please specify which sheets you want to laod.");
					}
				}
			}

			// the next thing is to look at relation ships and add the
			// appropriate column that I want to the respective concepts

			System.out.println("Lucky !!" + concepts + " <> " + relations);
			System.out.println("Ok.. now what ?");
			synchronizeRelations();

			// now I need to create the tables
			Enumeration<String> conceptKeys = concepts.keys();
			while (conceptKeys.hasMoreElements()) {
				String thisConcept = conceptKeys.nextElement();
				createTable(database, owlEngine, thisConcept);
				processTable(database, thisConcept, workbook);
			}
			// I need to first create all the concepts
			// then all the relationships
			Enumeration<String> relationConcepts = relations.keys();
			while (relationConcepts.hasMoreElements()) {
				String thisConcept = relationConcepts.nextElement();
				Vector<String> allRels = relations.get(thisConcept);
				if (!allRels.isEmpty()) {
					createRelations(database, owlEngine, thisConcept, allRels, workbook);
				}

				// for(int toIndex = 0;toIndex < allRels.size();toIndex++)
				// // now process each one of these things
				// createRelations(thisConcept, allRels.elementAt(toIndex), workbook);
			}
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new FileNotFoundException(e.getMessage());
			} else {
				throw new FileNotFoundException("Could not find Excel file located at " + fileName);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("Could not read Excel file located at " + fileName);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IOException(e.getMessage());
			} else {
				throw new IOException("File: " + fileName + " is not a valid Microsoft Excel (.xlsx, .xlsm) file");
			}
		} finally {
			owlEngine.close();
			
			if (poiReader != null) {
				try {
					poiReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IOException("Could not close Excel file stream");
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					//throw new IOException("Could not close Excel workbook");
				}
			}
		}
	}
	
	private void assimilateSheet(String sheetName, Workbook workbook) {
		// really simple job here
		// load the sheet
		// if you find it
		// get the first row
		// if the row is present
		Sheet lSheet = workbook.getSheet(sheetName);

		this.logger.info("Processing Sheet..  " + sheetName);

		// we need to convert from the generic data types from the FE to the sql
		// specific types
		if (sqlHash.isEmpty()) {
			RdbmsUploadReactorUtility.createSQLTypes(this.sqlHash);
		}

		if (lSheet != null) {
			Row header = lSheet.getRow(0);
			// I will come to you shortly
			Row colPredictor = lSheet.getRow(1);
			int colLength = header.getLastCellNum();
			int colcolLength = colPredictor.getLastCellNum();

			System.out.println(colLength + " <>" + colcolLength);

			if (header != null) {
				String type = header.getCell(0).getStringCellValue();

				// we need to perform the correct type check regardless if it is
				// a relationship or a node
				// we shouldn't be assuming a relationship can only exist for
				// something of type string
				// as a note, this is however the case in rdf since
				// relationships use URIs which are strings
				String[] initTypes = predictRowTypes(lSheet);

				// convert to sql types
				int numCols = initTypes.length;
				String[] sqlDataTypes = new String[numCols];
				for (int colIdx = 0; colIdx < numCols; colIdx++) {
					if (initTypes[colIdx] != null) {
						if (sqlHash.get(initTypes[colIdx]) == null)
							sqlDataTypes[colIdx] = initTypes[colIdx];
						else
							sqlDataTypes[colIdx] = sqlHash.get(initTypes[colIdx]);
					}
				}

				if (type.equalsIgnoreCase("Relation")) {
					// process it as relation
					String fromName = header.getCell(1).getStringCellValue();
					fromName = Utility.cleanString(fromName, true);
					String toName = header.getCell(2).getStringCellValue();
					toName = Utility.cleanString(toName, true);

					Vector<String> relatedTo = new Vector<String>();
					if (relations.containsKey(fromName))
						relatedTo = relations.get(fromName);

					relatedTo.addElement(toName);
					relations.put(fromName, relatedTo);

					// if the concepts dont have relation key
					if (!concepts.containsKey(fromName)) {
						Hashtable<String, String> props = new Hashtable<String, String>();
						// props.put(fromName, initTypes[1]);
						props.put(fromName, sqlDataTypes[1]);
						concepts.put(fromName, props);
					}

					// if the concepts dont have relation key
					if (!concepts.containsKey(toName)) {
						Hashtable<String, String> props = new Hashtable<String, String>();
						// props.put(toName, initTypes[2]);
						props.put(toName, sqlDataTypes[2]);
						concepts.put(toName, props);
					}

					sheets.put(fromName + "-" + toName, sheetName);
				} else {
					// now predict the columns
					// String [] firstRowCells = getCells(colPredictor);
					String[] headers = getCells(header);
					String[] types = new String[headers.length];

					// int delta = types.length - initTypes.length;
					// System.arraycopy(initTypes, 0, types, 0,
					// initTypes.length);
					System.arraycopy(sqlDataTypes, 0, types, 0, sqlDataTypes.length);

					// for(int deltaIndex = initTypes.length;deltaIndex <
					// types.length;deltaIndex++)
					for (int deltaIndex = sqlDataTypes.length; deltaIndex < types.length; deltaIndex++)
						types[deltaIndex] = "varchar(800)";

					String conceptName = headers[1];

					conceptName = Utility.cleanString(conceptName, true);

					sheets.put(conceptName, sheetName);
					Hashtable<String, String> nodeProps = new Hashtable<String, String>();

					if (concepts.containsKey(conceptName))
						nodeProps = concepts.get(conceptName);

					/*
					 * else { nodeProps = new Hashtable<String, String>();
					 * nodeProps.put(conceptName, types[1]); }
					 */
					// process it as a concept
					for (int colIndex = 0; colIndex < types.length; colIndex++) {
						String thisName = headers[colIndex];
						if (thisName != null) {
							thisName = Utility.cleanString(thisName, true);
							nodeProps.put(thisName, types[colIndex]);
						}
					}

					concepts.put(conceptName, nodeProps);
				}
			}
		}
	}

	private void synchronizeRelations() {
		Enumeration<String> relationKeys = relations.keys();

		while (relationKeys.hasMoreElements()) {
			String relKey = relationKeys.nextElement();
			Vector<String> theseRelations = relations.get(relKey);

			Hashtable <String, String> prop1 = concepts.get(relKey);
			//if(prop1 == null)
			//	prop1 = new Hashtable<String, String>();

			for(int relIndex = 0;relIndex < theseRelations.size();relIndex++)
			{
				String thisConcept = theseRelations.elementAt(relIndex);
				Hashtable<String, String> prop2 = concepts.get(thisConcept);
				// if(prop2 == null)
				// prop2 = new Hashtable<String, String>();

				// affinity is used to which table to get when I get to this
				// relation
				String affinity = relKey + "-" + thisConcept + "AFF"; // <-- right.. that is some random shit.. the kind of stuff that gets me in trouble
				// need to compare which one is bigger and if so add to the other one right now
				// I have no idea what is the logic here
				// I should be comparing the total number of records
				// oh well.. !!

				// I also need to record who has this piece
				// so when we get to the point of inserting I know what i am doing
				//				if(prop1.size() > prop2.size())
				//				{
				//					prop2.put(relKey + "_FK", prop1.get(relKey));
				//					concepts.put(thisConcept, prop2); // God am I ever sure of anything
				//					sheets.put(affinity, thisConcept);
				//				}
				//				else
				//				{

				// due to loops, can't use the previous logic to determine FK based on who has less props
				// TODO: need to enable using the * to determine the FK position like in CSV files
				prop1.put(thisConcept + "_FK", prop2.get(thisConcept));
				concepts.put(relKey, prop1); // God am I ever sure of anything
				sheets.put(affinity, relKey);
				// }
			}
		}
	}

	private void createTable(RDBMSNativeEngine database, WriteOWLEngine owlEngine, String thisConcept) {
		Hashtable<String, String> props = concepts.get(thisConcept);

		String conceptType = props.get(thisConcept);

		// add it to OWL
		owlEngine.addConcept(thisConcept, null, null);
		owlEngine.addProp(thisConcept, thisConcept, conceptType);

		String createString = "CREATE TABLE " + thisConcept + " (";
		createString = createString + " " + thisConcept + " " + conceptType;

		// owler.addProp(thisConcept, thisConcept, conceptType);

		props.remove(thisConcept);

		// while for create it is fine
		// I have to somehow figure out a way to get rid of most of the other stuff
		Enumeration<String> fields = props.keys();

		while (fields.hasMoreElements()) {
			String fieldName = fields.nextElement();
			String fieldType = props.get(fieldName);
			createString = createString + " , " + fieldName + " " + fieldType;

			// also add this to the OWLER
			// also add this to the OWLER
			//if (!fieldName.equalsIgnoreCase(thisConcept) && !fieldName.endsWith("_FK")) {
			owlEngine.addProp(thisConcept, fieldName, fieldType);
			//}
		}

		props.put(thisConcept, conceptType);

		createString = createString + ")";
		System.out.println("Creator....  " + createString);
		try {
			database.insertData(createString);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		// now I say process this table ?

	}

	private void processTable(RDBMSNativeEngine database, String conceptName, Workbook workbook) {
		// this is where the stuff kicks in
		String sheetName = sheets.get(conceptName);
		if (sheetName != null) {
			Sheet lSheet = workbook.getSheet(sheetName);
			Row thisRow = lSheet.getRow(0);

			String[] cells = getCells(thisRow);
			int numHeaders = cells.length;
			SemossDataType[] types = new SemossDataType[numHeaders];

			String inserter = "INSERT INTO " + conceptName + " ( ";
			for (int cellIndex = 1; cellIndex < numHeaders; cellIndex++) {
				String headerValue = (cells[cellIndex] + "").trim();
				if (cellIndex == 1) {
					inserter = inserter + headerValue;
				} else {
					inserter = inserter + " , " + headerValue;
				}
				types[cellIndex] = SemossDataType.convertStringToDataType( concepts.get(conceptName).get(headerValue) );
			}
			
			inserter = inserter + ") VALUES ";
			int lastRow = lSheet.getLastRowNum();
			String values = "";
			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				thisRow = lSheet.getRow(rowIndex);
				if(thisRow == null) {
					continue;
				}
				cells = getCells(thisRow);
				int thisRowLength = cells.length;
				// account for cells that do not exist (since the array returned might not be the same size as headers)
				if(thisRowLength < 2) {
					if (types[1] == SemossDataType.INT || types[1] == SemossDataType.DOUBLE) {
						values = "( null ";
					} else {
						values = "( '' ";
					}
				} else {
					// we have values
					if (types[1] == SemossDataType.INT) {
						values = "( " + Utility.getInteger(cells[1]);
					} else if(types[1] == SemossDataType.DOUBLE) {
						values = "( " + Utility.getDouble(cells[1]);
					} else {
						values = "( '" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[1]) + "'";
					}
				}
				for (int cellIndex = 2; cellIndex < numHeaders; cellIndex++) {
					// account for cells that do not exist (since the array returned might not be the same size as headers)
					if(cellIndex >= thisRowLength) {
						if (types[cellIndex] == SemossDataType.INT || types[cellIndex] == SemossDataType.DOUBLE) {
							values = values + ", null ";
						} else {
							values = values + ", '' ";
						}
					} else {
						// we have values
						if (types[cellIndex] == SemossDataType.INT) {
							values = values + " , " + Utility.getInteger(cells[cellIndex]);
						} else if(types[cellIndex] == SemossDataType.DOUBLE) {
							values = values + " , " + Utility.getDouble(cells[cellIndex]);
						} else {
							values = values + " , '" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[cellIndex]) + "'";
						}
					}
				}

				// close the values
				values = values + ")";
				try {
					database.insertData(inserter + values);
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	private void createRelations(RDBMSNativeEngine database, WriteOWLEngine owlEngine, String fromName, List<String> toNameList, Workbook workbook) throws SQLException {
		int size = toNameList.size();
		List<String> relsAdded = new ArrayList<String>();

		for (int i = 0; i < size; i++) {
			String toName = toNameList.get(i);

			String sheetName = sheets.get(fromName + "-" + toName);
			Sheet lSheet = workbook.getSheet(sheetName);

			int lastRow = lSheet.getLastRowNum();
			Row thisRow = lSheet.getRow(0);
			String[] headers = getCells(thisRow);
			// realistically it is only second and third
			headers[1] = Utility.cleanString(headers[1], true);
			headers[2] = Utility.cleanString(headers[2], true);

			String tableToSet = headers[1];
			String tableToInsert = headers[2];
			//			String tableToSet = sheets.get(fromName + "-" + toName + "AFF");
			//
			//			System.out.println("Affinity is " + tableToSet);
			//			String tableToInsert = toName;
			//
			//			// TODO: what is this if statement for?
			//			if(tableToSet.equalsIgnoreCase(tableToInsert)) {
			//				tableToInsert = fromName;
			//			}

			// we need to make sure we create the predicate appropriately
			String predicate = null;
			int setter, inserter;
			if (headers[1].equalsIgnoreCase(tableToSet)) {
				// this means the FK is on the tableToSet
				setter = 1;
				inserter = 2;
				predicate = tableToSet + "." + tableToInsert + "_FK." + tableToInsert + "." + tableToInsert;
			} else {
				// this means the FK is on the tableToInsert
				setter = 2;
				inserter = 1;
				predicate = tableToSet + "." + tableToSet + "." + tableToInsert + "." + tableToSet + "_FK";
			}
			owlEngine.addRelation(tableToSet, tableToInsert, predicate);
			// TODO: figure out where to find the data type for the join column to add it as a property!!!
			
			createIndices(database, tableToSet, tableToSet);

			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				thisRow = lSheet.getRow(rowIndex);
				if(thisRow == null) {
					continue;
				}
				String[] cells = getCells(thisRow);
				if(cells.length < 3) {
					// missing value
					continue;
				}
				
				if (cells[setter] == null || cells[setter].isEmpty() || cells[inserter] == null
						|| cells[inserter].isEmpty()) {
					continue; // why is there an empty in the excel sheet....
				}

				// need to determine if i am performing an update or an insert
				String getRowCountQuery = "SELECT COUNT(*) as ROW_COUNT FROM " + tableToSet + " WHERE " + tableToSet
						+ " = '" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[setter]) + "' AND " + tableToInsert + "_FK IS NULL";
				boolean isInsert = false;
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = WrapperManager.getInstance().getRawWrapper(database, getRowCountQuery);
					if (wrapper.hasNext()) {
						String rowcount = wrapper.next().getValues()[0].toString();
						if (rowcount.equals("0")) {
							isInsert = true;
						}
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(wrapper != null) {
						try {
							wrapper.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}

				if (isInsert) {
					// we want to pull all concept values from query
					String colsToSelect = "";
					List<String> cols = new ArrayList<String>();
					Hashtable<String, String> propsToSelect = concepts.get(tableToSet);
					for (String prop : propsToSelect.keySet()) {
						if (prop.equalsIgnoreCase(tableToSet) || prop.endsWith("_FK")) {
							continue;
						}

						cols.add(prop);
						if (colsToSelect.isEmpty()) {
							colsToSelect = prop;
						} else {
							colsToSelect = colsToSelect + ", " + prop;
						}
					}
					// only need to be concerned with the relations that have
					// been added
					for (String rel : relsAdded) {
						cols.add(rel);
						colsToSelect = colsToSelect + ", " + rel;
					}
					// will always have rel and col
					cols.add(tableToSet);
					cols.add(tableToInsert + "_FK");
					colsToSelect = colsToSelect + ", " + tableToSet + ", " + tableToInsert + "_FK";

					// is it a straight insert since there are only two columns
					if (cols.size() == 2) {
						String insert = "INSERT INTO " + tableToSet + "(" + tableToSet + " ," + tableToInsert + "_FK"
								+ ") VALUES ( '" + cells[setter] + "' , '" + cells[inserter] + "')";

						database.insertData(insert);

					} else {
						// need to generate query to pull all existing
						// information
						// then append the new relationship
						// and insert all those cells

						List<String> unknownColsList = new ArrayList<String>();
						String unknownCols = "";
						int numCols = cols.size();
						for (int colNum = 0; colNum < numCols; colNum++) {
							String col = cols.get(colNum);
							if (col.equalsIgnoreCase(tableToSet) || col.equalsIgnoreCase(tableToInsert + "_FK")) {
								continue;
							}

							// plus 3 since last two in col should go into the
							// if statement above
							if (colNum + 3 == numCols) {
								unknownCols += col + " ";
								unknownColsList.add(col);
							} else {
								unknownColsList.add(col);
								unknownCols += col + ", ";
							}
						}

						String existingValues = "(SELECT DISTINCT " + unknownCols + " FROM " + tableToSet + " WHERE "
								+ tableToSet + "='" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[setter]) + "' ) AS TEMP_FK";
						StringBuilder selectingValues = new StringBuilder();
						selectingValues.append("SELECT DISTINCT ");

						for (int colNum = 0; colNum < numCols; colNum++) {
							String col = cols.get(colNum);
							if (unknownColsList.contains(col)) {
								selectingValues.append("TEMP_FK.").append(col).append(" AS ").append(col).append(", ");
							}
						}
						selectingValues.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[setter]) + "'").append(" AS ").append(tableToSet).append(", ");
						selectingValues.append("'" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[inserter]) + "'").append(" AS ").append(tableToInsert + "_FK").append(" ");
						selectingValues.append(" FROM ").append(tableToSet).append(",");

						String insert = "INSERT INTO " + tableToSet + "(" + colsToSelect + " ) " + selectingValues.toString() + existingValues;
						database.insertData(insert);
					}
				} else {
					// this is a nice and simple insert
					String updateString = "Update " + tableToSet + "  SET ";
					String values = tableToInsert + "_FK" + " = '" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[inserter]) + "' WHERE " + tableToSet + " = '" + AbstractSqlQueryUtil.escapeForSQLStatement(cells[setter])  + "'";
					database.insertData(updateString + values);
				}
			}
			relsAdded.add(tableToInsert + "_FK");
		}
	}
	
	public String[] predictRowTypes(Sheet lSheet) {
		int startRow = 1;
		int startCol = 2;
		int numRows = lSheet.getLastRowNum();
		Row header = lSheet.getRow(0);
		int numCells = header.getLastCellNum();
		String[] types = new String[numCells];
		
		ExcelRange r = new ExcelRange(startCol, numCells, startRow, numRows);
		this.logger.info("Predicting datatypes for sheet = " + lSheet.getSheetName());
		Object[][] prediction = ExcelParsing.predictTypes(lSheet, r.getRangeSyntax());

		// we will keep types[i] to be null
		// TODO: in future should fix this but other places are using it this way
		for(int i = 0; i < (numCells - 1); i++) {
			types[i+1] = prediction[i][0].toString();
		}
		return types;
	}
	
	public String[] getCells(Row row) {
		int colLength = row.getLastCellNum();
		return getCells(row, colLength);
	}

	public String[] getCells(Row row, int totalCol) {
		int colLength = totalCol;
		String[] cols = new String[colLength];
		for (int colIndex = 1; colIndex < colLength; colIndex++) {
			Cell thisCell = row.getCell(colIndex);
			Object value = ExcelParsing.getCell(thisCell);
			if(value == null) {
				cols[colIndex] = null;
			} else if(value instanceof SemossDate) {
				if( ((SemossDate) value).hasTime()) {
					cols[colIndex] = ((SemossDate) value).getFormatted("yyyy-MM-dd HH:mm:ss");
				} else {
					cols[colIndex] = ((SemossDate) value).getFormatted("yyyy-MM-dd");
				}
			} else {
				cols[colIndex] = value + "";
			}
		}

		return cols;
	}

	private void createIndices(RDBMSNativeEngine database, String cleanTableKey, String indexStr) {
		String indexOnTable = cleanTableKey + " ( " + indexStr + " ) ";
		String indexName = "INDX_" + cleanTableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(database.getDbType());

		// "DROP INDEX " + indexName;
		String dropIndex = queryUtil.dropIndex(indexName, cleanTableKey);
		if (tempIndexAddedList.size() == 0) {
			try {
				database.insertData(createIndex);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			tempIndexAddedList.add(indexOnTable);
			tempIndexDropList.add(dropIndex);
			indexUniqueId++;
		} else {
			boolean indexAlreadyExists = false;
			for (String index : tempIndexAddedList) {
				// TODO check various order of keys since they are comma
				// separated
				if (index.equals(indexOnTable)) {
					indexAlreadyExists = true;
					break;
				}

			}
			if (!indexAlreadyExists) {
				try {
					database.insertData(createIndex);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				tempIndexDropList.add(dropIndex);
				tempIndexAddedList.add(indexOnTable);
				indexUniqueId++;
			}
		}
	}
	
}
