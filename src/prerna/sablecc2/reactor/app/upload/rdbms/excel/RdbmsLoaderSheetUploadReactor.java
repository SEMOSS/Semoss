package prerna.sablecc2.reactor.app.upload.rdbms.excel;

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

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.AbstractUploadFileReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class RdbmsLoaderSheetUploadReactor extends AbstractUploadFileReactor {
	
	protected Map<String, String> sqlHash = new Hashtable<String, String>();
	private Hashtable <String, Hashtable <String, String>> concepts = new Hashtable <String, Hashtable <String, String>>();
	private Hashtable <String, Vector <String>> relations = new Hashtable <String, Vector<String>>();
	private Hashtable <String, String> sheets = new Hashtable <String, String> ();

	private int indexUniqueId = 1;
	//	private List<String> recreateIndexList = new Vector<String>(); 
	private List<String> tempIndexAddedList = new Vector<String>();
	private List<String> tempIndexDropList = new Vector<String>();	
	public RdbmsLoaderSheetUploadReactor() {
		this.keysToGet = new String[] {
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH};
	}


	public void generateNewApp(User user, final String newAppId, final String newAppName, final String filePath) throws Exception{
		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(newAppId, newAppName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create properties file for database...");
		this.tempSmss = UploadUtilities.createTemporaryRdbmsSmss(newAppId, newAppName, owlFile, "H2_DB", null);
		DIHelper.getInstance().getCoreProp().setProperty(newAppId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		this.engine = new RDBMSNativeEngine();
		this.engine.setEngineId(newAppId);
		this.engine.setEngineName(newAppName);
		Properties props = Utility.loadProperties(this.tempSmss.getAbsolutePath());
		props.put("TEMP", true);
		this.engine.setProp(props);
		this.engine.openDB(null);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Load Data
		 */
		logger.info(stepCounter + ". Parsing file metadata...");
		OWLER owler = new OWLER(owlFile.getAbsolutePath(), this.engine.getEngineType());
		importFileRDBMS((RDBMSNativeEngine) this.engine, owler, filePath);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		/*
		 * Back to normal app flow
		 */
		logger.info(stepCounter + ". Commit app metadata...");
		owler.commit();
		owler.export();
		this.engine.setOWL(owler.getOwlPath());
		// if(scriptFile != null) {
		// scriptFile.println("-- ********* completed load process ********* ");
		// scriptFile.close();
		// }
		// and rename .temp to .smss
		logger.info(stepCounter + ". Complete...");
		stepCounter++;

		this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(tempSmss, this.smssFile);
		this.tempSmss.delete();
		this.engine.setPropFile(this.smssFile.getAbsolutePath());
		UploadUtilities.updateDIHelper(newAppId, newAppName, this.engine, this.smssFile);

		logger.info(stepCounter + ". Start generating default app insights");
		IEngine insightDatabase = UploadUtilities.generateInsightsDatabase(newAppId, newAppName);
		UploadUtilities.addExploreInstanceInsight(newAppId, insightDatabase);
		this.engine.setInsightDatabase(insightDatabase);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process app metadata to allow for traversing across apps	");
		UploadUtilities.updateMetadata(newAppId);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
	}

	public void addToExistingApp(final String appId, final String filePath) throws Exception {
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
	public void importFileRDBMS(RDBMSNativeEngine engine, OWLER owler, String fileName) throws FileNotFoundException, IOException {
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
				createTable(engine, owler, thisConcept);
				processTable(engine, thisConcept, workbook);
			}
			// I need to first create all the concepts
			// then all the relationships
			Enumeration<String> relationConcepts = relations.keys();
			while (relationConcepts.hasMoreElements()) {
				String thisConcept = relationConcepts.nextElement();
				Vector<String> allRels = relations.get(thisConcept);
				if (!allRels.isEmpty()) {
					createRelations(engine, owler, thisConcept, allRels, workbook);
				}

				// for(int toIndex = 0;toIndex < allRels.size();toIndex++)
				// // now process each one of these things
				// createRelations(thisConcept, allRels.elementAt(toIndex), workbook);
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
	
	private void assimilateSheet(String sheetName, Workbook workbook) {
		// really simple job here
		// load the sheet
		// if you find it
		// get the first row
		// if the row is present
		Sheet lSheet = workbook.getSheet(sheetName);

		System.err.println("Processing Sheet..  " + sheetName);

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

	private void createTable(RDBMSNativeEngine engine, OWLER owler, String thisConcept) {
		Hashtable<String, String> props = concepts.get(thisConcept);

		String conceptType = props.get(thisConcept);

		// add it to OWL
		owler.addConcept(thisConcept, conceptType);

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
			if (!fieldName.equalsIgnoreCase(thisConcept) && !fieldName.endsWith("_FK"))
				owler.addProp(thisConcept, fieldName, fieldType);
		}

		props.put(thisConcept, conceptType);

		createString = createString + ")";
		System.out.println("Creator....  " + createString);
		try {
			engine.insertData(createString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// now I say process this table ?

	}

	private void processTable(RDBMSNativeEngine engine, String conceptName, Workbook workbook) {
		// this is where the stuff kicks in
		String sheetName = sheets.get(conceptName);
		if (sheetName != null) {
			Sheet lSheet = workbook.getSheet(sheetName);
			Row thisRow = lSheet.getRow(0);

			String[] cells = getCells(thisRow);
			int totalCols = cells.length;
			String[] types = new String[cells.length];

			String inserter = "INSERT INTO " + conceptName + " ( ";

			for (int cellIndex = 1; cellIndex < cells.length; cellIndex++) {
				if (cellIndex == 1)
					inserter = inserter + cells[cellIndex];
				else
					inserter = inserter + " , " + cells[cellIndex];

				types[cellIndex] = concepts.get(conceptName).get(cells[cellIndex]);
			}
			inserter = inserter + ") VALUES ";
			int lastRow = lSheet.getLastRowNum();
			String values = "";
			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				thisRow = lSheet.getRow(rowIndex);
				String[] uCells = getCells(thisRow, totalCols);
				cells = Utility.castToTypes(uCells, types);
				if (types[1].equals("INT") || types[1].equals("DOUBLE")) {
					values = "( " + cells[1];
				} else {
					values = "( '" + cells[1] + "'";
				}
				for (int cellIndex = 2; cellIndex < cells.length; cellIndex++) {
					if (types[cellIndex].equals("INT") || types[cellIndex].equals("DOUBLE")) {
						values = values + " , " + cells[cellIndex];
					} else {
						values = values + " , '" + cells[cellIndex] + "'";
					}
				}

				values = values + ")";
				try {
					engine.insertData(inserter + values);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void createRelations(RDBMSNativeEngine engine, OWLER owler, String fromName, List<String> toNameList, Workbook workbook) throws SQLException {
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
			owler.addRelation(tableToSet, tableToInsert, predicate);

			createIndices(engine, tableToSet, tableToSet);

			for (int rowIndex = 1; rowIndex <= lastRow; rowIndex++) {
				thisRow = lSheet.getRow(rowIndex);
				String[] cells = getCells(thisRow);

				if (cells[setter] == null || cells[setter].isEmpty() || cells[inserter] == null
						|| cells[inserter].isEmpty()) {
					continue; // why is there an empty in the excel sheet....
				}

				// need to determine if i am performing an update or an insert
				String getRowCountQuery = "SELECT COUNT(*) as ROW_COUNT FROM " + tableToSet + " WHERE " + tableToSet
						+ " = '" + cells[setter] + "' AND " + tableToInsert + "_FK IS NULL";
				boolean isInsert = false;
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getRowCountQuery);
				if (wrapper.hasNext()) {
					String rowcount = wrapper.next().getValues()[0].toString();
					if (rowcount.equals("0")) {
						isInsert = true;
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

						engine.insertData(insert);

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
								+ tableToSet + "='" + cells[setter] + "' ) AS TEMP_FK";
						StringBuilder selectingValues = new StringBuilder();
						selectingValues.append("SELECT DISTINCT ");

						for (int colNum = 0; colNum < numCols; colNum++) {
							String col = cols.get(colNum);
							if (unknownColsList.contains(col)) {
								selectingValues.append("TEMP_FK.").append(col).append(" AS ").append(col).append(", ");
							}
						}
						selectingValues.append("'" + cells[setter] + "'").append(" AS ").append(tableToSet).append(", ");
						selectingValues.append("'" + cells[inserter] + "'").append(" AS ").append(tableToInsert + "_FK").append(" ");
						selectingValues.append(" FROM ").append(tableToSet).append(",");

						String insert = "INSERT INTO " + tableToSet + "(" + colsToSelect + " ) " + selectingValues.toString() + existingValues;

						engine.insertData(insert);

					}
				} else {
					// this is a nice and simple insert
					String updateString = "Update " + tableToSet + "  SET ";
					String values = tableToInsert + "_FK" + " = '" + cells[inserter] + "' WHERE " + tableToSet + " = '" + cells[setter] + "'";
					engine.insertData(updateString + values);

				}
			}
			relsAdded.add(tableToInsert + "_FK");
		}
	}
	
	public String[] predictRowTypes(Sheet lSheet) {
		int numRows = lSheet.getLastRowNum();
		Row header = lSheet.getRow(0);
		int numCells = header.getLastCellNum();
		String[] types = new String[numCells];

		// need to loop through and make sure types are good
		// we know the first col is always null as it is not used
		for (int i = 1; i < numCells; i++) {
			String type = null;
			ROW_LOOP: for (int j = 1; j < numRows; j++) {
				Row row = lSheet.getRow(j);
				if (row != null) {
					Cell cell = row.getCell(i);
					if (cell != null) {
						String val = getCell(cell);
						if (val.isEmpty()) {
							continue ROW_LOOP;
						}
						String newTypePred = (Utility.findTypes(val)[0] + "").toUpperCase();
						if (newTypePred.contains("VARCHAR")) {
							type = newTypePred;
							break ROW_LOOP;
						}

						// need to also add the type null check for the first
						// row
						if (!newTypePred.equals(type) && type != null) {
							// this means there are multiple types in one column
							// assume it is a string
							if ((type.equals("BOOLEAN") || type.equals("INT") || type.equals("DOUBLE")) && 
									(newTypePred.equals("INT") || newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ){
								// for simplicity, make it a double and call it a day
								// TODO: see if we want to impl the logic to choose the greater of the newest
								// this would require more checks though
								type = "DOUBLE";
							} else {
								// should only enter here when there are numbers and dates
								// TODO: need to figure out what to handle this case
								// for now, making assumption to put it as a string
								type = "VARCHAR(800)";
								break ROW_LOOP;
							}
						} else {
							// type is the same as the new predicated type
							// or type is null on first iteration
							type = newTypePred;
						}
					}
				}
			}
			if (type == null) {
				// no data for column....
				types[i] = "varchar(255)";
			} else {
				types[i] = type;
			}
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
			// get all of this into a string
			if (thisCell != null && row.getCell(colIndex).getCellType() != Cell.CELL_TYPE_BLANK) {
				if (thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
					cols[colIndex] = thisCell.getStringCellValue();
					cols[colIndex] = Utility.cleanString(cols[colIndex], true);
				}
				if (thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC)
					cols[colIndex] = "" + thisCell.getNumericCellValue();
			} else {
				cols[colIndex] = "";
			}
		}

		return cols;
	}

	public String getCell(Cell thisCell) {
		if (thisCell != null && thisCell.getCellType() != Cell.CELL_TYPE_BLANK) {
			if (thisCell.getCellType() == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			} else if (thisCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return thisCell.getNumericCellValue() + "";
			}
		}
		return "";
	}

	private void createIndices(RDBMSNativeEngine engine, String cleanTableKey, String indexStr) {
		String indexOnTable = cleanTableKey + " ( " + indexStr + " ) ";
		String indexName = "INDX_" + cleanTableKey + indexUniqueId;
		String createIndex = "CREATE INDEX " + indexName + " ON " + indexOnTable;
		SQLQueryUtil queryUtil = SQLQueryUtil.initialize(engine.getDbType());

		// "DROP INDEX " + indexName;
		String dropIndex = queryUtil.getDialectDropIndex(indexName, cleanTableKey);
		if (tempIndexAddedList.size() == 0) {
			try {
				engine.insertData(createIndex);
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
					engine.insertData(createIndex);
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
