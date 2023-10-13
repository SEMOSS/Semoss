package prerna.reactor.database.upload.rdf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RdfConvertLiteralTypeReactor extends AbstractReactor {

	private static final String CLASS_NAME = RdfConvertLiteralTypeReactor.class.getName();
	
	public RdfConvertLiteralTypeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), 
				ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.DATA_TYPE.getKey()
		};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String concept = this.keyValue.get(this.keysToGet[1]);
		String property = this.keyValue.get(this.keysToGet[2]);
		String dataType = this.keyValue.get(this.keysToGet[3]);

		if(databaseId == null || databaseId.isEmpty()) {
			throw new NullPointerException("Must provide an database id");
		}
		if(!SecurityEngineUtils.userIsOwner(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user is not an owner to the database");
		}
		
		if(concept == null || concept.isEmpty()) {
			throw new NullPointerException("Must provide a value for concept");
		}
		if(property == null || property.isEmpty()) {
			throw new NullPointerException("Must provide a value for property");
		}
		if(dataType == null || dataType.isEmpty()) {
			throw new NullPointerException("Must provide a value for data type");
		}
		
		Logger logger = getLogger(CLASS_NAME);
		
		final SemossDataType newDataType = SemossDataType.convertStringToDataType(dataType);
		
		final String propertyUri = "http://semoss.org/ontologies/Relation/Contains/" + property;
		String query = "select ?concept ?property where { "
				+ "{?concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/" + concept + ">} "
				+ "{?concept <" + propertyUri + "> ?property} "
				+ "}";
		
		List<Object[]> collection = new Vector<>();
		
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, query);
			while(iterator.hasNext()) {
				IHeadersDataRow row = iterator.next();
				String rawUri = row.getRawValues()[0] + "";
				Object literal = row.getValues()[1];
				collection.add(new Object[] {rawUri, literal, null});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		int counter = 0;
		String warning = null;
		for(Object[] modification : collection) {
			String subject = modification[0] + "";
			Object object = modification[1];
			
			logger.info("Modifying object " + Utility.cleanLogString(Arrays.toString(modification)));
			// remove
			if(object instanceof SemossDate) {
				engine.doAction(ACTION_TYPE.REMOVE_STATEMENT, 
						new Object[] {subject, propertyUri, ((SemossDate) object).getDate(), false});
			} else {
				engine.doAction(ACTION_TYPE.REMOVE_STATEMENT, 
						new Object[] {subject, propertyUri, object, false});
			}
			// now we try to convert the object
			try {
				Object newObject = null;
				if(newDataType == SemossDataType.STRING
						|| newDataType == SemossDataType.FACTOR) {
					newObject = object + "";
				} else if(newDataType == SemossDataType.INT 
						|| newDataType == SemossDataType.DOUBLE) {
					newObject = Double.parseDouble(object + "");
				} else if(newDataType == SemossDataType.DATE
						|| newDataType == SemossDataType.TIMESTAMP) {
					
					if(object instanceof SemossDate) {
						newObject = ((SemossDate) object).getDate();
					} else if(object instanceof String){
						SemossDate dateObject = SemossDate.genDateObj(object + "");
						if(dateObject == null) {
							warning = "Some values did not properly parse";
							continue;
						}
						
						newObject = dateObject.getDate();
					} else {
						warning = "Some values did not properly parse";
						continue;
					}
				} else if(newDataType == SemossDataType.BOOLEAN) {
					newObject = Boolean.parseBoolean(object + "");
				}
				
				// add
				engine.doAction(ACTION_TYPE.ADD_STATEMENT, 
						new Object[] {subject, propertyUri, newObject, false});
				
				
				// update the collection array for the new object
				// so we can export to an excel file
				modification[2] = newObject;
				counter++;
			} catch(Exception e) {
				warning = "Some values did not properly parse";
			}
		}
		engine.commit();
		
		// write an excel file with the data
		writeExcel(concept, property, dataType, collection);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(warning == null) {
			noun.addAdditionalReturn(getSuccess("Successfully modified the data type of " + property + " to " + newDataType + ". Number of rows modifed = " + counter));
		} else {
			noun.addAdditionalReturn(getWarning(warning));
		}
		return noun;
	}
	
	/**
	 * Write the excel for auditing
	 * @param modifications
	 */
	private void writeExcel(String concept, String property, String datatype, List<Object[]> modifications) {
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS");
		formatter.setTimeZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		String modifiedDate = formatter.format(date);
		
		String baseFolder = this.insight.getInsightFolder();
		String filename = Utility.normalizePath( ("Convert_" + concept + "_" + property + "_totype_" + datatype + "_" + modifiedDate).
				replaceAll("[^a-zA-Z0-9\\.\\-]", "_")) + ".xlsx";
		String fileLocation = baseFolder + DIR_SEPARATOR + filename;
		
		SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
		CreationHelper createHelper = workbook.getCreationHelper();
		String sheetName = "Modifications";

		SXSSFSheet sheet = workbook.createSheet(sheetName);
		sheet.setRandomAccessWindowSize(100);
		// freeze the first row
		sheet.createFreezePane(0, 1);
		
		// style dates
		CellStyle dateCellStyle = workbook.createCellStyle();
        dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
		
		// create typesArr as an array for faster searching
		String[] headers = new String[] {"Instance URI", "Original Value", "Original Value Type", "Modified Value"};
		
		// the excel data row
		int excelRowCounter = 0;
		Row excelRow = null;
		// we need to iterate and write the headers during the first time
		{
			// create the header row
	        Row headerRow = sheet.createRow(excelRowCounter++);
			// create a Font for styling header cells
			Font headerFont = workbook.createFont();
			headerFont.setBold(true);
			// create a CellStyle with the font
			CellStyle headerCellStyle = workbook.createCellStyle();
			headerCellStyle.setFont(headerFont);
	        headerCellStyle.setAlignment(HorizontalAlignment.CENTER);
	        headerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

			// generate the header row
			// and define constants used throughout like size, and types
			for(int i = 0; i < headers.length; i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
			}
		}
		
		Iterator<Object[]> it = modifications.iterator();
		while(it.hasNext()) {
			excelRow = sheet.createRow(excelRowCounter++);
			
			Object[] row = it.next();
			String instanceUri = row[0] + "";
			Object origValue = row[1];
			Object modValue = row[2];
			if(modValue == null) {
				modValue = "Unable to parse original value";
			}
			
			// instance uri
			{
				Cell cell = excelRow.createCell(0);
				cell.setCellValue(instanceUri);
			}
			
			// original values
			{
				Cell origCell = excelRow.createCell(1);
				Cell origCellType = excelRow.createCell(2);

				if(origValue instanceof String) {
					origCell.setCellValue(origValue + "");
					origCellType.setCellValue("String");
				} else if(origValue instanceof Number) {
					origCell.setCellValue( ((Number) origValue).doubleValue());
					origCellType.setCellValue("Number");
				} else if(origValue instanceof SemossDate) {
					origCell.setCellValue( ((SemossDate) origValue).getDate());
					origCell.setCellStyle(dateCellStyle);
					origCellType.setCellValue("Date");
				} else if(origValue instanceof Date) {
					origCell.setCellValue( ((Date) origValue));
					origCell.setCellStyle(dateCellStyle);
					origCellType.setCellValue("Date");
				} else if(origValue instanceof Boolean) {
					origCell.setCellValue( ((Boolean) origValue));
					origCellType.setCellValue("Boolean");
				}
			}
			
			// modified values
			{
				Cell modCell = excelRow.createCell(3);

				if(origValue instanceof String) {
					modCell.setCellValue(origValue + "");
				} else if(origValue instanceof Number) {
					modCell.setCellValue( ((Number) origValue).doubleValue());
				} else if(origValue instanceof SemossDate) {
					modCell.setCellValue( ((SemossDate) origValue).getDate());
					modCell.setCellStyle(dateCellStyle);
				} else if(origValue instanceof Date) {
					modCell.setCellValue( ((Date) origValue));
					modCell.setCellStyle(dateCellStyle);
				} else if(origValue instanceof Boolean) {
					modCell.setCellValue( ((Boolean) origValue));
				}
			}
		}

		// fixed size at the end
		for(int i = 0; i < headers.length; i++) {
			sheet.setColumnWidth(i, 5_000);
		}
		
		// write file
		ExcelUtility.writeToFile(workbook, fileLocation);
	}

}
 