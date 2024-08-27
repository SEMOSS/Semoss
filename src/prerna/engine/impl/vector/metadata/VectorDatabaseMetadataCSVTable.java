package prerna.engine.impl.vector.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.query.querystruct.CsvQueryStruct;

public class VectorDatabaseMetadataCSVTable {
	
	public static final String SOURCE = "Source";
	public static final String ATTRIBUTE = "Attribute";
	public static final String STR_VALUE = "Str_Value";
	public static final String INT_VALUE = "Int_Value";
	public static final String NUM_VALUE = "Num_Value";
	public static final String BOOL_VALUE = "Bool_Value";
	public static final String DATE_VAL = "Date_Value";
	public static final String TIMESTAMP_VAL = "Timestamp_Value";

    public List<VectorDatabaseMetadataCSVRow> rows;
	
    public VectorDatabaseMetadataCSVTable() {
        this.rows = new ArrayList<>();
    }

    public void addRow(String source, String attribute, String strValue, Number intValue, Number numValue, Boolean boolValue, SemossDate dateValue, SemossDate timestampValue) {
    	VectorDatabaseMetadataCSVRow newRow = new VectorDatabaseMetadataCSVRow(source, attribute, strValue, intValue, numValue, boolValue, dateValue, timestampValue);
        this.rows.add(newRow);
    }
    
    public List<VectorDatabaseMetadataCSVRow> getRows() {
    	return this.rows;
    }
    
    public static VectorDatabaseMetadataCSVTable initCSVTable(File file) throws IOException {
    	VectorDatabaseMetadataCSVTable csvTable = new VectorDatabaseMetadataCSVTable();
    	
    	final String STR_DT = SemossDataType.STRING.toString();
    	final String INT_DT = SemossDataType.INT.toString();
    	final String NUM_DT = SemossDataType.DOUBLE.toString();
    	final String BOOL_DT = SemossDataType.BOOLEAN.toString();
    	final String DATE_DT = SemossDataType.DATE.toString();
    	final String TIMESTAMP_DT = SemossDataType.TIMESTAMP.toString();

    	CsvQueryStruct qs = new CsvQueryStruct();
    	qs.setDelimiter(',');
    	qs.setFilePath(file.getAbsolutePath());
    	qs.setSelectorsAndTypes(new String[] {SOURCE, ATTRIBUTE, STR_VALUE, INT_VALUE, NUM_VALUE, BOOL_VALUE, DATE_VAL, TIMESTAMP_VAL}, 
    			new String[] {STR_DT, STR_DT, STR_DT, INT_DT, NUM_DT, BOOL_DT, DATE_DT, TIMESTAMP_DT});
    	
    	CsvFileIterator csvIt = null;
    	try {
    		csvIt = new CsvFileIterator(qs);
        	while(csvIt.hasNext()) {
        		Object[] row = csvIt.next().getValues();
        		csvTable.addRow(
        				(String) row[0],
        				(String) row[1],
        				(String) row[2],
        				(Number) row[3],
        				(Number) row[4],
        				(Boolean) row[5],
        				(SemossDate) row[6],
        				(SemossDate) row[7]
    				);
        	}
    	} finally {
    		if(csvIt != null) {
    			csvIt.close();
    		}
    	}

		return csvTable;
    }
}
