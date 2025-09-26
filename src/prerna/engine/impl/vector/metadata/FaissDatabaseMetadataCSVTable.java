package prerna.engine.impl.vector.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.impl.vector.FaissDatabaseCSVTable;
import prerna.query.querystruct.CsvQueryStruct;

public class FaissDatabaseMetadataCSVTable {

	public static final String ID = "ID";
	public static final String SOURCE = FaissDatabaseCSVTable.SOURCE;
	public static final String ATTRIBUTE = "Attribute";
	public static final String STR_VALUE = "Str_Value";
	public static final String INT_VALUE = "Int_Value";
	public static final String NUM_VALUE = "Num_Value";
	public static final String BOOL_VALUE = "Bool_Value";
	public static final String DATE_VAL = "Date_Value";
	public static final String TIMESTAMP_VAL = "Timestamp_Value";

	public List<FaissDatabaseMetadataCSVRow> rows;
	private File file;

	public FaissDatabaseMetadataCSVTable() {
		this.rows = new ArrayList<>();
	}

	// Updated method: requires an id as the first parameter
	public void addRow(String id, String source, String attribute, String strValue, Number intValue, Number numValue,
			Boolean boolValue, SemossDate dateValue, SemossDate timestampValue) {

		FaissDatabaseMetadataCSVRow newRow = new FaissDatabaseMetadataCSVRow(id, source, attribute, strValue, intValue,
				numValue, boolValue, dateValue, timestampValue);
		this.rows.add(newRow);
	}

	public List<FaissDatabaseMetadataCSVRow> getRows() {
		return this.rows;
	}

	/**
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static FaissDatabaseMetadataCSVTable initCSVTable(File file) throws IOException {
		return initCSVTable(file, -1);
	}

	/**
	 * @param file
	 * @param limit
	 * @return
	 * @throws IOException
	 */
	public static FaissDatabaseMetadataCSVTable initCSVTable(File file, long limit) throws IOException {
		FaissDatabaseMetadataCSVTable csvTable = new FaissDatabaseMetadataCSVTable();
		csvTable.file = file;

		final String STR_DT = SemossDataType.STRING.toString();
		final String INT_DT = SemossDataType.INT.toString();
		final String NUM_DT = SemossDataType.DOUBLE.toString();
		final String BOOL_DT = SemossDataType.BOOLEAN.toString();
		final String DATE_DT = SemossDataType.DATE.toString();
		final String TIMESTAMP_DT = SemossDataType.TIMESTAMP.toString();

		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setDelimiter(',');
		qs.setFilePath(file.getAbsolutePath());
		qs.setSelectorsAndTypes(
				new String[] { ID, SOURCE, ATTRIBUTE, STR_VALUE, INT_VALUE, NUM_VALUE, BOOL_VALUE, DATE_VAL,
						TIMESTAMP_VAL },
				new String[] { STR_DT, STR_DT, STR_DT, STR_DT, INT_DT, NUM_DT, BOOL_DT, DATE_DT, TIMESTAMP_DT });
		if (limit > 0) {
			qs.setLimit(limit);
		}

		CsvFileIterator csvIt = null;
		try {
			csvIt = new CsvFileIterator(qs);
			while (csvIt.hasNext()) {
				Object[] row = csvIt.next().getValues();
				csvTable.addRow((String) row[0], // id
						(String) row[1], // source
						(String) row[2], // attribute
						(String) row[3], // strValue
						(Number) row[4], // intValue
						(Number) row[5], // numValue
						(Boolean) row[6], // boolValue
						(SemossDate) row[7], // dateValue
						(SemossDate) row[8] // timestampValue
				);
			}
		} finally {
			if (csvIt != null) {
				csvIt.close();
			}
		}

		return csvTable;
	}

	/**
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static boolean validateCSVTable(File file) throws IOException {
		FaissDatabaseMetadataCSVTable csvTable = initCSVTable(file, 1);
		return !csvTable.getRows().isEmpty();
	}
}
