/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.vector.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.query.querystruct.CsvQueryStruct;

public class VectorDatabaseMetadataCSVTable {

	public static final String SOURCE = VectorDatabaseCSVTable.SOURCE;
	public static final String ATTRIBUTE = "Attribute";
	public static final String STR_VALUE = "Str_Value";
	public static final String INT_VALUE = "Int_Value";
	public static final String NUM_VALUE = "Num_Value";
	public static final String BOOL_VALUE = "Bool_Value";
	public static final String DATE_VAL = "Date_Value";
	public static final String TIMESTAMP_VAL = "Timestamp_Value";

	public List<VectorDatabaseMetadataCSVRow> rows;
	private File file;

	public VectorDatabaseMetadataCSVTable() {
		this.rows = new ArrayList<>();
	}

	public void addRow(String source, String attribute, String strValue, Number intValue, Number numValue,
			Boolean boolValue, SemossDate dateValue, SemossDate timestampValue) {
		VectorDatabaseMetadataCSVRow newRow = new VectorDatabaseMetadataCSVRow(source, attribute, strValue, intValue,
				numValue, boolValue, dateValue, timestampValue);
		this.rows.add(newRow);
	}

	public List<VectorDatabaseMetadataCSVRow> getRows() {
		return this.rows;
	}

	/**
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static VectorDatabaseMetadataCSVTable initCSVTable(File file) throws IOException {
		return initCSVTable(file, -1);
	}

	/**
	 * @param file
	 * @param limit
	 * @return
	 * @throws IOException
	 */
	public static VectorDatabaseMetadataCSVTable initCSVTable(File file, long limit) throws IOException {
		VectorDatabaseMetadataCSVTable csvTable = new VectorDatabaseMetadataCSVTable();
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
				new String[]{SOURCE, ATTRIBUTE, STR_VALUE, INT_VALUE, NUM_VALUE, BOOL_VALUE, DATE_VAL, TIMESTAMP_VAL},
				new String[]{STR_DT, STR_DT, STR_DT, INT_DT, NUM_DT, BOOL_DT, DATE_DT, TIMESTAMP_DT});
		if (limit > 0) {
			qs.setLimit(limit);
		}
		CsvFileIterator csvIt = null;
		try {
			csvIt = new CsvFileIterator(qs);
			while (csvIt.hasNext()) {
				Object[] row = csvIt.next().getValues();
				csvTable.addRow((String) row[0], (String) row[1], (String) row[2], (Number) row[3], (Number) row[4],
						(Boolean) row[5], (SemossDate) row[6], (SemossDate) row[7]);
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
		VectorDatabaseMetadataCSVTable csvTable = initCSVTable(file, 1);
		if (!csvTable.getRows().isEmpty()) {
			return true;
		}

		return false;
	}
}
