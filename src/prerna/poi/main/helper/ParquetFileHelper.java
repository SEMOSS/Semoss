/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.poi.main.helper;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import prerna.algorithm.api.SemossDataType;
import prerna.om.HeadersException;

public class ParquetFileHelper {

	private static final Logger classLogger = LogManager.getLogger(ParquetFileHelper.class);

	private String fileLocation = null;
	private ParquetReader<Group> reader = null;
	private MessageType schema = null;
	// we need to keep two sets of headers
	// we will keep the headers as is within the physical file
	private String[] allParquetHeaders = null;
	// that is all good and all, but when we have duplicates, it
	// messes things up. to reduce complexity elsewhere, we will just
	// create a new unique csv headers string[] to store the values
	// this will in essence become the new "physical names" for each column
	private List<String> newUniqueParquetHeaders = null;

	// keep track of integer with values s.t. we can easily reset to get all the
	// values without getting an error when there are duplicate headers within the
	// univocity api
	// this will literally be [0,1,2,3,...,n] where n = number of columns - 1
	private Integer[] headerIntegerArray = null;

	// keep track of the current headers being used
	public String[] currHeaders = null;

	/*
	 * THIS IS REALLY ANNOYING In thick client, need to know if the last column is
	 * the path to the prop file location for csv upload
	 */
	private boolean propFileExists = false;

	public ParquetFileHelper() {

	}

	public static boolean isParquetFile(String filePath) {
		String file = filePath.toLowerCase();
		if (file.endsWith(".parquet")) {
			return true;
		}
		return false;
	}

	/**
	 * Parse the new file passed
	 * 
	 * @param fileLocation The String location of the fileName
	 */
	public void parse(String fileLocation) {
		this.fileLocation = fileLocation;
		createReader();
		collectHeaders();
	}

	/**
	 * Return the headers for parquet file
	 * 
	 * @return
	 */
	public String[] getHeaders() {
		if (this.currHeaders == null) {
			collectHeaders();
			return this.newUniqueParquetHeaders.toArray(new String[this.newUniqueParquetHeaders.size()]);
		}
		return this.currHeaders;
	}

	/**
	 * 
	 */
	private void createReader() {
		try {
			GroupReadSupport readSupport = new GroupReadSupport();
			this.reader = ParquetReader.builder(readSupport, new org.apache.hadoop.fs.Path(this.fileLocation))
					.withConf(new Configuration()).build();
			// read the schema
			try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(Paths.get(fileLocation)))) {
				this.schema = fileReader.getFooter().getFileMetaData().getSchema();
			}
		} catch (IOException e) {
			classLogger.error("Failed to create parquet reader: {}", e.getMessage(), e);
		}
	}

	/**
	 * 
	 */
	public void collectHeaders() {
		if (allParquetHeaders == null) {
			if (this.reader == null) {
				createReader();
			}

			int numCols = schema.getFieldCount();
			String[] cols = new String[numCols];
			for (int i = 0; i < schema.getFieldCount(); ++i) {
				cols[i] = schema.getFields().get(i).getName();
			}
			allParquetHeaders = cols;

			// need to keep track and make sure our headers are good
			if (allParquetHeaders == null) {
				throw new IllegalArgumentException("No headers found");
			}
			numCols = allParquetHeaders.length;

			// TODO circle back on this
			/*
			 * THIS IS REALLY ANNOYING In thick client, need to know if the last column is
			 * the path to the prop file location for csv upload
			 */
			if (propFileExists) {
				numCols--;
			}
			newUniqueParquetHeaders = new ArrayList<>(numCols);

			// create the integer array s.t. we can reset the value to get in the future
			headerIntegerArray = new Integer[numCols];
			// grab the headerChecker
			HeadersException headerChecker = HeadersException.getInstance();

			for (int colIdx = 0; colIdx < numCols; colIdx++) {
				// just trim all the headers
				allParquetHeaders[colIdx] = allParquetHeaders[colIdx].trim();
				String origHeader = allParquetHeaders[colIdx];
				if (origHeader.trim().isEmpty()) {
					origHeader = "BLANK_HEADER";
				}
				String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueParquetHeaders);

				// now update the unique headers, as this will be used to match duplications
				newUniqueParquetHeaders.add(newHeader);

				// fill in integer array
				headerIntegerArray[colIdx] = colIdx;
			}

			currHeaders = newUniqueParquetHeaders.toArray(new String[] {});
		}
	}

	public void modifyCleanedHeaders(Map<String, String> thisFileHeaderChanges) {
		// iterate through all sets of oldHeader -> newHeader
		collectHeaders();
		for (String desiredNewHeaderValue : thisFileHeaderChanges.keySet()) {
			String oldHeader = thisFileHeaderChanges.get(desiredNewHeaderValue);

			// since the user may not want all the headers, we only check if new headers are
			// valid
			// based on the headers they want
			// thus, we need to check and see if the newHeaderValue is actually already used
			int newNameIndex = this.newUniqueParquetHeaders.indexOf(desiredNewHeaderValue);
			if (newNameIndex >= 0) {
				// this new header exists
				// lets modify it
				this.newUniqueParquetHeaders.set(newNameIndex, "NOT_USED_COLUMN_1234567890");
			}

			// now we modify what was the old header to be the new header
			int oldHeaderIndex = this.newUniqueParquetHeaders.indexOf(oldHeader);
			this.newUniqueParquetHeaders.set(oldHeaderIndex, desiredNewHeaderValue);
			currHeaders = newUniqueParquetHeaders.toArray(new String[] {});
		}
	}

	/**
	 * Set a limit on which columns you want to be parsed
	 * 
	 * @param columns The String[] containing the headers you want
	 */
	public void parseColumns(String[] columns) {
		// map it back to clean columns
		if (newUniqueParquetHeaders == null) {
			collectHeaders();
		}
		// must use index for when there are duplicate values
		Integer[] values = new Integer[columns.length];
		for (int colIdx = 0; colIdx < columns.length; colIdx++) {
			values[colIdx] = newUniqueParquetHeaders.indexOf(columns[colIdx]);
		}
		currHeaders = columns;
	}

	/**
	 * 
	 * @return
	 */
	public Object[] getNextRow() {
		try {
			Group group = this.reader.read();
			if (group == null) {
				return null;
			}

			Object[] row = new Object[this.currHeaders.length];
			for (int i = 0; i < this.currHeaders.length; i++) {
				String header = this.currHeaders[i];
				int headerIndex = this.newUniqueParquetHeaders.indexOf(header);

				if (group.getFieldRepetitionCount(headerIndex) > 0) {
					Type t = this.schema.getFields().get(headerIndex);
					PrimitiveTypeName parquetType = t.asPrimitiveType().getPrimitiveTypeName();
					LogicalTypeAnnotation logicalType = t.getLogicalTypeAnnotation();

					switch (parquetType) {
					case BOOLEAN:
						row[i] = group.getBoolean(headerIndex, 0);
						break;

					case INT32:
						if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
							int days = group.getInteger(headerIndex, 0);
							row[i] = LocalDate.ofEpochDay(days);
						} else {
							row[i] = group.getInteger(headerIndex, 0);
						}
						break;

					case INT64:
						if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
							LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
							long raw = group.getLong(headerIndex, 0);
							Instant instant;

							switch (ts.getUnit()) {
							case MICROS:
								instant = Instant.ofEpochMilli(raw / 1000); // micros -> millis
								break;
							case MILLIS:
							default:
								instant = Instant.ofEpochMilli(raw);
								break;
							}

							row[i] = LocalDateTime.ofInstant(instant, ZoneOffset.UTC); // LocalDateTime
						} else {
							row[i] = group.getLong(headerIndex, 0);
						}
						break;

					case FLOAT:
						row[i] = group.getFloat(headerIndex, 0);
						break;

					case DOUBLE:
						row[i] = group.getDouble(headerIndex, 0);
						break;

					case BINARY:
						row[i] = group.getBinary(headerIndex, 0).toStringUsingUTF8();
						break;

					case FIXED_LEN_BYTE_ARRAY:
						// often DECIMAL - leaving as string for now
						row[i] = group.getValueToString(headerIndex, 0);
						break;

					default:
						row[i] = group.getValueToString(headerIndex, 0);
						break;
					}
				} else {
					row[i] = null;
				}
			}
			return row;
		} catch (IOException e) {
			classLogger.error("Failed to read next parquet row: {}", e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 
	 */
	public void reset() {
		clear();
		createReader();
	}

	/**
	 * 
	 */
	public void clear() {
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (IOException e) {
				classLogger.error("Failed to close parquet reader: {}", e.getMessage(), e);
			}
		}
	}

	/**
	 * Return the headers and data valid types of the parquet file
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Map<String, SemossDataType> getHeadersAndDataTypes(String fileLocation) throws IOException {
		Map<String, SemossDataType> headersPlusDataTypes = new LinkedHashMap<>();

		ParquetFileReader reader = null;
		try {
			reader = ParquetFileReader.open(new LocalInputFile(Paths.get(fileLocation)));
			MessageType schema = reader.getFooter().getFileMetaData().getSchema();

			int numCols = schema.getFieldCount();
			List<ColumnDescriptor> columns = schema.getColumns();
			List<String[]> paths = schema.getPaths();
			boolean[] missingColumns = new boolean[numCols];

			for (int i = 0; i < schema.getFieldCount(); ++i) {
				Type t = schema.getFields().get(i);

				if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
					throw new UnsupportedOperationException("Complex types not supported.");
				}

				String colName = t.getName();
				PrimitiveTypeName primitiveType = t.asPrimitiveType().getPrimitiveTypeName();
				LogicalTypeAnnotation logicalType = t.getLogicalTypeAnnotation();

				SemossDataType dtype;
				switch (primitiveType) {
				case BOOLEAN:
					dtype = SemossDataType.BOOLEAN;
					break;

				case INT32:
					if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
						dtype = SemossDataType.DATE;
					} else {
						dtype = SemossDataType.INT;
					}
					break;

				case INT64:
					if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
						dtype = SemossDataType.TIMESTAMP;
					} else {
						dtype = SemossDataType.INT;
					}
					break;

				case FLOAT:
				case DOUBLE:
					dtype = SemossDataType.DOUBLE;
					break;

				case FIXED_LEN_BYTE_ARRAY:
				case BINARY:
					if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
						dtype = SemossDataType.DOUBLE;
					} else {
						dtype = SemossDataType.STRING;
					}
					break;

				default:
					dtype = SemossDataType.STRING;
				}

				String[] colPath = paths.get(i);
				if (schema.containsPath(colPath)) {
					ColumnDescriptor fd = schema.getColumnDescription(colPath);
					if (!fd.equals(columns.get(i))) {
						throw new UnsupportedOperationException("Schema evolution not supported.");
					}
				} else {
					if (columns.get(i).getMaxDefinitionLevel() == 0) {
						throw new IOException(
								"Required column is missing in data file. Col: " + Arrays.toString(colPath));
					}
					missingColumns[i] = true;
				}

				headersPlusDataTypes.put(colName, dtype);
			}

			return headersPlusDataTypes;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					classLogger.error(e.toString());
				}
			}
		}
	}

}
