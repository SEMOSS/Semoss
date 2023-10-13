package prerna.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.ITask;

public class PPTDataHandler {
	
	private Map<String, List<Object>> dataMap = new HashMap<String, List<Object>>();
	private String[] headers = null;
	private SemossDataType[] typesArr = null;

	public void setData(ITask task) {
		int i = 0;
		int size = 0;

		// Grab headers
		if (task.hasNext()) {
			IHeadersDataRow row = task.next();
			List<Map<String, Object>> headerInfo = task.getHeaderInfo();

			// generate the header row
			// and define constants used throughout like size, and types
			i = 0;
			this.headers = row.getHeaders();
			size = this.headers.length;

			this.typesArr = new SemossDataType[size];
			for (; i < size; i++) {
				this.typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
			}

			Object[] dataRow = row.getValues();
			for (i = 0; i < size; i++) {
				List<Object> data = new ArrayList<Object>();
				String header = this.headers[i];

				Object value = dataRow[i];
				if (value == null) {
					data.add("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						data.add(value);
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						data.add(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						if(value instanceof SemossDate) {
							data.add( ((SemossDate) value).getDate() ) ;
						} else {
							data.add(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							data.add( ((SemossDate) value).getDate() ) ;
						} else {
							data.add(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						data.add(Boolean.toString((boolean) value));
					} else {
						data.add(value + "");
					}
				}

				this.dataMap.put(header, data);
			}
		}

		// now iterate through all the data
		while (task.hasNext()) {
			IHeadersDataRow row = task.next();
			Object[] dataRow = row.getValues();

			for (i = 0; i < size; i++) {
				String header = headers[i];
				List<Object> data = this.dataMap.get(header);

				Object value = dataRow[i];
				if (value == null) {
					data.add("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						data.add(value);
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						data.add(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						if(value instanceof SemossDate) {
							data.add( ((SemossDate) value).getDate() ) ;
						} else {
							data.add(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							data.add( ((SemossDate) value).getDate() ) ;
						} else {
							data.add(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						data.add(Boolean.toString((boolean) value));
					} else {
						data.add(value + "");
					}
				}
			}
		}
	}

	private String getColumnDataType(String col) {
		if (!this.dataMap.containsKey(col)) {
			return null;
		}

		List<Object> colAsList = this.dataMap.get(col);
		if (colAsList.size() == 0) {
			return null;
		}

		Object firstItem = colAsList.get(0);
		if (firstItem instanceof String) {
			return "String";
		} else if (firstItem instanceof Number) {
			return "Number";
		}

		return null;

	}

	/**
	 * Return a column as an array of strings (meant for
	 * XDDFDataSourcesFactory.fromArray())
	 * 
	 * @param col
	 * @return
	 */
	public String[] getColumnAsStringArray(String col) {
		if (!this.dataMap.containsKey(col)) {
			return null;
		}

		List<Object> colAsList = this.dataMap.get(col);
		String[] colAsStringArray = colAsList.toArray(new String[colAsList.size()]);

		return colAsStringArray;
	}

	/**
	 * Return a column as an array of numbers (meant for
	 * XDDFDataSourcesFactory.fromArray())
	 * 
	 * @param col
	 * @return
	 */
	public Number[] getColumnAsNumberArray(String col) {
		if (!this.dataMap.containsKey(col)) {
			return null;
		}

		List<Object> colAsList = this.dataMap.get(col);
		Number[] colAsNumberArray = new Number[colAsList.size()];
		for (int i = 0; i < colAsList.size(); i++) {
			if (colAsList.get(i) != null && !"".equals(colAsList.get(i))) {
				colAsNumberArray[i] = (Number) colAsList.get(i);
			} else {
				colAsNumberArray[i] = 0;
			}
		}

		return colAsNumberArray;
	}	
	
	/**
	 * Automatically figure out whether to build XDDFDataSource 
	 * from String Array or Number Array
	 * 
	 * @param col
	 * @return
	 */
	public XDDFDataSource<?> getColumnAsXDDFDataSource(String col) {
		String colDataType = getColumnDataType(col);
		if (colDataType.equals("String")) {
			String[] stringArray = getColumnAsStringArray(col);
			return XDDFDataSourcesFactory.fromArray(stringArray);
		} else if (colDataType.equals("Number")) {
			Number[] numberArray = getColumnAsNumberArray(col);
			return XDDFDataSourcesFactory.fromArray(numberArray);
		}

		return null;
	}

	/**
	 * Getting the correct XDDFDataSource based on the data type of array
	 * 
	 * @param col
	 * @param xAxisIndx
	 * @return
	 */
	public XDDFDataSource<?> getColumnAsXDDFDataSourceByType(String col, int xAxisIndx) {
		if (this.typesArr[xAxisIndx] == SemossDataType.STRING || 
				this.typesArr[xAxisIndx] == SemossDataType.DATE || 
				this.typesArr[xAxisIndx] == SemossDataType.TIMESTAMP) {
			String[] stringArray = getColumnAsStringArray(col);
			return XDDFDataSourcesFactory.fromArray(stringArray);
		} else if (this.typesArr[xAxisIndx] == SemossDataType.INT
				|| this.typesArr[xAxisIndx] == SemossDataType.DOUBLE) {
			Number[] numberArray = getColumnAsNumberArray(col);
			return XDDFDataSourcesFactory.fromArray(numberArray);
		}

		return null;
	}
}
