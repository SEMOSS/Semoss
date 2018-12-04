package prerna.poi.main.helper.excel;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.poi.main.helper.FileHelperUtil;

public class ExcelParsing {

	private static final int NUM_ROWS_TO_PREDICT_TYPES = 500;
	
	private ExcelParsing() {
		
	}
	
	public static boolean isExcelFile(String filePath) {
		String file = filePath.toLowerCase();
		if(file.endsWith(".xlsx") || file.endsWith("xlsm") || file.endsWith("xls")) {
			return true;	
		}
		
		return false;
		
	}
	public static boolean isEmptyCell(Cell thisCell) {
		if (thisCell == null 
				|| thisCell.getCellType() == Cell.CELL_TYPE_BLANK
				|| thisCell.toString().trim().isEmpty()) {
			return true;
		}
		return false;
	}
	
	/**
	 * Get the cell values
	 * @param thisCell
	 * @return
	 */
	public static Object getCell(Cell thisCell) {
		if(thisCell == null) {
			return "";
		}
		int type = thisCell.getCellType();
		if(type == Cell.CELL_TYPE_BLANK) {
			return "";
		}
		if(type == Cell.CELL_TYPE_STRING) {
			return thisCell.getStringCellValue();
		} else if(type == Cell.CELL_TYPE_NUMERIC) {
			if(DateUtil.isCellDateFormatted(thisCell)) {
				return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
			}
			return thisCell.getNumericCellValue();
		} else if(type == Cell.CELL_TYPE_BOOLEAN) {
			return thisCell.getBooleanCellValue() + "";
		} else if(type == Cell.CELL_TYPE_FORMULA) {
			// do the same for the formula value
			int formulatype = thisCell.getCachedFormulaResultType();
			if(formulatype == Cell.CELL_TYPE_BLANK) {
				return "";
			}
			if(formulatype == Cell.CELL_TYPE_STRING) {
				return thisCell.getStringCellValue();
			} else if(formulatype == Cell.CELL_TYPE_NUMERIC) {
				if(DateUtil.isCellDateFormatted(thisCell)) {
					return new SemossDate(thisCell.getDateCellValue(), thisCell.getCellStyle().getDataFormatString());
				}
				return thisCell.getNumericCellValue();
			} else if(formulatype == Cell.CELL_TYPE_BOOLEAN) {
				return thisCell.getBooleanCellValue();
			}
		}
		return "";
	}
	
	
	/*
	 * Methods around predicting types
	 */

	public static Object[][] predictTypes(Sheet sheet, String range) {
		// for a given sheet
		// loop through and determine the types
		// based on a block in a given range
		
		// range index is start col, start row, end col, end row
		int[] rangeIndex = ExcelRange.getSheetRangeIndex(range);
		
		int numCols = rangeIndex[2] - rangeIndex[0] + 1;
		
		Object[][] predictedTypes = new Object[numCols][3];
		List<Map<String, Integer>> additionalFormatTracker = new Vector<Map<String, Integer>>(numCols);

		// Loop through cols, and up to 1000 rows
		int counter = 0;
		for(int colIndex = rangeIndex[0]; colIndex <= rangeIndex[2]; colIndex++) {
			predictTypesLoop(sheet, rangeIndex, predictedTypes, additionalFormatTracker, colIndex, counter);
			counter++;
		}
		
		return predictedTypes;
	}
	
	private static void predictTypesLoop(Sheet sheet, int[] rangeIndex, Object[][] predictedTypes, List<Map<String, Integer>> additionalFormatTracker, int cellIndex, int colIndex) {
		int startRow = rangeIndex[1];
		int endRow = rangeIndex[3];
		// only use up to 500 rows for determining the types
		if(endRow - startRow > NUM_ROWS_TO_PREDICT_TYPES) {
			endRow = startRow + NUM_ROWS_TO_PREDICT_TYPES;
		}

		SemossDataType type = null;
		Map<String, Integer> formatTracker = new HashMap<String, Integer>();
		additionalFormatTracker.add(formatTracker);
		
		ROW_LOOP : for(int j = startRow; j < endRow; j++) {
			Row row = sheet.getRow(j);
			if(row != null) {
				// remember, excel is 1 based while java is 0 based
				Object value = ExcelParsing.getCell(row.getCell(cellIndex-1));
				if(value instanceof String && value.toString().isEmpty()) {
					continue ROW_LOOP;
				}
				
				SemossDataType newTypePrediction = getTypeByCast(value);
				String additionalFormatting = null;
				if(value instanceof SemossDate) {
					additionalFormatting = ((SemossDate) value).getPattern();
				}
				
				// handle the additional formatting
				if(additionalFormatting != null) {
					if(formatTracker.containsKey(additionalFormatting)) {
						// increase counter by 1
						formatTracker.put(additionalFormatting, new Integer(formatTracker.get(additionalFormatting) + 1));
					} else {
						formatTracker.put(additionalFormatting, new Integer(1));
					}
				}
				
				// if we hit a string
				// we are done
				if(newTypePrediction == SemossDataType.STRING || newTypePrediction == SemossDataType.BOOLEAN) {
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = SemossDataType.STRING;
					predictedTypes[colIndex] = columnPrediction;
					break ROW_LOOP;
				}
				
				if(type == null) {
					// this is the first time we go through
					// just set the type and we are done
					// we only need to go through when we hit a difference
					type = newTypePrediction;
					continue;
				}
				
				if(type == newTypePrediction) {
					// well, nothing for us to do if its the same
					// again, we handle additional formatting
					// at the top
					continue;
				}
				
				// if we hit an integer
				else if(newTypePrediction == SemossDataType.INT) {
					if(type == SemossDataType.DOUBLE) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a double
				else if(newTypePrediction == SemossDataType.DOUBLE) {
					if(type == SemossDataType.INT) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a date
				else if(newTypePrediction == SemossDataType.DATE) {
					if(type == SemossDataType.TIMESTAMP) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
				
				// if we hit a timestamp
				else if(newTypePrediction == SemossDataType.TIMESTAMP) {
					if(type == SemossDataType.DATE) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break ROW_LOOP;
					}
				}
			}
		}

		// if an entire column is empty, type will be null
		// why someone has a csv file with an empty column, i do not know...
		if(type == null) {
			type = SemossDataType.STRING;
		}

		// if format tracking is empty
		// just add the type to the matrix
		// and continue
		if(formatTracker.isEmpty()) {
			Object[] columnPrediction = new Object[2];
			columnPrediction[0] = type;
			predictedTypes[colIndex] = columnPrediction;
		} else {
			// format tracker is not empty
			// need to figure out the date situation
			if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				Object[] results = FileHelperUtil.determineDateFormatting(type, formatTracker);
				predictedTypes[colIndex] = results;
			} else {
				// UGH... how did you get here if you are not a date???
				Object[] columnPrediction = new Object[2];
				columnPrediction[0] = type;
				predictedTypes[colIndex] = columnPrediction;
			}
		}
	}
	
	/**
	 * Predict the type via casting
	 * @param value
	 * @return
	 */
	public static SemossDataType getTypeByCast(Object value) {
		if(value instanceof String) {
			return SemossDataType.STRING;
		} else if(value instanceof Number) {
			// check if actually a number
			if( ((Number) value).doubleValue() == Math.rint(((Number) value).doubleValue()) ) {
				return SemossDataType.INT;
			}
			return SemossDataType.DOUBLE;
		} else if(value instanceof SemossDate) {
			// not a perfect check by any means
			// but quick and easy to do
			if(hasTime( ((SemossDate) value).getDate() )) {
				return SemossDataType.TIMESTAMP;
			} else {
				return SemossDataType.DATE;
			}
		} else if(value instanceof Boolean) {
			return SemossDataType.BOOLEAN;
		}
		
		return SemossDataType.STRING;
	}
	
    /**
     * Determines whether or not a date has any time values.
     * @param date The date.
     * @return true iff the date is not null and any of the date's hour, minute,
     * seconds or millisecond values are greater than zero.
     */
	public static boolean hasTime(Date date) {
        if (date == null) {
            return false;
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        if (c.get(Calendar.HOUR_OF_DAY) > 0) {
            return true;
        }
        if (c.get(Calendar.MINUTE) > 0) {
            return true;
        }
        if (c.get(Calendar.SECOND) > 0) {
            return true;
        }
        if (c.get(Calendar.MILLISECOND) > 0) {
            return true;
        }
        return false;
    } 
}
