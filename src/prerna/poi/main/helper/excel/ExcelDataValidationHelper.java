package prerna.poi.main.helper.excel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.DataValidationConstraint.ValidationType;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

/**
 * This class gets the data validation constraints from an excel sheet
 */
public class ExcelDataValidationHelper {
	public enum WIDGET_COMPONENT {
		CHECKLIST, DROPDOWN, EXECUTE, FREETEXT, NUMBER, RADIO, SLIDER, TEXTAREA, TYPEAHEAD
	};

	public static Map<String, Object> getDataValidation(Sheet sheet) {
		Map<String, Object> validationMap = new HashMap<>();
		List<? extends DataValidation> validations = sheet.getDataValidations();
		for (DataValidation dv : validations) {
			Map<String, Object> headerMeta = new HashMap<>();
			DataValidationConstraint constraint = dv.getValidationConstraint();
			CellRangeAddressList region = dv.getRegions();
			CellRangeAddress[] cellRangeAddresses = region.getCellRangeAddresses();
			String cleanHeader = null;
			for (CellRangeAddress rangeAddress : cellRangeAddresses) {
				String address = rangeAddress.formatAsString();
				// get the header for the range
				String[] split = address.split(":");
				CellReference cellReference = new CellReference(split[0]);
				Row row = sheet.getRow(cellReference.getRow() - 1);
				Cell c = row.getCell(cellReference.getCol());
				Object header = ExcelParsing.getCell(c);
				cleanHeader = Utility.cleanString(header.toString(), true, true, false);
				headerMeta.put("range", address);
			}
			boolean allowEmptyCells = dv.getEmptyCellAllowed();
			int validationType = constraint.getValidationType();
			headerMeta.put("emptyCells", allowEmptyCells);
			headerMeta.put("validationType", validationTypeToString(validationType));
			if (validationType == DataValidationConstraint.ValidationType.ANY) {
				headerMeta.put("type", SemossDataType.STRING.toString());
			} else if (validationType == DataValidationConstraint.ValidationType.INTEGER
					|| validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH
					|| validationType == DataValidationConstraint.ValidationType.DECIMAL) {
				int operator = constraint.getOperator();
				String formula1 = constraint.getFormula1();
				String formula2 = constraint.getFormula2();
				headerMeta.put("operator", operatorToString(operator));
				headerMeta.put("f1", formula1);
				if (formula2 != null) {
					headerMeta.put("f2", formula2);
				}
				if(validationType == DataValidationConstraint.ValidationType.INTEGER) {
					headerMeta.put("type", SemossDataType.INT.toString());
				}
				if(validationType == DataValidationConstraint.ValidationType.DECIMAL) {
					headerMeta.put("type", SemossDataType.DOUBLE.toString());
				}
				if(validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
					headerMeta.put("type", SemossDataType.STRING.toString());
				}

			} else if (validationType == DataValidationConstraint.ValidationType.LIST) {
				String[] values = constraint.getExplicitListValues();
				headerMeta.put("values", values);
				headerMeta.put("type", SemossDataType.STRING.toString());
			} else if (validationType == DataValidationConstraint.ValidationType.DATE) {
				int operator = constraint.getOperator();
				String formula1 = constraint.getFormula1();
				String formula2 = constraint.getFormula2();
				headerMeta.put("operator", operatorToString(operator));
				headerMeta.put("f1", formula1);
				if (formula2 != null) {
					headerMeta.put("f2", formula2);
				}
				headerMeta.put("type", SemossDataType.DATE.toString());
			} else if (validationType == DataValidationConstraint.ValidationType.TIME) {
				int operator = constraint.getOperator();
				String formula1 = constraint.getFormula1();
				String formula2 = constraint.getFormula2();
				headerMeta.put("operator", operatorToString(operator));
				headerMeta.put("f1", formula1);
				if (formula2 != null) {
					headerMeta.put("f2", formula2);
				}
				headerMeta.put("type", SemossDataType.TIMESTAMP.toString());

			} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {
				headerMeta.put("type", SemossDataType.STRING.toString());
			}
			if (cleanHeader != null) {
				validationMap.put(cleanHeader, headerMeta);
			}

		}
		return validationMap;

	}

	public static String validationTypeToString(int validationType) {
		String validationName = "";
		if (validationType == DataValidationConstraint.ValidationType.ANY) {
			validationName = "ANY";
		} else if (validationType == DataValidationConstraint.ValidationType.INTEGER) {
			validationName = "INTEGER";
		} else if (validationType == DataValidationConstraint.ValidationType.DECIMAL) {
			validationName = "DECIMAL";
		} else if (validationType == DataValidationConstraint.ValidationType.LIST) {
			validationName = "LIST";
		} else if (validationType == DataValidationConstraint.ValidationType.DATE) {
			validationName = "DATE";
		} else if (validationType == DataValidationConstraint.ValidationType.TIME) {
			validationName = "TIME";
		} else if (validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
			validationName = "TEXT_LENGTH";
		} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {
			validationName = "FORMULA";
		}
		return validationName;
	}
	
	public static int stringToValidationType(String validationType) {
		int vt = ValidationType.ANY;
		if (validationType.equals("ANY")) {
			vt = DataValidationConstraint.ValidationType.ANY;
		} else if (validationType.equals("INTEGER")) {
			vt = DataValidationConstraint.ValidationType.INTEGER; 
		} else if (validationType.equals("DECIMAL")) {
			vt = DataValidationConstraint.ValidationType.DECIMAL; 
		} else if (validationType.equals("LIST")) {
			vt = DataValidationConstraint.ValidationType.LIST; 
		} else if (validationType.equals("DATE")) {
			vt = DataValidationConstraint.ValidationType.DATE; 
		} else if (validationType.equals("TIME")) {
			vt = DataValidationConstraint.ValidationType.TIME; 
		} else if (validationType.equals("TEXT_LENGTH")) {
			vt = DataValidationConstraint.ValidationType.TEXT_LENGTH; 
		} else if (validationType.equals("FORMULA")) {
			vt = DataValidationConstraint.ValidationType.FORMULA; 
		}		
		return vt;
	}
	
	public static WIDGET_COMPONENT validationTypeToComponent(int validationType) {
		WIDGET_COMPONENT widgetComponent = WIDGET_COMPONENT.FREETEXT;
		if (validationType == DataValidationConstraint.ValidationType.ANY) {
			
		} else if (validationType == DataValidationConstraint.ValidationType.INTEGER) {
			widgetComponent = WIDGET_COMPONENT.NUMBER;
		} else if (validationType == DataValidationConstraint.ValidationType.DECIMAL) {
			widgetComponent = WIDGET_COMPONENT.NUMBER;
		} else if (validationType == DataValidationConstraint.ValidationType.LIST) {
			widgetComponent = WIDGET_COMPONENT.DROPDOWN;
		} else if (validationType == DataValidationConstraint.ValidationType.DATE) {
			
		} else if (validationType == DataValidationConstraint.ValidationType.TIME) {
			
		} else if (validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
			widgetComponent = WIDGET_COMPONENT.TEXTAREA;
		} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {
			widgetComponent = WIDGET_COMPONENT.FREETEXT;
		}
		return widgetComponent;
	}

	public static String operatorToString(int operatorType) {
		String operatorName = "";
		if (operatorType == DataValidationConstraint.OperatorType.BETWEEN) {
			operatorName = "BETWEEN";
		} else if (operatorType == DataValidationConstraint.OperatorType.NOT_BETWEEN) {
			operatorName = "NOT_BETWEEN";
		} else if (operatorType == DataValidationConstraint.OperatorType.EQUAL) {
			operatorName = "EQUAL";
		} else if (operatorType == DataValidationConstraint.OperatorType.NOT_EQUAL) {
			operatorName = "NOT_EQUAL";
		} else if (operatorType == DataValidationConstraint.OperatorType.GREATER_THAN) {
			operatorName = "GREATER_THAN";
		} else if (operatorType == DataValidationConstraint.OperatorType.LESS_THAN) {
			operatorName = "LESS_THAN";
		} else if (operatorType == DataValidationConstraint.OperatorType.GREATER_OR_EQUAL) {
			operatorName = "GREATER_OR_EQUAL";
		} else if (operatorType == DataValidationConstraint.OperatorType.LESS_OR_EQUAL) {
			operatorName = "LESS_OR_EQUAL";
		}
		return operatorName;
	}
	
	public static void main(String[] args) {
		String fileLocation = "C:\\Users\\rramirezjimenez\\Documents\\My Received Files\\dropDown.xlsx";
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(fileLocation);
		String sheetName = "Sheet1";
		Sheet sheet = helper.getSheet(sheetName);
		Map<String, Object> dataValidationMap = getDataValidation(sheet);
		Gson gson = GsonUtility.getDefaultGson();
		Map<String, Object> form = UploadUtilities.createForm("test", sheetName, dataValidationMap);
		System.out.println(gson.toJson(form));

	}

}
