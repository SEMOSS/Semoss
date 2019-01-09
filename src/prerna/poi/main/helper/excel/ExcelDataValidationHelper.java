package prerna.poi.main.helper.excel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.DataValidationConstraint.ValidationType;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

/**
 * This class gets the data validation constraints from an excel sheet
 */
public class ExcelDataValidationHelper {
	public enum WIDGET_COMPONENT {
		CHECKLIST, DROPDOWN, EXECUTE, FREETEXT, NUMBER, RADIO, SLIDER, TEXTAREA, TYPEAHEAD
	};

	public static Map<String, Object> getDataValidation(Sheet sheet, Map<String, String> newHeaders) {
		Map<String, Object> validationMap = new HashMap<>();
		List<? extends DataValidation> validations = sheet.getDataValidations();
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> newUniqueCleanHeaders = new Vector<String>();
		// check if validations exist
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
				// add header comment as description
				Comment cellComment = c.getCellComment();
				if (cellComment != null) {
					RichTextString commentStr = cellComment.getString();
					String comment = commentStr.getString();
					// comment may have author
					String author = cellComment.getAuthor();
					if (author != null) {
						comment = comment.replace(author + ":\n", "");
					}
					headerMeta.put("description", comment);
				}
				Object header = ExcelParsing.getCell(c);
				cleanHeader = headerChecker.recursivelyFixHeaders(header.toString(), newUniqueCleanHeaders);
				// get new header from user input
				if (newHeaders.containsKey(cleanHeader)) {
					cleanHeader = newHeaders.get(cleanHeader);
				}
				newUniqueCleanHeaders.add(cleanHeader);
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
				if (validationType == DataValidationConstraint.ValidationType.INTEGER) {
					headerMeta.put("type", SemossDataType.INT.toString());
				}
				if (validationType == DataValidationConstraint.ValidationType.DECIMAL) {
					headerMeta.put("type", SemossDataType.DOUBLE.toString());
				}
				if (validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
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

	/**
	 * Create data validation map from excel specific range
	 * 
	 * @param sheet
	 * @param newHeaders
	 * @param headers
	 * @param types
	 * @param headerIndicies
	 * @param startRow
	 * @return
	 */
	public static Map<String, Object> getDataValidation(Sheet sheet, Map<String, String> newHeaders, String[] headers,
			SemossDataType[] types, int[] headerIndicies, int startRow) {
		Map<String, Object> validationMap = new HashMap<>();
		List<? extends DataValidation> validations = sheet.getDataValidations();
		HeadersException headerChecker = HeadersException.getInstance();
		List<String> newUniqueCleanHeaders = new Vector<String>();
		// check if validations exist
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
				int cellRow = cellReference.getRow();
				// this is the case if we are only uploading headers 
				if(cellRow == 0) {
					cellRow = 1;
				}
				Row row = sheet.getRow(cellRow - 1);
				Cell c = row.getCell(cellReference.getCol());
				// add header comment as description
				Comment cellComment = c.getCellComment();
				if (cellComment != null) {
					RichTextString commentStr = cellComment.getString();
					String comment = commentStr.getString();
					// comment may have author
					String author = cellComment.getAuthor();
					if (author != null) {
						comment = comment.replace(author + ":\n", "");
					}
					headerMeta.put("description", comment);
				}
				Object header = ExcelParsing.getCell(c);
				cleanHeader = headerChecker.recursivelyFixHeaders(header.toString(), newUniqueCleanHeaders);
				// get new header from user input
				if (newHeaders.containsKey(cleanHeader)) {
					cleanHeader = newHeaders.get(cleanHeader);
				}
				newUniqueCleanHeaders.add(cleanHeader);
				headerMeta.put("range", address);
			}
			boolean allowEmptyCells = dv.getEmptyCellAllowed();
			int validationType = constraint.getValidationType();
			headerMeta.put("emptyCells", allowEmptyCells);
			headerMeta.put("validationType", validationTypeToString(validationType));
			if (validationType == DataValidationConstraint.ValidationType.ANY) {
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
				if (validationType == DataValidationConstraint.ValidationType.INTEGER) {
				}
				if (validationType == DataValidationConstraint.ValidationType.DECIMAL) {
				}
				if (validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
				}

			} else if (validationType == DataValidationConstraint.ValidationType.LIST) {
				String[] values = constraint.getExplicitListValues();
				headerMeta.put("values", values);
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
			} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {
			}
			if (cleanHeader != null && Arrays.asList(headers).contains(cleanHeader)) {
				int index = Arrays.asList(headers).indexOf(cleanHeader);
				SemossDataType type = types[index];
				if (type != null) {
					headerMeta.put("type", type.toString());
					validationMap.put(cleanHeader, headerMeta);
					headers[index] = null;
					types[index] = null;
				}
			}
		}
		// add remaining missing columns to validationMap
		if (!validationMap.isEmpty()) {
			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				if (header != null) {
					SemossDataType type = types[i];
					Map<String, Object> headerMeta = new HashMap<>();
					if (type == SemossDataType.STRING) {
						headerMeta.put("type", SemossDataType.STRING.toString());
						int validationType = DataValidationConstraint.ValidationType.TEXT_LENGTH;
						headerMeta.put("validationType", validationTypeToString(validationType));
					}
					if (Utility.isNumericType(type.toString())) {
						headerMeta.put("type", SemossDataType.DOUBLE.toString());
						int validationType = DataValidationConstraint.ValidationType.DECIMAL;
						headerMeta.put("validationType", validationTypeToString(validationType));
					}
					// TODO
					headerMeta.put("range", "");
					headerMeta.put("emptyCells", true);
					// add comment
					Row row = sheet.getRow(startRow - 1);
					Cell c = row.getCell(headerIndicies[i] - 1);
					// add header comment as description
					Comment cellComment = c.getCellComment();
					if (cellComment != null) {
						RichTextString commentStr = cellComment.getString();
						String comment = commentStr.getString();
						// comment may have author
						String author = cellComment.getAuthor();
						if (author != null) {
							comment = comment.replace(author + ":\n", "");
						}
						headerMeta.put("description", comment);
					}
					validationMap.put(header, headerMeta);
				}
			}
		}
		return validationMap;
	}
	
	/**
	 * Get description for headers to create form
	 * 
	 * @param sheet
	 * @param newHeaders
	 * @param headers
	 * @param types
	 * @param headerIndicies
	 * @param startRow
	 * @return
	 */
	public static Map<String, Object> getHeaderComments(Sheet sheet, Map<String, String> newHeaders, String[] headers,
			SemossDataType[] types, int[] headerIndicies, int startRow) {
		Map<String, Object> validationMap = new HashMap<>();
		for (int i = 0; i < headers.length; i++) {
			String header = headers[i];
			if (header != null) {
				SemossDataType type = types[i];
				Map<String, Object> headerMeta = new HashMap<>();
				if (type == SemossDataType.STRING) {
					headerMeta.put("type", SemossDataType.STRING.toString());
					int validationType = DataValidationConstraint.ValidationType.TEXT_LENGTH;
					headerMeta.put("validationType", validationTypeToString(validationType));
				}
				if (Utility.isNumericType(type.toString())) {
					headerMeta.put("type", SemossDataType.DOUBLE.toString());
					int validationType = DataValidationConstraint.ValidationType.DECIMAL;
					headerMeta.put("validationType", validationTypeToString(validationType));
				}
				if (Utility.isDateType(type.toString())) {
					headerMeta.put("type",SemossDataType.DATE.toString());
					int validationType = DataValidationConstraint.ValidationType.DATE;
					headerMeta.put("validationType", validationTypeToString(validationType));
				}
				headerMeta.put("range", "");
				headerMeta.put("emptyCells", true);
				// add comment
				Row row = sheet.getRow(startRow - 1);
				Cell c = row.getCell(headerIndicies[i] - 1);
				// add header comment as description
				Comment cellComment = c.getCellComment();
				if (cellComment != null) {
					RichTextString commentStr = cellComment.getString();
					String comment = commentStr.getString();
					// comment may have author
					String author = cellComment.getAuthor();
					if (author != null) {
						comment = comment.replace(author + ":\n", "");
					}
					headerMeta.put("description", comment);
				}
				validationMap.put(header, headerMeta);
			}
		}
		return validationMap;
	}

	/**
	 * Create smss form map from excel data validation
	 * 
	 * @param appId
	 * @param dataValidationMap
	 * @return
	 */
	public static Map<String, Object> createInsertForm(String appId, String sheetName,
			Map<String, Object> dataValidationMap, String[] headerList) {
		Map<String, Object> formMap = new HashMap<>();
		List<String> propertyList = new ArrayList<String>();
		if (headerList != null && headerList.length > 0) {
			for (String header : headerList) {
				propertyList.add(header);
			}
		} else {
			for (String header : dataValidationMap.keySet()) {
				propertyList.add(header);
			}
		}
		// create values and into strings for query
		StringBuilder intoString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			intoString.append(sheetName + "__" + property);
			valuesString.append(" \"<Parameter_" + i + ">\"");
			if (i < propertyList.size() - 1) {
				intoString.append(",");
				valuesString.append(",");
			}
		}
		formMap.put("query", "Database(database=[\"" + appId + "\"]) | Insert (into=[" + intoString + "], values=["
				+ valuesString + "]);");
		// TODO
		formMap.put("label", "");
		formMap.put("description", "");
		// build param list
		List<Map<String, Object>> paramList = new Vector<>();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			Map<String, Object> propMap = (Map<String, Object>) dataValidationMap.get(property);
			// build param for each property
			Map<String, Object> paramMap = new HashMap<>();
			paramMap.put("paramName", "Parameter_" + i);
			String type = (String) propMap.get("type");
			SemossDataType propType = SemossDataType.valueOf(type);
			// build view for param map
			Map<String, Object> viewMap = new HashMap<>();
			viewMap.put("label", property);
			String description = "";
			if (propMap.containsKey("description")) {
				description = (String) propMap.get("description");
			}
			if (propType == SemossDataType.DATE) {
				if (description.length() > 0) {
					description += " Please enter a date (yyyy-mm-dd)";
				} else {
					description = "Please enter a date (yyyy-mm-dd)";
				}
			}
			viewMap.put("description", description);
			// change validation type to display type
			String validationType = (String) propMap.get("validationType");
			int vt = ExcelDataValidationHelper.stringToValidationType(validationType);
			WIDGET_COMPONENT wc = ExcelDataValidationHelper.validationTypeToComponent(vt);
			viewMap.put("displayType", wc.toString().toLowerCase());
			paramMap.put("view", viewMap);
			// build model for param map
			Map<String, Object> modelMap = new HashMap<>();
			modelMap.put("defaultValue", "");
			modelMap.put("defaultOptions", new Vector());
			if (wc == WIDGET_COMPONENT.DROPDOWN) {
				String[] values = (String[]) propMap.get("values");
				modelMap.put("defaultOptions", values);
			} else {
				if (wc != WIDGET_COMPONENT.TEXTAREA) {
					if (propType == SemossDataType.STRING) {
						// if prop type is a string
						modelMap.put("query",
								"(Parameter_" + i + "_infinite = Database( database=[\"" + appId + "\"] )|" + "Select("
										+ sheetName + "__" + property + ").as([" + property + "])|" + "Filter("
										+ sheetName + "__" + property + " ?like \"<Parameter_" + i
										+ "_search>\") | Iterate())| Collect(50);");
						modelMap.put("infiniteQuery", "Parameter_" + i + "_infinite | Collect(50);");
						modelMap.put("searchParam", "Parameter_" + i + "_search");
						// add dependency
						List<String> dependencies = new Vector<>();
						dependencies.add("Parameter_" + i + "_search");
						modelMap.put("dependsOn", dependencies);
						// if prop type is a string build a search param
						Map<String, Object> searchMap = new HashMap<>();
						searchMap.put("paramName", "Parameter_" + i + "_search");
						searchMap.put("view", false);
						Map<String, Object> searchModelMap = new HashMap<>();
						searchModelMap.put("defaultValue", "");
						searchMap.put("model", searchModelMap);
						paramList.add(searchMap);
					}
				}
			}
			if (wc != WIDGET_COMPONENT.TEXTAREA) {
				paramMap.put("model", modelMap);
			}
			paramList.add(paramMap);
		}
		formMap.put("params", paramList);
		formMap.put("execute", "Submit");
		return formMap;
	}

	/**
	 * Create the grid delta map
	 * 
	 * @param appId
	 * @param sheetName
	 * @param dataValidationMap
	 * @param headerList
	 * @return
	 */
	public static Map<String, Object> createUpdateForm(String appId, String sheetName,
			Map<String, Object> dataValidationMap) {
		Map<String, Object> updateMap = new HashMap<>();
		updateMap.put("database", appId);
		updateMap.put("table", sheetName.toUpperCase());
		// config map
		Map<String, Object> configMap = new HashMap<>();
		for (String property : dataValidationMap.keySet()) {
			Map<String, Object> propMap = (Map<String, Object>) dataValidationMap.get(property);
			Map<String, Object> configPropMap = new HashMap<>();
			String propType = (String) propMap.get("type");
			SemossDataType type = SemossDataType.convertStringToDataType(propType);
			boolean readOnly = false;
			configPropMap.put("read-only", readOnly);
			String validationType = (String) propMap.get("validationType");
			if (validationType.equals("LIST")) {
				String[] values = (String[]) propMap.get("values");
				configPropMap.put("seletion-type", "custom");
				configPropMap.put("selections", values);
			} else {
				if (type == SemossDataType.DOUBLE) {
					ArrayList<String> validationList = new ArrayList<>();
					String regex = "^\\d+(\\.\\d*)?$";
					validationList.add(regex);
					configPropMap.put("validation", validationList);
				} else if (type == SemossDataType.INT) {
					ArrayList<String> validationList = new ArrayList<>();
					String regex = "^\\d*$";
					validationList.add(regex);
					configPropMap.put("validation", validationList);
				} else if (type == SemossDataType.STRING) {
//					configPropMap.put("selection-type", "database");
				} else if (type == SemossDataType.DATE) {
					ArrayList<String> validationList = new ArrayList<>();
					String regex = "^\\d{4}-\\d{2}-\\d{2}$";
					validationList.add(regex);
					configPropMap.put("validation", validationList);
				}
			}
			configMap.put(property, configPropMap);
		}
		updateMap.put("config", configMap);
		return updateMap;
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
			widgetComponent = WIDGET_COMPONENT.FREETEXT;
		} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {
			widgetComponent = WIDGET_COMPONENT.FREETEXT;
		}
		return widgetComponent;
	}

	public static SemossDataType widgetComponentToDataType(WIDGET_COMPONENT widgetComponent) {
		SemossDataType dataType = SemossDataType.STRING;
		if (widgetComponent == WIDGET_COMPONENT.CHECKLIST) {
		} else if (widgetComponent == WIDGET_COMPONENT.DROPDOWN) {
		} else if (widgetComponent == WIDGET_COMPONENT.FREETEXT) {
		} else if (widgetComponent == WIDGET_COMPONENT.NUMBER) {
			dataType = SemossDataType.DOUBLE;
		} else if (widgetComponent == WIDGET_COMPONENT.RADIO) {
		} else if (widgetComponent == WIDGET_COMPONENT.SLIDER) {
		} else if (widgetComponent == WIDGET_COMPONENT.TEXTAREA) {
		} else if (widgetComponent == WIDGET_COMPONENT.TYPEAHEAD) {
		}
		return dataType;
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
		String fileLocation = "C:\\Users\\rramirezjimenez\\Desktop\\SweatShirt.xlsx";
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(fileLocation);
		String sheetName = "test";
		Sheet sheet = helper.getSheet(sheetName);
		String[] headers = new String[] { "Date_1" };
		int[] headerInidcies = new int[] { 1 };
		SemossDataType[] types = new SemossDataType[] { SemossDataType.DATE };
		Map<String, Object> dataValidationMap = getDataValidation(sheet, new HashMap<>(), headers, types,
				headerInidcies, 1);
		createUpdateForm("appID", sheetName, dataValidationMap);
		Gson gson = GsonUtility.getDefaultGson();
		Map<String, Object> form = createInsertForm("test", sheetName, dataValidationMap,
				new String[] { "Age", "Gender" });
		System.out.println(gson.toJson(form));

	}



}
