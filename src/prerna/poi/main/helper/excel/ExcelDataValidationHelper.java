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
import prerna.util.upload.FormUtility;

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
					String comment = cleanComment(commentStr.getString());
					headerMeta.put("description", comment);
				}
				Object header = ExcelParsing.getCell(c);
				cleanHeader = headerChecker.recursivelyFixHeaders(header + "", newUniqueCleanHeaders);
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
				if (cellRow == 0) {
					cellRow = 1;
				}
				Row row = sheet.getRow(cellRow - 1);
				Cell c = row.getCell(cellReference.getCol());
				// add header comment as description
				Comment cellComment = c.getCellComment();
				if (cellComment != null) {
					RichTextString commentStr = cellComment.getString();
					String comment = cleanComment(commentStr.getString());
					headerMeta.put("description", comment);
				}
				Object header = ExcelParsing.getCell(c);
				cleanHeader = headerChecker.recursivelyFixHeaders(header + "", newUniqueCleanHeaders);
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
					if (type == SemossDataType.TIMESTAMP) {
						headerMeta.put("type", SemossDataType.TIMESTAMP.toString());
						int validationType = DataValidationConstraint.ValidationType.TIME;
						headerMeta.put("validationType", validationTypeToString(validationType));
					}
					if (type == SemossDataType.DATE) {
						headerMeta.put("type", SemossDataType.DATE.toString());
						int validationType = DataValidationConstraint.ValidationType.DATE;
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
						String comment = cleanComment(commentStr.getString());
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
				} else if (type == SemossDataType.INT || type == SemossDataType.DOUBLE) {
					headerMeta.put("type", SemossDataType.DOUBLE.toString());
					int validationType = DataValidationConstraint.ValidationType.DECIMAL;
					headerMeta.put("validationType", validationTypeToString(validationType));
				} else if (type == SemossDataType.DATE) {
					headerMeta.put("type", SemossDataType.DATE.toString());
					int validationType = DataValidationConstraint.ValidationType.DATE;
					headerMeta.put("validationType", validationTypeToString(validationType));
				} else if (type == SemossDataType.TIMESTAMP) {
					headerMeta.put("type", SemossDataType.TIMESTAMP.toString());
					int validationType = DataValidationConstraint.ValidationType.DATE;
					headerMeta.put("validationType", validationTypeToString(validationType));
				}
				// this is here to ensure that it gets added
				else if (type == SemossDataType.BOOLEAN) {
					headerMeta.put("type", SemossDataType.BOOLEAN.toString());
					int validationType = DataValidationConstraint.ValidationType.TEXT_LENGTH;
					headerMeta.put("validationType", validationTypeToString(validationType));
				}

				headerMeta.put("range", "");
				headerMeta.put("emptyCells", true);
				// add comment
				Row row = sheet.getRow(startRow - 1);
				Cell c = row.getCell(headerIndicies[i] - 1);
				if (c != null) {
					// add header comment as description
					Comment cellComment = c.getCellComment();
					if (cellComment != null) {
						RichTextString commentStr = cellComment.getString();
						String comment = cleanComment(commentStr.getString());
						headerMeta.put("description", comment);
					}
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
		formMap.put("js", new Vector<>());
		formMap.put("css", new Vector<>());

		// grab all the properties
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
		// create values and into strings for insert query
		StringBuilder intoString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			intoString.append(sheetName + "__" + property);
			valuesString.append(" (<" + property + ">)");
			if (i < propertyList.size() - 1) {
				intoString.append(",");
				valuesString.append(",");
			}
		}

		// create insert pixel map
		Map<String, Object> pixelMap = new HashMap<>();
		Map<String, Object> insertMap = new HashMap<>();
		insertMap.put("name", "Insert");
		insertMap.put("pixel", "Database(database=[\"" + appId + "\"]) | Insert (into=[" + intoString + "], values=["
				+ valuesString + "]);");
		pixelMap.put("Insert", insertMap);
		formMap.put("pixel", pixelMap);

		StringBuilder htmlSb = new StringBuilder();
		Map<String, Object> dataMap = new HashMap<>();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			htmlSb.append(FormUtility.getTextComponent(property));
			Map<String, Object> propMap = (Map<String, Object>) dataValidationMap.get(property);
			String type = (String) propMap.get("type");
			SemossDataType propType = SemossDataType.valueOf(type);
			
			// grab description from excel we will add this after
			// adding appropriate input component
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
			
			// build data property map for data binding
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put("defaultValue", "");
			propertyMap.put("options", new Vector());
			propertyMap.put("name", property);
			propertyMap.put("dependsOn", new Vector());
			propertyMap.put("required", true);
			propertyMap.put("autoPopulate", false);
			Map<String, Object> configMap = new HashMap<>();
			configMap.put("table", sheetName);
			Map<String, Object> appMap = new HashMap<>();
			appMap.put("value", appId);
			configMap.put("app", appMap);
			propertyMap.put("config", configMap);
			propertyMap.put("pixel", "");

			// change validation type to display type
			String validationType = (String) propMap.get("validationType");
			int vt = ExcelDataValidationHelper.stringToValidationType(validationType);
			WIDGET_COMPONENT wc = ExcelDataValidationHelper.validationTypeToComponent(vt);
			
			// build html based on input component
			if (wc == WIDGET_COMPONENT.DROPDOWN) {
				String[] values = (String[]) propMap.get("values");
				propertyMap.put("manualOptions", String.join(",", values));
				htmlSb.append(FormUtility.getDropdownComponent(property));
			} else if (wc == WIDGET_COMPONENT.NUMBER) {
				//TODO: min and max ranges for slider
				Object f1 = propMap.get("f1");
				Object f2 = propMap.get("f2");
				propertyMap.put("defaultValue", "0");
				htmlSb.append(FormUtility.getNumberPickerComponent(property));
			} else if (wc == WIDGET_COMPONENT.TEXTAREA) {
				htmlSb.append(FormUtility.getTextAreaComponent(property));
			} else {
				htmlSb.append(FormUtility.getInputComponent(property));
			}

			if (description.length() > 0) {
				htmlSb.append(FormUtility.getDescriptionComponent(description));
			}
			// adding pixel data binding for non-numeric values
			if (propType == SemossDataType.STRING && wc != WIDGET_COMPONENT.DROPDOWN) {
				String pixel = "Database( database=[\"" + appId + "\"] )|" + "Select(" + sheetName + "__" + property
						+ ").as([" + property + "])| Collect(-1);";
				propertyMap.put("pixel", pixel);
			}
			dataMap.put(property, propertyMap);
		}
		htmlSb.append(FormUtility.getSubmitComponent("Insert"));
		formMap.put("html", htmlSb.toString());
		formMap.put("data", dataMap);
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
				configPropMap.put("selection-type", "custom");
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
					// configPropMap.put("selection-type", "database");
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

	public static String cleanComment(String commentToClean) {
		// takes out spaces before the string
		String regex = "^\\s+";
		String trimmedComment = commentToClean;
		if (commentToClean.contains("Comment:")) {
			trimmedComment = commentToClean.substring(commentToClean.indexOf("Comment:") + 8);
			trimmedComment = trimmedComment.replace("\n", "").replace("\r", "");
			trimmedComment = trimmedComment.replaceAll(regex, "");
			trimmedComment = trimmedComment.trim();
		}
		return trimmedComment;
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

//	public static void main(String[] args) {
//		String fileLocation = "C:\\Users\\rramirezjimenez\\Desktop\\SweatShirt.xlsx";
//		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
//		helper.parse(fileLocation);
//		String sheetName = "test";
//		Sheet sheet = helper.getSheet(sheetName);
//		String[] headers = new String[] { "Date_1" };
//		int[] headerInidcies = new int[] { 1 };
//		SemossDataType[] types = new SemossDataType[] { SemossDataType.DATE };
//		Map<String, Object> dataValidationMap = getDataValidation(sheet, new HashMap<>(), headers, types,
//				headerInidcies, 1);
//		createUpdateForm("appID", sheetName, dataValidationMap);
//		Gson gson = GsonUtility.getDefaultGson();
//		Map<String, Object> form = createInsertForm("test", sheetName, dataValidationMap,
//				new String[] { "Age", "Gender" });
//		System.out.println(gson.toJson(form));
//
//	}

}
