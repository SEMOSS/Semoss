package prerna.sablecc2.reactor.export;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.om.ColorByValueRule;

/*
 * Utility class to process additional tools applied on the data while exporting.
 */
public class FormattingUtility {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	// data format options
	public static final String PREPEND = "prepend";
	public static final String APPEND = "append";
	public static final String ROUND = "round";
	public static final String THOUSAND = "thousand";
	public static final String MILLION = "million";
	public static final String BILLION = "billion";
	public static final String TRILLION = "trillion";
	public static final String ACCOUNTING = "accounting";
	public static final String DELIMITER = "delimiter";
	public static final String BETWEEN_DIGITS = "\\B(?=(\\d{3})+(?!\\d))";

	/**
	 * @param value-
	 *            cell value
	 * @param dataType-
	 *            cell data type
	 * @param panelFormatting
	 *            -panel format data value ex {round=0, delimiter=,,
	 *            type=thousand}
	 * @return it will return the cell value after applying the format
	 */
	public static Object formatDataValues(Object value, String dataType, String metamodelAdditionalDataType, Map<String, String> panelFormatting) {
		Object formatted = value;
		if (Objects.isNull(formatted)) {
			return "null";
		}
		
		boolean dontRound = false;

		// panel formatting goes first
		// then formatting at the metamodel level
		
		if (Objects.nonNull(dataType) && Objects.nonNull(panelFormatting)) {
			String prepend = panelFormatting.get(PREPEND);
			String append = panelFormatting.get(APPEND);
			SemossDataType semossDataType = SemossDataType.convertStringToDataType(dataType + "");
			// strings only replace underscore
			if (semossDataType == SemossDataType.STRING && formatted instanceof String) {
				// in ui (/_/g)
				formatted = formatted.toString().replace("_", " ");
			} else if (semossDataType == SemossDataType.INT || semossDataType == SemossDataType.DOUBLE) {
				String[] parts = null;
				String formatType = panelFormatting.get("type");
				// convert string type numeric
				double numericValue = Double.parseDouble(formatted.toString());
				// getting the round value
				Integer round = panelFormatting.containsKey(ROUND) ? Integer.parseInt(panelFormatting.get(ROUND))
						: null;
				String delimiter = panelFormatting.get(DELIMITER);
				Pattern betweenDigitsPattern = Pattern.compile(BETWEEN_DIGITS);

				if (null != formatType && !"".equalsIgnoreCase(formatType) && !formatType.equalsIgnoreCase("Default")) {

					if (formatType.equalsIgnoreCase(THOUSAND) || formatType.equalsIgnoreCase(MILLION)
							|| formatType.equalsIgnoreCase(BILLION) || formatType.equalsIgnoreCase(TRILLION)) {

						String type = getType(formatType);
						double exponitalValue = exponentialType(formatType);

						if (!Double.isNaN(numericValue)) {
							BigDecimal bigD = new BigDecimal((Math.abs(numericValue) / exponitalValue));
							formatted = bigD.setScale(2, BigDecimal.ROUND_HALF_EVEN).doubleValue();
							// removing the all the zeros after dot if any

							while (formatted.toString().toCharArray()[formatted.toString().length() - 1] == '0') {
								formatted = formatted.toString().substring(0, formatted.toString().length() - 1);
							}

							if (formatted.toString().toCharArray()[formatted.toString().length() - 1] == '.') {
								// removing dot if last index value is dot
								formatted = formatted.toString().substring(0, formatted.toString().length() - 1);
							}
						}
						formatted = formatted.toString() + type;
					}

					else if (formatType.equalsIgnoreCase(ACCOUNTING)) {
						boolean negative = false;
						// if no custom prepend symbol is added, use $
						if (prepend == null || "".equals(prepend)) {
							prepend = "$";
						}

						if (!Double.isNaN(numericValue)) {
							// negatives display $ (500.00)
							if (numericValue < 0) {
								formatted = numericValue * -1;
								negative = true;
							}
							// round before converted to string
							if (round != null) {
								double shift = Math.pow(10, round);
								if (!Double.isNaN(numericValue)) {
									formatted = new BigDecimal(Math.round(shift * numericValue) / shift)
											.setScale(round);

								}
								dontRound = true;
							}

							// add commas
							parts = formatted.toString().split("/.");
							parts[0] = betweenDigitsPattern.matcher(parts[0]).replaceAll(",");
							formatted = String.join(".", parts);
							// zero display
							if (formatted.equals("0")) {
								formatted = " - ";
							}
							if (negative) {
								formatted = "(" + formatted + ")";
							}
						}
					}
				}
				
				// create a BigDecimal object
				// don't round for scientific notation (already rounded)
				if ((round != null) && !dontRound) {
					double shift = Math.pow(10, round);
					if (!Double.isNaN(numericValue)) {
						formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round);
					}
				}

				if (delimiter != null && !delimiter.equalsIgnoreCase("Default")) {
					parts = formatted.toString().split("/.");
					parts[0] = betweenDigitsPattern.matcher(parts[0]).replaceAll("" + delimiter);
					formatted = String.join(".", parts);
				}
			}
			else if (semossDataType == SemossDataType.DATE || semossDataType == SemossDataType.TIMESTAMP) {

				String targetDateFormat = panelFormatting.get("dateType");
				if (targetDateFormat != null && !targetDateFormat.equals("Default")) {
					if (formatted instanceof SemossDate) {
						SemossDate dateObj = (SemossDate) formatted;
						formatted = getDate(formatted.toString(), dateObj.getPattern(), targetDateFormat);
					}
				}
			}
			
			if (prepend != null && !prepend.equals("") && !prepend.equals(0)) {
				// prepend the value to beginning of string
				formatted = prepend + formatted;
			}

			if (append != null && !prepend.equals("0")) {
				// append the value to the end
				formatted = formatted + append;
			}
			
			// now return if we have something
			// this way we know that we are applying panel level 
			// as opposed to the metamodel level
			if(formatted != value) {
				return formatted;
			}
		}
		
		// now we will try to do the metamodel level
		if(metamodelAdditionalDataType != null) {
			// first check if this is a map
    		Map<String, String> customDataFormat = null;
    		try {
    			customDataFormat = gson.fromJson(metamodelAdditionalDataType, Map.class);
    			if(customDataFormat != null && !customDataFormat.isEmpty()) {
    				metamodelAdditionalDataType = customDataFormat.get("type").toLowerCase();
    			}
    		} catch(JsonSyntaxException e) {
    			// ignore
    		} catch(Exception e) {
    			e.printStackTrace();
    		}

        	String prepend = null;
        	String append = null;
        	Integer round = null;
			String delimiter = null;

    		if(customDataFormat != null) {
    			prepend = customDataFormat.get(PREPEND);
    			append = customDataFormat.get(APPEND);
        		round = customDataFormat.containsKey(ROUND) ? Integer.parseInt(customDataFormat.get(ROUND))
    					: null;
    			delimiter = customDataFormat.get(DELIMITER);
    		}
    		
    		SemossDataType semossDataType = SemossDataType.convertStringToDataType(dataType + "");
			// strings only replace underscore
			if (semossDataType == SemossDataType.STRING && formatted instanceof String) {
				// in ui (/_/g)
				formatted = formatted.toString().replace("_", " ");
			} else if (semossDataType == SemossDataType.INT || semossDataType == SemossDataType.DOUBLE) {
				String[] parts = null;
				String formatType = metamodelAdditionalDataType;
				// convert string type numeric
				double numericValue = Double.parseDouble(formatted.toString());
				// getting the round value
				
				Pattern betweenDigitsPattern = Pattern.compile(BETWEEN_DIGITS);
				if (null != formatType && !"".equalsIgnoreCase(formatType) && !formatType.equalsIgnoreCase("Default")) {

					if (formatType.equalsIgnoreCase(THOUSAND) || formatType.equalsIgnoreCase(MILLION)
							|| formatType.equalsIgnoreCase(BILLION) || formatType.equalsIgnoreCase(TRILLION)) {

						String type = getType(formatType);
						double exponitalValue = exponentialType(formatType);

						if (!Double.isNaN(numericValue)) {
							BigDecimal bigD = new BigDecimal((Math.abs(numericValue) / exponitalValue));
							formatted = bigD.setScale(2, BigDecimal.ROUND_HALF_EVEN).doubleValue();
							// removing the all the zeros after dot if any

							while (formatted.toString().toCharArray()[formatted.toString().length() - 1] == '0') {
								formatted = formatted.toString().substring(0, formatted.toString().length() - 1);
							}

							if (formatted.toString().toCharArray()[formatted.toString().length() - 1] == '.') {
								// removing dot if last index value is dot
								formatted = formatted.toString().substring(0, formatted.toString().length() - 1);
							}
						}
						formatted = formatted.toString() + type;
					}

					else if (formatType.equalsIgnoreCase(ACCOUNTING)) {
						boolean negative = false;
						// if no custom prepend symbol is added, use $
						if (prepend == null || "".equals(prepend)) {
							prepend = "$";
						}

						if (!Double.isNaN(numericValue)) {
							// negatives display $ (500.00)
							if (numericValue < 0) {
								formatted = numericValue * -1;
								negative = true;
							}
							// round before converted to string
							if (round != null) {
								double shift = Math.pow(10, round);
								if (!Double.isNaN(numericValue)) {
									formatted = new BigDecimal(Math.round(shift * numericValue) / shift)
											.setScale(round);

								}
								dontRound = true;
							}

							// add commas
							parts = formatted.toString().split("/.");
							parts[0] = betweenDigitsPattern.matcher(parts[0]).replaceAll(",");
							formatted = String.join(".", parts);
							// zero display
							if (formatted.equals("0")) {
								formatted = " - ";
							}
							if (negative) {
								formatted = "(" + formatted + ")";
							}
						}
					}
				}
				
				// create a BigDecimal object
				// don't round for scientific notation (already rounded)
				if ((round != null) && !dontRound) {
					double shift = Math.pow(10, round);
					if (!Double.isNaN(numericValue)) {
						formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round);
					}
				}

				if (delimiter != null && !delimiter.equalsIgnoreCase("Default")) {
					parts = formatted.toString().split("/.");
					parts[0] = betweenDigitsPattern.matcher(parts[0]).replaceAll("" + delimiter);
					formatted = String.join(".", parts);
				}
			} 
			else if (semossDataType == SemossDataType.DATE) {
				// the input is a date format itself
				formatted = getDate(formatted.toString(), "yyyy-MM-dd", metamodelAdditionalDataType);
			} else if (semossDataType == SemossDataType.TIMESTAMP){
				// the input is a date format itself
				formatted = getDate(formatted.toString(), "yyyy-MM-dd hh:mm:ss", metamodelAdditionalDataType);
			}

    		// apply prepend/append
    		if (prepend != null && !prepend.isEmpty()) {
    			formatted = prepend + formatted;
    		} 
    		if (append != null && !append.isEmpty()) {
    			formatted = formatted + append;
    		}

    		return formatted;
		}
		
		
		return formatted;
	}

	/**
	 * @param type-
	 *            format type
	 * @return it will return suffix value based on format type
	 */
	private static String getType(String type) {
		switch (type) {
		case THOUSAND:
			return "k";
		case MILLION:
			return "M";
		case BILLION:
			return "B";
		case TRILLION:
			return "T";
		default:
			return "";
		}
	}

	/**
	 * @param type
	 * @return <double>-exponential value this will return exponential value for
	 *         given format type
	 */
	private static double exponentialType(String type) {
		switch (type) {
		case THOUSAND:
			return 1.0e+3;
		case MILLION:
			return 1.0e+6;
		case BILLION:
			return 1.0e+9;
		case TRILLION:
			return 1.0e+12;
		default:
			return 0;
		}
	}

	/**
	 * @param value
	 *            - cell date value
	 * @param currFormat
	 *            -current date format
	 * @param targetFormat-
	 *            target date format
	 * @return converts cell value from current date format to target date
	 *         format
	 */
	public static String getDate(Object value, String currFormat, String targetFormat) {
		// current simple format from current date format
		SimpleDateFormat sdf = new SimpleDateFormat(currFormat);
		Date mydate = null;
		try {
			mydate = sdf.parse(value.toString());
		} catch (ParseException e) {
			// ignore
			e.printStackTrace();
		}
		// target SimpleDateformat from selected format
		SimpleDateFormat outdate = new SimpleDateFormat(targetFormat);
		// date conversion
		return outdate.format(mydate).toString();
	}

	
	/** 
	 * @param colorRuleMap -contains the colorbyvalue rule object as key and valuesToColor as value
	 * 			key- {comparator={display=is Equal To,value===},color=#4FA4DE,colorOn=Director} valuesColumn=Genre},
	 * 			value-[Adam_McKay, Adam_Shankman, Alexander_Payne, Alfonso_Cuarón,etc] 
	 * @param headers-header data 
	 * @param rowData-row data frame
	 * @param colIdx- index of the  cell index with respect to columns
	 * @return This method returns the cell background color
	 */
	public static Object getBackgroundColor(Map<ColorByValueRule, List<Object>> colorRuleMap, String[] headers,
			Object[] rowData, int colIdx) {
		
		Object backgroundColor = "";

		for (Entry<ColorByValueRule, List<Object>> cbvRuleValues : colorRuleMap.entrySet()) {
			ColorByValueRule cbv = cbvRuleValues.getKey();
			Map<String, Object> optionsMap = cbv.getOptions();
			List<Object> valuesToColorList = cbvRuleValues.getValue();
			Object colorOn = optionsMap.get("colorOn");

			boolean highlightRow = Boolean.parseBoolean(optionsMap.get("highlightRow") + "");
			// check if the color is applied for entire row
			// headers are used here to find the colorOn value index
			boolean isRowColor = highlightRow && valuesToColorList != null && valuesToColorList.size() > 0
					&& valuesToColorList.indexOf(rowData[Arrays.asList(headers).indexOf(colorOn)]) > -1;

			Object cell = rowData[colIdx];
			// check if the color is applied for cell
			boolean isCellColor = !highlightRow && Arrays.asList(headers).indexOf(colorOn) == colIdx
					&& valuesToColorList.indexOf(cell) > -1;

			if (isRowColor || isCellColor) {
				backgroundColor = "background-color: " + optionsMap.get("color");
			}
		}
		return backgroundColor;
	}

	/*
	 * Main method for testing
	 * 
	 * public static void main(String args[]) {
	 * 
	 * String value = "0.1523234"; String text =
	 * value.replace("\\B(?=(\\d{3})+(?!\\d))/g", ",");
	 * System.out.println("text::" + text);
	 * 
	 * }
	 */

}
