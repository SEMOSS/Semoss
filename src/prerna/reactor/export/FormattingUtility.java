package prerna.reactor.export;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.om.ColorByValueRule;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

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
	public static final String PERCENTAGE = "percentage";
	public static final String DATE = "date";
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
		
		if (Utility.isNullValue(formatted)) {
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
						dontRound = true;
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
											.setScale(round, BigDecimal.ROUND_HALF_EVEN);

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
					
					else if (formatType.equalsIgnoreCase(PERCENTAGE)) {
						// if no custom append symbol is added, use %
						if (append == null || "".equals(append)) {
							append = "%";
						}
						//multiply with 100 to get the % of it
						numericValue = numericValue * 100;
						//rounding the value
						if (round != null) {
							double shift = Math.pow(10, round);
							formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round,
									BigDecimal.ROUND_HALF_EVEN);
						} else {
							formatted = numericValue;
						}
						dontRound = true;// don't round since already rounded
					}
					
				}
				
				// create a BigDecimal object
				// don't round for scientific notation (already rounded)
				if ((round != null) && !dontRound) {
					double shift = Math.pow(10, round);
					if (!Double.isNaN(numericValue)) {
						formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round , BigDecimal.ROUND_HALF_EVEN);
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
			
			if (prepend != null && !prepend.isEmpty()) {
				// prepend the value to beginning of string
				formatted = prepend + formatted;
			}

			if (append != null && !append.isEmpty()) {
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
		// Adding Empty Check as it was breaking the Date export.
		if(metamodelAdditionalDataType != null && !metamodelAdditionalDataType.isEmpty()) {
			// first check if this is a map
    		Map<String, Object> customDataFormat = null;
    		String prepend = null, append = null, delimiter = null, date = null;
        	Integer round = null;
			
    		try {
    			customDataFormat = mapFormatOpts(metamodelAdditionalDataType, dataType);
    			if(customDataFormat != null && !customDataFormat.isEmpty() && customDataFormat.containsKey(ROUND) && !customDataFormat.get(ROUND).equals("")) {
    				round = (Integer) customDataFormat.get(ROUND);
    			}
    			if(customDataFormat == null || customDataFormat.isEmpty()) {
    				customDataFormat = gson.fromJson(metamodelAdditionalDataType, Map.class);
        			if(customDataFormat != null && !customDataFormat.isEmpty()) {
        				metamodelAdditionalDataType = ((String) customDataFormat.get("type")).toLowerCase();
        				if(customDataFormat.containsKey(ROUND) && !customDataFormat.get(ROUND).equals("")) {
        					round  = (int)Math.round((Double) customDataFormat.get(ROUND));
        				}
        			}
    			}
    		} catch(JsonSyntaxException e) {
    			// ignore
    			
    		} catch(Exception e) {
    			e.printStackTrace();
    		}

    		if(customDataFormat != null && !customDataFormat.isEmpty()) {
    			prepend = (String) customDataFormat.get(PREPEND);
    			append = (String) customDataFormat.get(APPEND);
    			delimiter = (String) customDataFormat.get(DELIMITER);
    			
    			// Needed for custom timestamp and dates
    			if(customDataFormat.containsKey(DATE) && !customDataFormat.get(DATE).equals("")) {
    				date = (String) customDataFormat.get(DATE);
    			}
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

					    if(exponitalValue == 0 || exponitalValue == 0.0 ) {
					        throw new IllegalArgumentException("exponitalValue can not be 0");
					      }
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
						dontRound = true;
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
											.setScale(round, BigDecimal.ROUND_HALF_EVEN);

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
					} else if (formatType.equalsIgnoreCase(PERCENTAGE)) {
						// if no custom append symbol is added, use %
						if (append == null || "".equals(append)) {
							append = "%";
						}
						//multiply with 100 to get the % of it
						numericValue = numericValue * 100;
						//rounding the value
						if (round != null) {
							double shift = Math.pow(10, round);
							formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round,
									BigDecimal.ROUND_HALF_EVEN);
						} else {
							formatted = numericValue;
						}
						dontRound = true;// don't round since already rounded
					}
				}
				
				// create a BigDecimal object
				// don't round for scientific notation (already rounded)
				if ((round != null) && !dontRound) {
					double shift = Math.pow(10, round);
					if (!Double.isNaN(numericValue)) {
						formatted = new BigDecimal(Math.round(shift * numericValue) / shift).setScale(round, BigDecimal.ROUND_HALF_EVEN);
					}
				}

				if (delimiter != null && !delimiter.equalsIgnoreCase("Default")) {
					parts = formatted.toString().split("/.");
					parts[0] = betweenDigitsPattern.matcher(parts[0]).replaceAll("" + delimiter);
					formatted = String.join(".", parts);
				}
			} 
			else if (semossDataType == SemossDataType.DATE) {
				// Check if a custom format is there
				if(date != null) {
					formatted = getDate(formatted.toString(), "yyyy-MM-dd", date);
				} else {
					formatted = getDate(formatted.toString(), "yyyy-MM-dd", metamodelAdditionalDataType);
				}
				// the input is a date format itself
				
			} else if (semossDataType == SemossDataType.TIMESTAMP){
				// Check if a custom format is there
				if(date != null) {
					formatted = getDate(formatted.toString(), "yyyy-MM-dd hh:mm:ss", date);
				} else {
					formatted = getDate(formatted.toString(), "yyyy-MM-dd hh:mm:ss", metamodelAdditionalDataType);
				}
				// the input is a date format itself
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

			// Added condition for the restrict 
			boolean restrict=optionsMap.containsKey("restrict")?Boolean.parseBoolean(optionsMap.get("restrict")+""): false;
			//restrict check
			if(restrict && valuesToColorList.size() > 0 ){
				if(!isColoringValid(cbv,rowData,headers)){
					 continue;
				}
			}

			boolean highlightRow = Boolean.parseBoolean(optionsMap.get("highlightRow") + "");
			// check if the color is applied for entire row
			// headers are used here to find the colorOn value index
			boolean isRowColor = highlightRow && valuesToColorList != null && valuesToColorList.size() > 0
					&& isCellEligibleForColor(valuesToColorList, rowData[Arrays.asList(headers).indexOf(colorOn)]);

			Object cell = rowData[colIdx];

			// check if the color is applied for cell
			boolean isCellColor = !highlightRow && valuesToColorList != null && valuesToColorList.size() > 0
					&& Arrays.asList(headers).indexOf(colorOn) == colIdx
					&& isCellEligibleForColor(valuesToColorList, cell);
			
			if (isRowColor || isCellColor) {
				backgroundColor = "background-color: " + optionsMap.get("color");
			}
		}
		return backgroundColor;
	}
	
	/**
	 * @param cellData
	 *            -Cell Data
	 * @param selectedValue-Selected
	 *            column value
	 * @param comparator-comparator
	 * @return true or false if criteria matches
	 */
	public static boolean passedSimpleFilterCheck(Object cellData, Object selectedValue, String comparator) {
		boolean isValid = true;
		 boolean selectedValueType=	selectedValue instanceof Collection;
		 List selectedListValue= selectedValueType? (List)selectedValue: Arrays.asList(selectedValue);
		 
		 if (comparator.equals("==")) {
				isValid =  selectedListValue.indexOf(cellData.toString()) > -1;
			} else if (comparator.equals("!=")) {
				isValid = selectedListValue.indexOf(cellData.toString()) == -1;
			}
		 
		// String type comparator check
			else if (cellData instanceof String && comparator == "?like") {
				isValid = cellData.toString().toLowerCase().contains(selectedValue.toString());

		}
		// Number type and Date type comparator check
		else if ((cellData instanceof Number || cellData instanceof SemossDate) && !selectedValueType) {
			double formattedCellData = NumberUtils.isCreatable(selectedValue.toString())
					? Double.parseDouble(cellData.toString()) : ((SemossDate) cellData).getDate().getTime();
			double formattedSelectedValue = NumberUtils.isCreatable(selectedValue.toString())
					? Double.parseDouble(selectedValue.toString())
					: new SemossDate(selectedValue.toString(), ((SemossDate) cellData).getPattern()).getDate()
							.getTime();

			if (comparator.equals(">")) {
				isValid = formattedCellData > formattedSelectedValue;
			} else if (comparator.equals("<")) {
				isValid = formattedCellData < formattedSelectedValue;
			} else if (comparator.equals(">=")) {
				isValid = formattedCellData >= formattedSelectedValue;
			} else if (comparator.equals("<=")) {
				isValid = formattedCellData <= formattedSelectedValue;
			}

		}
		
		return isValid;
	}
	
	

	/**
	 * @param rule
	 *            ColorbyValue Rule Filter
	 * @param rowData
	 *            -row Data
	 * @param headers-header
	 *            data
	 * @return true or false- if true particular cell or row will be colored
	 */
	public static boolean isColoringValid(ColorByValueRule rule, Object[] rowData, String[] headers) {

		boolean isValid = true;
		List<IQueryFilter> filterVec = rule.getQueryStruct().getExplicitFilters().getFilters();

		for (IQueryFilter iQueryFilter : filterVec) {

			IQueryFilter.QUERY_FILTER_TYPE filterType = iQueryFilter.getQueryFilterType();
			if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				NounMetadata leftComp = ((SimpleQueryFilter) iQueryFilter).getLComparison(); // this is the column, I am not going to bother ?
				NounMetadata rightComp = ((SimpleQueryFilter) iQueryFilter).getRComparison();
				String comparator = ((SimpleQueryFilter) iQueryFilter).getComparator();

				Object cellData = rowData[Arrays.asList(headers).indexOf(leftComp.getValue().toString())];

				isValid= passedSimpleFilterCheck(cellData, rightComp.getValue(), comparator);
			} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {// And Query Filter process

				for (IQueryFilter andQueryFilter : ((AndQueryFilter) iQueryFilter).getFilterList()) {
					NounMetadata leftComp = ((SimpleQueryFilter) andQueryFilter).getLComparison(); // this is the column,I am not going to bother ?																									// 
					NounMetadata rightComp = ((SimpleQueryFilter) andQueryFilter).getRComparison();
					String thisComparator = ((SimpleQueryFilter) andQueryFilter).getComparator();
					Object cellData = rowData[Arrays.asList(headers).indexOf(leftComp.getValue().toString())];

					// if one is false, we will return false.
					if (!passedSimpleFilterCheck(cellData, rightComp.getValue(), thisComparator)) {
						isValid = false;
						break;
					}

				}

			} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {// Or Query Filter process

				for (IQueryFilter orQueryFilter : ((OrQueryFilter) iQueryFilter).getFilterList()) {
					NounMetadata leftComp = ((SimpleQueryFilter) orQueryFilter).getLComparison(); 
					NounMetadata rightComp = ((SimpleQueryFilter) orQueryFilter).getRComparison();
					String thisComparator = ((SimpleQueryFilter) orQueryFilter).getComparator();
					Object cellData = rowData[Arrays.asList(headers).indexOf(leftComp.getValue().toString())];

					// if one is true, we will return true.
					if (passedSimpleFilterCheck(cellData, rightComp.getValue(), thisComparator)) {
						isValid = true;
						break;
					}
				}
			}

		}
		return isValid;

	}
	
	

	/**
	 * @param valuesToColorList - Filtered Rule Result
	 * @param cell - Actual Cell Value
	 * @return - Boolean - Return true if 'valuesToColorList' contains 'cell' Value
	 */
	public static boolean isCellEligibleForColor(List<Object> valuesToColorList, Object cell) {
		return valuesToColorList.stream()
				.anyMatch(valuesColorObj -> ((cell instanceof SemossDate && valuesColorObj instanceof SemossDate)
						? (((SemossDate) valuesColorObj).getDate().getTime() == ((SemossDate) cell).getDate().getTime())
						: valuesColorObj.equals(cell)));
	}
	
	/**
	 * @param metamodelAdditionalDataType
	 * @param dataType
	 * @return - Map - the Mapping has been kept same as of the UI 
	 */
	public static Map<String, Object> mapFormatOpts (String metamodelAdditionalDataType, String dataType) {
		Map<String, Object> format = new HashMap<String, Object>();
		
		String formatString = metamodelAdditionalDataType, delimiter = "", prepend = "", append = "", type = "";
		Integer round = null;
		
		if (dataType.equals("INT") || dataType.equals("DOUBLE") || dataType.equals("NUMBER")) {
	
			switch (formatString) {
                // 1000
                case "int_default": 
                	// same as default
                    round = 0;
                    break;
                // 1,000
                case "int_comma":
                    round = 0;
                    delimiter = ",";
                    break;
                case "int_currency":
                    round = 0;
                    prepend = "$";
                    break;
                case "int_currency_comma":
                    round = 0;
                    delimiter = ",";
                    prepend = "$";
                    break;
                case "int_percent":
                    round = 0;
                    append = "%";
                    break;
                case "thousand" :
                    type = "Thousand";
                    delimiter = ",";
                    break;
                case "million":
                    type = "Million";
                    delimiter = ",";
                    break;
                case "billion":
                    type = "Billion";
                    delimiter = ",";
                    break;
                case "trillion":
                    type = "Trillion";
                    delimiter = ",";
                    break;
                case "accounting":
                    // same as $1,000 but negatives are in parentheses
                    type = "Accounting";
                    delimiter = ",";
                    prepend = "$";
                    round = 2;
                    break;
                case "scientific":
                    type = "Scientific";
                    delimiter = "Default";
                    break;
                case "double_round1":
                    round = 1;
                    break;
                case "double_round2":
                    // same as default
                    round = 2;
                    break;
                case "double_round3":
                    round = 3;
                    break;
                case "double_comma_round1":
                    round = 2;
                    delimiter = ",";
                    break;
                case "double_comma_round2":
                    round = 2;
                    delimiter = ",";
                    break;
                case "double_currency_comma_round2":
                    // same as accounting
                    round = 2;
                    delimiter = ",";
                    prepend = "$";
                    break;
                case "double_percent_round1":
                    round = 1;
                    append = "%";
                    break;
                case "double_percent_round2":
                    round = 2;
                    append = "%";
                    break;
                default:
                    // do nothing;
            }
			
		}
			if (!append.equals("")) {
				format.put(APPEND, append);
			}
			
			if (round != null) {
				format.put(ROUND, round);	
			}
			if (!delimiter.equals("")) {
				format.put(DELIMITER, delimiter);
			}
			if (!prepend.equals("")) {
				format.put(PREPEND, prepend);
			}
			if (!type.equals("")) {
				format.put("TYPE", type);
			}
			
        return format;
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
