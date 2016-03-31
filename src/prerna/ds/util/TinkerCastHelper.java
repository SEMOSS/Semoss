package prerna.ds.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import prerna.util.Utility;

public class TinkerCastHelper {

	private String DBL = "Double";
	private String BOOL = "Boolean";
	private String STR = "String";
	private String DTE = "Date";
	private String INT = "Integer";
	//big decimal?
	//money
	//time
	
	String[] date_formats = {
            //"dd/MM/yyyy",
            "MM/dd/yyyy",
            //"dd-MM-yyyy",
            "yyyy-MM-dd",
            "yyyy/MM/dd", 
            "yyyy MMM dd",
            "yyyy dd MMM",
            "dd MMM yyyy",
            "dd MMM",
            "MMM dd",
            "dd MMM yyyy",
            "MMM yyyy"
            };
	
	public TinkerCastHelper() {
		
	}
	
	public String[] guessTypes(String[][] data) {
		
		return null;
	}
	
	public String[] guessTypes(String[] row) {
		
		String[] types = new String[row.length];
		for(int i = 0; i < row.length; i++) {
			String value = row[i];
			
			if(isDate(value)) {
				types[i] = DTE;
			}
			else if(isDouble(value)) {
				types[i] = DBL;
			}
//			else if(isBoolean(value)) {
//				types[i] = BOOL;
//			}
			else {
				types[i] = STR;
			}
		}
		return types;
	}
	
	public Object[] castToTypes(String[] row, String[] types) {

		Object[] returnRow = new Object[row.length];
		
		for(int i = 0; i < types.length; i++) {
			
			if(types[i].equals(STR)) {
				returnRow[i] = getString(row[i]);
			} 
			
			else if(types[i].equals(DBL)) {
				Double d = getDouble(row[i]);
				if(d!= null) {
					returnRow[i] = d;
				} else {
					returnRow[i] = getString(row[i]);
				}
			} 
			
			else if(types[i].equals(DTE)) {

				Date d = getDate(row[i]);
				if(d!= null) {
					returnRow[i] = d;
				} else {
					returnRow[i] = getString(row[i]);
				}
			} 
			
			else if(types[i].equals(BOOL)) {

				Boolean b = getBoolean(row[i]);
				if(b != null) {
					returnRow[i] = b;
				} else {
					returnRow[i] = getString(row[i]);
				}
			}
		}
		
		return returnRow;
	}
	
	
	private String getString(String value) {
		if(value == null) value = "";
		return Utility.cleanString(value, true, true, false);
	}
	
	private Double getDouble(String value) {
		if(value == null) return null;
		
		Double doub = null;
		try {
			doub = Double.parseDouble(value);
		} catch(NumberFormatException e) {
			
		}
		return doub;
	}
	
	private Date getDate(String value) {
		if(value == null) return null;
		
		Date date = null;
		for(String dateformat : date_formats) {
			try {
				SimpleDateFormat format = new SimpleDateFormat(dateformat);
				date = format.parse(value);
				return date;
			} catch(Exception e) {
			
			}
		}
		return date;
	}
	
	private Boolean getBoolean(String value) {
		if(value == null) return null;
		
		Boolean bool = null;
		try {
			bool = Boolean.parseBoolean(value);
		} catch(Exception e) {
			
		}
		return bool;
	}

	
	private boolean isDate(String value) {
		
		for(String dateformat : date_formats) {
			try {
				SimpleDateFormat format = new SimpleDateFormat(dateformat);
				format.parse(value);
				return true;
			} catch(Exception e) {
			
			}
		}
		return false;
	}
	
	private boolean isDouble(String value) {
		
		try {
			Double.parseDouble(value);
			return true;
		} catch(NumberFormatException e) {
		
		}
		return false;
	}
	
	private boolean isBoolean(String value) {
		try {
			Boolean.parseBoolean(value);
			return true;
		} catch(Exception e) {
		
		}
		return false;
	}
	
	private boolean isInteger(String value) {
		return false;
	}
}
