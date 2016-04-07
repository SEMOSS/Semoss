package prerna.ds.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import prerna.util.Utility;

public class TinkerCastHelper {

	private String DBL = "DOUBLE";
	private String BOOL = "BOOLEAN";
	private String STR = "VARCHAR";
	private String DTE = "DATE";
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
	
	public Object[] castToTypes(String[] row, String[] types) {

		Object[] returnRow = new Object[row.length];
		
		for(int i = 0; i < types.length; i++) {
			
			if(types[i].contains(STR)) {
				returnRow[i] = getString(row[i]);
			} 
			
			else if(types[i].contains(DBL)) {
				Double d = getDouble(row[i]);
				if(d!= null) {
					returnRow[i] = d;
				} else {
					returnRow[i] = getString(row[i]);
				}
			} 
			
			else if(types[i].contains(DTE)) {

				Date d = getDate(row[i]);
				if(d!= null) {
					returnRow[i] = d;
				} else {
					returnRow[i] = getString(row[i]);
				}
			} 
			
			else if(types[i].contains(BOOL)) {

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
}
