package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SemossDate {

	private String dateVal;
	private String pattern;

	private transient Date date;
	
	/*
	 * This is just a basic wrapper around a date
	 * so we can store the pattern with the date
	 */
	
	public SemossDate(Date date) {
		this(date, "yyyy-MM-dd");
	}
	
	public SemossDate(Date date, String pattern) {
		this.date = date;
		this.pattern = pattern;
		getFormattedDate();
	}
	
	public SemossDate(String dateVal, String pattern) {
		this.dateVal = dateVal;
		this.pattern = pattern;
		getDate();
	}
	
	public String getPattern() {
		return this.pattern;
	}

	/**
	 * Get the string version of the date
	 * @return
	 */
	public String getFormattedDate() {
		if(this.dateVal == null) {
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			this.dateVal = formatter.format(this.date);
		}
		return this.dateVal;
	}
	
	/**
	 * Get the formal date object
	 * @return
	 */
	public Date getDate() {
		if(this.date == null) {
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			try {
				this.date = formatter.parse(this.dateVal);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Could not parse the date " + this.dateVal + " with the format " + formatter.toPattern());
			}
		}
		return this.date;
	}
	
	@Override
	public String toString() {
		return getFormattedDate();
	}
	
	/**
	 * Using this to test the string value + pattern
	 * @return
	 */
	public String testToString() {
		return this.dateVal + " ::: " + this.pattern;
	}
	
}
