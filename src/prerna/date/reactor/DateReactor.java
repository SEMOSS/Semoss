package prerna.date.reactor;

import java.util.Calendar;

import prerna.date.SemossDate;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactor extends AbstractReactor {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";
	
	public DateReactor() {
		this.keysToGet = new String[]{"date", "format"};
		this.keyRequired = new int[] {0,0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		SemossDate date = null;
		String pattern = DEFAULT_FORMAT;
		
		/*
		 * If there is no date input, then we will grab todays date
		 * If there is a date input, we assume it is yyyy-MM-dd format
		 * If there is a date input and a format, we will use that format
		 */
		
		// determine if we should use the default format
		// or the user defined format
		if(this.keyValue.containsKey(this.keysToGet[1])) {
			pattern = this.keyValue.get(this.keysToGet[1]);
		}
					
		if(this.keyValue.containsKey(this.keysToGet[0])) {
			String strDate = this.keyValue.get(this.keysToGet[0]);
			
			date = new SemossDate(strDate, pattern);
			date.getZonedDateTime();
		} else {
			// the user hasn't specified a date
			date = new SemossDate(Calendar.getInstance().getTime(), pattern);
		}
		
		return new NounMetadata(date, PixelDataType.CONST_DATE);
	}
	
	@Override
	public String getReactorDescription() {
		return "Get todays date or return a date based on a specific date input and format";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if("date".equals(key)) {
			return "A specific date to return. This is a string and assumes a date of yyyy-MM-dd";
		} else if("format".equals(key)) {
			return "A specified format for the date parameter to parse. This should be a Java compliant format";
		}
		return super.getDescriptionForKey(key);
	}

}
