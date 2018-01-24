package prerna.date;

import com.ibm.icu.util.Calendar;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;

public class DateReactor extends AbstractReactor {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";
	
	public DateReactor() {
		this.keysToGet = new String[]{"date", "format"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		SemossDate date = null;
		String pattern = DEFAULT_FORMAT;
		
		/*
		 * If there is no date input, then we will grab todays date
		 * If there is a date input, we assume it is yyy-MM-dd format
		 * If there is a date input and a format, we will use that format
		 */
		
		if(this.keyValue.containsKey(this.keysToGet[0])) {
			String strDate = this.keyValue.get(this.keysToGet[0]);
			
			// determine if we should use the default format
			// or the user defined format
			if(this.keyValue.containsKey(this.keysToGet[1])) {
				pattern = this.keyValue.get(this.keysToGet[1]);
			}
			date = new SemossDate(strDate, pattern);
			date.getDate();
		} else {
			// the user hasn't specified a date
			date = new SemossDate(Calendar.getInstance().getTime());
		}
		
		return new NounMetadata(date, PixelDataType.CONST_DATE);
	}

}
