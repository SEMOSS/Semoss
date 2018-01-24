package prerna.date;

import java.text.ParseException;
import java.util.Date;

import com.ibm.icu.text.SimpleDateFormat;
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
		Date date = null;
		SimpleDateFormat formatter = null;
		
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
				formatter = new SimpleDateFormat(this.keyValue.get(this.keysToGet[1]));
			} else {
				formatter = new SimpleDateFormat(DEFAULT_FORMAT);
			}
			
			try {
				date = formatter.parse(strDate);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Could not parse the date " + strDate + " with the format " + formatter.toPattern());
			}
			
		} else {
			// the user hasn't specified a date
			date = Calendar.getInstance().getTime();
		}
		
		return new NounMetadata(date, PixelDataType.CONST_DATE);
	}

}
