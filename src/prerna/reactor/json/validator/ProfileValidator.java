package prerna.reactor.json.validator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import org.apache.commons.lang3.math.NumberUtils;

import prerna.reactor.json.GreedyJsonReactor;

public class ProfileValidator extends GreedyJsonReactor {

	private static final String EFFECTIVE_DATE_KEY = "originalEffectiveDate";
	private static final String TERMINATION_DATE_KEY = "terminationDate";
	private static final String NPI_KEY = "npi";
	private static final String EID_KEY = "eid";

	public ProfileValidator() {
		// 1) termination date must be larger than todays date
		// 2) termination date must be larger than effective date
		// 3) eid cannot be null
		// 4) npi should be 10 digits
		this.keysToValidate.add(TERMINATION_DATE_KEY);
		this.keysToValidate.add(EFFECTIVE_DATE_KEY);
		this.keysToValidate.add(NPI_KEY);
	}
	
	@Override
	public void process() {
		System.out.println(this.parentReactor.getClass().getName());
		// testing
		// print out all values
		Hashtable<String, Object> dataHash = this.store.getDataHash();
//		for(String key : dataHash.keySet()) {
//			System.out.println(key + " : " + dataHash.get(key));
//		}
		
		String npi = (String) dataHash.get(NPI_KEY);
		if(npi == null || npi.isEmpty()) {
			addError("NPI", "NPI must be defined");
		} else {
			if(!NumberUtils.isDigits(npi)) {
				addError("NPI", "NPI must only contain numeric values");
			}
			if(npi.length() != 10) {
				addError("NPI", "NPI must contain exactly 10 digits");
			}
		}
		
		String effectiveDateString = (String) dataHash.get(EFFECTIVE_DATE_KEY);
		if(npi == null || npi.isEmpty()) {
			addError("EffectiveDate", "Effective date must be defined");
		}
		String terminationDateString = (String) dataHash.get(TERMINATION_DATE_KEY);
		if(npi == null || npi.isEmpty()) {
			addError("TerminationDate", "Termination date must be defined");
		}
		
		Date effectiveDate = convertStrToDate(effectiveDateString);
		Date terminationDate = convertStrToDate(terminationDateString);

		if(!validDateToToday(effectiveDate)) {
			addError("EffectiveDate", "Effective date is not valid compared to todays date");
		}
		if(!validDateToToday(terminationDate)) {
			addError("TerminationDate", "Termination date is not valid compared to todays date");
		}
		if(!validEffectiveToTermination(effectiveDate, terminationDate)) {
			addError("EffectiveToTermination", "Effective date must be before the termination date");
		}
	
		String eid = (String) dataHash.get(EID_KEY);
		if(eid == null || eid.isEmpty()) {
			addError("EID", "EID must be defined");
		}
	
	}
	
	/**
	 * Convert string date to Date object
	 * @param strDate
	 * @return
	 */
	private Date convertStrToDate(String strDate) {
		String format = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";
		DateFormat formatter = new SimpleDateFormat(format);
		try {
			Date date = formatter.parse(strDate);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean validDateToToday(Date date) {
		if(date == null) {
			return false;
		}
		Calendar calendar = Calendar.getInstance();
		if(date.after(calendar.getTime())) {
			return false;
		}
		return true;
	}
	
	private boolean validEffectiveToTermination(Date effectiveDate, Date terminationDate) {
		if(effectiveDate == null || terminationDate == null) {
			return false;
		}
		return effectiveDate.before(terminationDate);
	}
}
