package prerna.reactor.json;

import java.lang.reflect.Modifier;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.validator.routines.DateValidator;
import org.apache.commons.validator.routines.DoubleValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.IntegerValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GreedyJsonReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(GreedyJsonReactor.class);

	public static final String ERROR = "ERROR";
	public static final String STAGE = "STAGE";
	public static final String CHILDS = "CHILDS";
	public static final String DATA = "DATA";
	public static final String FULL_MESSAGE = "FULL_MESSAGE";
	public static final String SQL = "SQL";
	public static final String TXN = "TXN";
	
	// gets the top most parent
	// just an optimization so I dont need to go about finding this information
	IReactor superParent = null;
	
	boolean error = false;
	
	public Hashtable inputHash = new Hashtable();
	public Hashtable outputHash = new Hashtable();
	public Hashtable errorHash = new Hashtable();
	
	public List<String> keysToValidate = new Vector<>();
	
	
	// set which fields you think are in what bucket
	
	// these fields should not be null
	protected String [] mandatoryFields = {};
	
	// these fields should be dates
	protected String [] dateFields = {};
	
	// these are email fields
	protected String [] emailFields = {};
	
	// these are number fields
	protected String [] intFields = {};
	
	// these are zipcode Fields
	protected String [] zipFields = {};
	
	// these are npi fields
	protected String [] npiFields = {};
	
	// these are npi fields
	protected String [] ssnFields = {};
	// these are npi fields
	protected String [] phoneFields = {};
	
	// Need a way to keep the output structure
	// may be I keep doing an add level ?
	// everytime I see a new mapentry which is not a new reactor or even if it is
	// I add a new hashtable with that entry and record it there as part of curnoun call
	// which basically means, I also need some way to say set this value somewhere in that hierarchy
	// question is what happens if I have 2 "different" values set up at different levels
	// The only way to resolve this problem is introduce an intermediary reactor which will set the specific value
	// which I think is not a bad idea
	
	// I also need a way to look up stuff i.e. get this to me in parent or child type stuff
	// getFromParent("keyname", boolean levels) / getFromChild("keyname", boolean levels)
	
	// when I set the value of the result
	// I will always set this in the child
	
	// the typical the execute will be the validate
	// and the out will be where you either get the results back or you get the errors back
	// the only problem is out has to directly synchronize with parent nounsttore
	// the only issue is there is another child with the same name
	// how do we account
	// wait I dont need to because it will be an array and will continue to be.. ok
	// need to test this possibility
	
	// When there is an error, the error will have a key which says ERROR to be tracked
	// the error can be true / false
	
	// when error is true
	
	// the errors are tracked at an element level
	// the errors are kept as one of the keys on hashtable
	// the actual error to display will be recorded through 
	// the <item which errored>__ERROR and value - this is the one to be displayed if we so chose
	
	// when error is false
	// the element level gives you the actual value
	
	// either we can stop on the first error or 
	// run through everything.. assimilate the error and give back
	// when the parent is empty, we do the childs recursion to get to it
	// and clean up childs ?
	
	
	@Override
	public NounMetadata execute() {
		process();

		Hashtable daOutput = getOutput();
		return new NounMetadata(daOutput, PixelDataType.MAP);
	}
	
	// This method will be overridden in the case of processor
	// this will execute the sql and then jam the result back in
	public Hashtable getOutput()
	{
		// need to get all the nouns but the noun all and then send it out from there
		if(!store.getNoun(NounStore.all).isEmpty())
		{
			// move it to something else
			GenRowStruct rowStruct = this.store.getNoun(NounStore.all);
			// set it as something else
			if(reactorName == null || reactorName.isEmpty())
				reactorName = DATA;
			this.store.addNoun(reactorName, rowStruct);
		}
		this.store.nounRow.remove(NounStore.all);
		
		// I need to run row by row and turn it into a full data object
		// I created a method in nounstore for this
		// this is now in the store
		
		// also add the childs here
		if(hasProp(CHILDS))
		{
			// need to add the noun to nounstore as well
			NounMetadata nmd = new NounMetadata(getProp(CHILDS), PixelDataType.MAP);
			GenRowStruct grs = new GenRowStruct();
			grs.add(nmd);
			this.store.addNoun(CHILDS, grs);
		}

		// if there are errors plug the errors also ?
		if(errorHash.size() > 0)
		{
			NounMetadata nmd = new NounMetadata(errorHash, PixelDataType.MAP);
			GenRowStruct grs = new GenRowStruct();
			grs.add(nmd);
			this.store.addNoun(ERROR, grs);
		}			
		// need a way to say this is an array 
		// so push everything to array and if so process as array
		return this.store.getDataHash();
	}
	
	@Override
	public Object Out() {
		// do the database stuff
		// synchronize everything with the parent
		// if it got to this point there was no error so we are good at this point
		
		// see if someone has plugged in errors
		
		return this.parentReactor;
	}	
	
	public void process()
	{
		// this will be overridden to validate or process
	}
	
	public void addError(String element, String errorDescription)
	{
		// need to add the elements appropriately here
		errorHash.put(element+ "__"+ ERROR, errorDescription);
		outputHash.put(element, ERROR);
		Vector <String> errorVector = null;
		
		if(outputHash.containsKey(ERROR))
			errorVector = (Vector<String>)outputHash.get(ERROR);
		else
			errorVector = new Vector<>();
		
		errorVector.add(element);
		errorHash.put(ERROR + "_ELEMENTS", errorVector);
		outputHash.put(ERROR, errorVector);
	}

	public void addErrorWithStage(String element, String errorDescription, String stage)
	{
		addError(element, errorDescription);
		errorHash.put(element+ "__" + STAGE, stage);
	}
		
	// this is the recursive piece
	public boolean isError()
	{
		boolean retError = false;
		Vector <Hashtable> dataHashVector = new Vector<>();
		// we can optimize the creation of dataHash but.. 
		dataHashVector.add(this.store.getDataHash());
		
		return hasError(dataHashVector);
	}
	
	// this is doing breadth first.. we could change it to depth first if we so chose to
	public boolean hasError(Vector <Hashtable> remainingHash)
	{
		// all the childs are in the childs key
//		if(hasError)
//			return hasError;
		Vector <Hashtable> nextLevelHash = new Vector<>();
	
		// If I reach until end or if there is an error in between
		for(int nodeIndex = 0; nodeIndex < remainingHash.size();nodeIndex++)
		{
			Hashtable childHash = (Hashtable)remainingHash.get(0);
			// if noError is true and childHash doesn't contain error
			error = error || (childHash.containsKey(ERROR));
			
			// now get all the childs from this one and add it to the next level
			if(childHash.containsKey(CHILDS))
			{
				Vector <String> childs = (Vector<String>)childHash.get(CHILDS);
				for(int childIndex = 0;childIndex < childs.size();childIndex++) {
					if(childHash.get(childs.get(childIndex)) instanceof Hashtable) {
						nextLevelHash.add((Hashtable)childHash.get(childs.get(childIndex)));
					}
				}
			}
		}
		
		// no more to go.. yay !!
		if(!nextLevelHash.isEmpty() && !error)
			hasError(nextLevelHash);
		
		return error;
	}
	
	public Hashtable getCleanOutput()
	{
		boolean hasError = isError();
		Hashtable mainTable = new Hashtable();
		
		if(hasError)
		{
			mainTable = store.getDataHash();
			
			Vector <Hashtable> allHashes = new Vector<>();
			allHashes.add(mainTable);
			
			while(!allHashes.isEmpty())
			{
				Hashtable thisHash = allHashes.remove(0);
				if(thisHash.containsKey(CHILDS))
				{
					Vector <String> childs = (Vector<String>)thisHash.get(CHILDS);
					for(int childIndex = 0;childIndex < childs.size();childIndex++)
					{
						// seems to come 2 times need to see why
						if(thisHash.containsKey(childs.get(childIndex)))
						{
							// need to make a check to see if this is an array
							// if so leave it
							if(thisHash.get(childs.get(childIndex)) instanceof Hashtable)
								allHashes.add((Hashtable)thisHash.get(childs.get(childIndex)));
							else
								logger.debug("Find out what this is is then ?!");
							//thisHash.remove(childs.get(childIndex));
						}
					}
					thisHash.remove(CHILDS);
				}
				thisHash.remove("all");
			}
		}		
		mainTable.put("IS_ERROR", hasError);
		return mainTable;
	}
	
	// the idea here is to take the sql thaat is there
	// print it out
	// in the right location
	public Hashtable getFullOutput()
	{
		boolean hasError = isError();
		Hashtable mainTable = new Hashtable();
		
		if(hasError)
		{
			mainTable = store.getDataHash();
			
			Vector <Hashtable> allHashes = new Vector<>();
			allHashes.add(mainTable);
			
			while(!allHashes.isEmpty())
			{
				Hashtable thisHash = allHashes.remove(0);
				if(thisHash.containsKey(CHILDS))
				{
					Vector <String> childs = (Vector<String>)thisHash.get(CHILDS);
					for(int childIndex = 0;childIndex < childs.size();childIndex++)
					{
						// seems to come 2 times need to see why
						if(thisHash.containsKey(childs.get(childIndex)))
						{
							// need to make a check to see if this is an array
							// if so leave it
							if(thisHash.get(childs.get(childIndex)) instanceof Hashtable)
								allHashes.add((Hashtable)thisHash.get(childs.get(childIndex)));
							else
								logger.debug("Find out what this is is then ?!");
						}
					}
					thisHash.remove(CHILDS);
				}
				thisHash.remove("all");
			}
		}		
		mainTable.put("IS_ERROR", hasError);
		return mainTable;
	}

	
	
	public StringBuffer getJsonOutput()
	{
		
		StringBuffer retOutput = new StringBuffer();
		// right now I am keeping everything
		Gson gson = new GsonBuilder()
		.disableHtmlEscaping()
		.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
		//.registerTypeAdapter(Double.class, new NumberAdaptor())
		.create();
		String propString = gson.toJson(this.store.getDataHash());
		retOutput.append(propString);

		/*
		retOutput.append("{");
		
		Vector<Hashtable> nextLevelHash = new Vector<Hashtable>();
		// I need to do a couple of things here
		// get the data hash
		// remove the childs, all
		// add the childs to the next vector
		// print the key and the value
		for(int nodeIndex = 0;nodeIndex < childVector.size();nodeIndex++)
		{
			Hashtable childHash = childVector.get(nodeIndex);
			childHash.remove("all");
			if(childHash.containsKey(CHILDS))
			{
				Vector <String> childs = (Vector<String>)childHash.get(CHILDS);
				for(int childIndex = 0;childIndex < childs.size();childIndex++)
				{					
					nextLevelHash.add((Hashtable)childHash.get(childs.get(childIndex)));
					childHash.remove(childs.get(childIndex));
				}
			}
			
			// now that I have the pure hashtable print it out
			Gson gson = new GsonBuilder()
			.disableHtmlEscaping()
			.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
			//.registerTypeAdapter(Double.class, new NumberAdaptor())
			.create();
			String propString = gson.toJson(childHash);
			
			// send this for output again
			retOutput.append(propString);
		}
		
		retOutput.append("}");
	*/
		return retOutput;
	}
	
	public List<Object> getData()
	{
		List<Object> retVector  = null;
		if(this.store.getNoun(reactorName) != null)
			retVector = this.store.getNoun(reactorName).getAllValues();
		return retVector;
	}
	
	
	// searchAllparent will telescopically search each of the parent and see if it can get the value
	// the biggest index being the highest parent
	public Object getValue(String key, boolean searchAllParent)
	{
		// if search all parent and getAllValues
		List <IReactor> nextSet = new Vector<>();
		Object retObject = null;
		if(searchAllParent){
			retObject = new Vector<Object>();
			nextSet.add(superParent);
		}
		else 
			nextSet.add(this);
		while(!nextSet.isEmpty())
		{
			IReactor reactor = nextSet.remove(0);
			GenRowStruct output = reactor.getNounStore().getNoun(key);
			List <Object> realValueList = null;
			Object realValue = null;
			if(output != null)
			{
				realValueList = output.getAllValues();
				// find if there is really only one value
				realValue = realValueList;
				
				// if there is only one value just get that value
				if(realValueList.size() == 1)
					realValue = realValueList.get(0);
			}
			
			// if continuing with parent.. add all the child to the next list so we can continue to search
			if(searchAllParent)
			{
				List <IReactor> childList = reactor.getChildReactors();
				if(retObject != null && realValue != null) {
					((Vector)retObject).add(realValue);
				}
				nextSet.addAll(childList);
			}
			else
				retObject = realValue;
		}

		return retObject;
	}
	

	public static Gson gson = new GsonBuilder()
	.disableHtmlEscaping()
	.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
	//.registerTypeAdapter(Double.class, new NumberAdaptor())
	.create();

	public void superParentReactor(IReactor superParent) {
		this.superParent = superParent;
	}
	
	public boolean validateEmail(String email)
	{
		return EmailValidator.getInstance().isValid(email);
	}
	
	public boolean validateNull(String input)
	{
		return (input == null || input.equalsIgnoreCase("null"));
	}

	public boolean validateNumber(String number)
	{
		return IntegerValidator.getInstance().isValid(number);
	}

	public boolean validateNPI(String number)
	{
		return IntegerValidator.getInstance().isValid(number) && number.length() == 10;
	}

	public boolean validateDouble(String input)
	{
		return DoubleValidator.getInstance().isValid(input);
	}
	
	public boolean validateDate(String date)
	{
		// couple of things to do here
		// is this a valid date
		// is this date in the future TBD
		return DateValidator.getInstance().validate(date) != null;
	}
	
	public boolean validateSocial(String social)
	{
		String regex = "^(?!000|666)[0-8][0-9]{2}-(?!00)[0-9]{2}-(?!0000)[0-9]{4}$";
		 
		Pattern pattern = Pattern.compile(regex);
		 
	    Matcher matcher = pattern.matcher(social);
	    return matcher.matches();
	}

	public boolean validatePhone(String phone)
	{
		// accomodates for
		/*
		 * 1234567890 :        true
			123-456-7890 :      true
			123.456.7890 :      true
			123 456 7890 :      true
			(123) 456 7890 :    true
		 */
		String regex = "^\\(?([0-9]{3})\\)?[-.\\s]?([0-9]{3})[-.\\s]?([0-9]{4})$";
		 
		Pattern pattern = Pattern.compile(regex);
		 
	    Matcher matcher = pattern.matcher(phone);
	    return matcher.matches();		
	}
	
	public boolean validateZip(String zipCode)
	{
		String regex = "^[0-9]{5}(?:-[0-9]{4})?$";
		 
		Pattern pattern = Pattern.compile(regex);
		 
	    Matcher matcher = pattern.matcher(zipCode);
	    return matcher.matches();
	}
	
	public void shallowValidate()
	{
		// this goes through each of the fields
		// and adds error if there is one
		Hashtable keyHash = store.getDataHash();
		
		// first are the non null fields
		for(int nullIndex = 0;nullIndex < mandatoryFields.length;nullIndex++)
		{
			if(keyHash.containsKey(mandatoryFields[nullIndex])) // check even if we have something
			{
				if(validateNull((String)keyHash.get(mandatoryFields[nullIndex])))
					addError((mandatoryFields[nullIndex]), "Field is null");
			}
			else
			{
				addError((mandatoryFields[nullIndex]), "Field is null");				
			}
		}
		
		for(int emailIndex = 0;emailIndex < emailFields.length;emailIndex++)
			if(keyHash.containsKey(emailFields[emailIndex])) // check even if we have something
				if(!validateEmail((String)keyHash.get(emailFields[emailIndex])))
					addError((String)keyHash.get(emailFields[emailIndex]), "Not a valid email address");

		for(int intIndex = 0;intIndex < intFields.length;intIndex++)
			if(keyHash.containsKey(intFields[intIndex])) // check even if we have something
				if(!validateNumber((String)keyHash.get(intFields[intIndex])))
					addError((String)keyHash.get(intFields[intIndex]), "Not a valid number");

		for(int zipIndex = 0;zipIndex < zipFields.length;zipIndex++)
			if(keyHash.containsKey(zipFields[zipIndex])) // check even if we have something
				if(!validateNumber((String)keyHash.get(zipFields[zipIndex])))
					addError((String)keyHash.get(zipFields[zipIndex]), "Not a valid zipcode");

		for(int npiIndex = 0;npiIndex < npiFields.length;npiIndex++)
			if(keyHash.containsKey(npiFields[npiIndex])) // check even if we have something
				if(!validateNumber((String)keyHash.get(npiFields[npiIndex])))
					addError((String)keyHash.get(npiFields[npiIndex]), "Not a valid NPI");


		for(int ssnIndex = 0;ssnIndex < ssnFields.length;ssnIndex++)
			if(keyHash.containsKey(ssnFields[ssnIndex])) // check even if we have something
				if(!validateSocial((String)keyHash.get(ssnFields[ssnIndex])))
					addError((String)keyHash.get(ssnFields[ssnIndex]), "Not a valid Social Number");

		for(int phoneIndex = 0;phoneIndex < phoneFields.length;phoneIndex++)
			if(keyHash.containsKey(phoneFields[phoneIndex])) // check even if we have something
				if(!validatePhone((String)keyHash.get(phoneFields[phoneIndex])))
					addError((String)keyHash.get(npiFields[phoneIndex]), "Not a valid phone number");
		
		// we can add other stuff as necessary
		
	}
	
	public static String fillParam2(String query, Map<String, String> paramHash) {
		// NOTE: this process always assumes only one parameter is selected
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		//LOGGER.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while(keys.hasNext()) {
			String key = (String)keys.next();
			String value = paramHash.get(key);
			//LOGGER.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if(!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}

		return query;
	}

}
