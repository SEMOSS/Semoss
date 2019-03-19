package prerna.poi.main;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class HeadersException {

	/*
	 * Object to clear the headers and determine any exceptions that are invalid for loading
	 * 
	 * Its a singleton since we need to read the giant list of values that are saved in RDF_Map which 
	 * I do not want to do multiple times
	 */
	
	// the singleton object
	private static HeadersException singleton;
	
	// the list of prohibited words read through the RDF_MAP
	// we will store everything in upper case format
	private static Set<String> prohibitedHeaders = new HashSet<String>();;
	
	public final static String DUP_HEADERS_KEY = "DUPLICATE_HEADERS";
	public final static String ILLEGAL_HEADERS_KEY = "ILLEGAL_HEADERS";
	public final static String ILLEGAL_CHARACTER_KEY = "ILLEGAL_CHARACTER_KEY";
	public final static String ILLEGAL_START_CHARACTER_KEY = "ILLEGAL_START_CHARACTER_KEY";

	// the constructor
	// responsible for loading in the prohibited headers
	// requires DIHelper
	private HeadersException() {
		// grab the giant string from helper
		try {
			String prohibitedHeadersStr = DIHelper.getInstance().getProperty(Constants.PROBHIBITED_HEADERS);
			// the string is semicolon delimited
			String[] words = prohibitedHeadersStr.split(",");
			for(String word : words) {
				// keep everything upper case for simplicity in comparisons
				prohibitedHeaders.add(word.toUpperCase());
			}
		} catch(Exception e) {
			System.err.println("DIHelper is not loaded. THIS SHOUDL ONLY BE THE CASE DURING TESTING!");
		}
	}
	
	// singleton access point
	public static HeadersException getInstance() {
		if(singleton == null) {
			singleton = new HeadersException();
		}
		return singleton;
	}
	
	/**
	 * This will compare the headers from the file and report all issues to the user if present
	 * @param headers				The String[] containing the headers to test
	 * @return						boolean true/false if headers are good
	 * 								if headers are not good, it just throws an exception at the end
	 * 								which needs to be caught and sent to the FE
	 * @throws IOException 
	 */
	public boolean compareHeaders(String fileName, String[] headers) throws IOException {
		headers = upperCaseAllHeaders(headers);
		// instantiate errorMessage objects
		boolean foundError = false;
		StringBuilder errorMessage = new StringBuilder();
		errorMessage.append("FILE ERROR : " + Utility.getOriginalFileName(fileName) + "<br>");
		
		// two tests
		// first one is if we have duplicate headers
		// second one is if we have illegal headers - based on some sql terms i found online...
		
		// for optimization to run through the headers only once, I combined the two tests
		Map<String, Set<String>> comparisons = runAllComparisons(headers);
		
		// duplicate headers will store which headers are duplicated
		Set<String> duplicateHeaders = comparisons.get(DUP_HEADERS_KEY);
		// illegal headers will store which headers are illegal
		Set<String> illegalHeaders = comparisons.get(ILLEGAL_HEADERS_KEY);
		// illegal characters will store headers which have any of the following: %+;@
		Set<String> illCharacterHeaders = comparisons.get(ILLEGAL_CHARACTER_KEY);
		// illegal start characters will store headers which do not start with a digit
		Set<String> illegalStartHeaders = comparisons.get(ILLEGAL_START_CHARACTER_KEY);
		
		if(!duplicateHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("<br>");
			errorMessage.append("ERROR - Duplicate Column Names:<br>");
			int dupCounter = 1;
			for(String dupHeader : duplicateHeaders) {
				errorMessage.append(dupCounter + ") " + dupHeader + "<br>");
				dupCounter++;
			}
		}
		
		if(!illegalHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("<br>");
			errorMessage.append("ERROR - Prohibited Column Names:<br>");
			// cause i'm ill son
			int illCounter = 1;
			for(String illHeader : illegalHeaders) {
				errorMessage.append(illCounter + ") " + illHeader + "<br>");
				illCounter++;
			}
		}
		
		if(!illCharacterHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("<br>");
			errorMessage.append("ERROR - Column name can't contain any of the following characters: %+;@ <br>");
			// cause i'm ill son
			int illCounter = 1;
			for(String illHeader : illCharacterHeaders) {
				errorMessage.append(illCounter + ") " + illHeader + "<br>");
				illCounter++;
			}
		}
		
		if(!illegalStartHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("<br>");
			errorMessage.append("ERROR - Column name must start with a letter<br>");
			// cause i'm ill son
			int illCounter = 1;
			for(String illHeader : illCharacterHeaders) {
				errorMessage.append(illCounter + ") " + illHeader + "<br>");
				illCounter++;
			}
		}
		
		if(foundError) {
			throw new IOException(errorMessage.toString());
		} 
		
		return true;
	}
	
	public Map<String, Set<String>> runAllComparisons(String[] headers) {
		// this method is just a combination of finding duplicate headers
		// and finding illegal headers
		
		Map<String, Set<String>> returnComparisonsMap = new Hashtable<String, Set<String>>();
		
		// this is a bit messier in terms of implementation, but it only requires
		// us to go through the data once, instead of iterating once to find duplicate
		// headers and another time to find illegal headers
		
		// store duplicate values.. and make it an ordered set
		Set<String> duplicateHeaders = new TreeSet<String>();
		// store the illegal headers... make it an ordered set
		Set<String> illegalHeaders = new TreeSet<String>();
		// keep a list of the headers current seen
		Set<String> currHeadersProcessed = new HashSet<String>();
		// store the illegal headers... make it an ordered set
		Set<String> illConcatHeaders = new TreeSet<String>();
		// store the headers that start with non-letters... make it an ordered set
		Set<String> illealStartHeaders = new TreeSet<String>();
		
		int size = headers.length;
		for(int headIdx = 0; headIdx < size; headIdx++) {
			String thisHeader = headers[headIdx];
			
			// THIS IS THE PORTION OF CODE FOR DUPLICATE HEADERS
			if(currHeadersProcessed.contains(thisHeader)) {
				// we found a duplicate value!
				duplicateHeaders.add(thisHeader);
			} else {
				// add it to the set to see if we run into it again
				currHeadersProcessed.add(thisHeader);
			}
			// END DUPLICATE HEADERS

			// THIS IS THE PORTION OF CODE FOR ILLEGAL HEADERS
			if(prohibitedHeaders.contains(thisHeader)) {
				// we found an illegal value!
				illegalHeaders.add(thisHeader);
			}
			// END ILLEGAL HEADERS
			
			// THIS IS THE PORTION OF CODE FOR ILLEGAL CHARACTERS
			if(containsIllegalCharacter(thisHeader)) {
				// we found an illegal value!
				illConcatHeaders.add(thisHeader);
			}
			// END ILLEGAL CONCATENATIONS
			
			if(isIllegalStartCharacter(thisHeader)) {
				illealStartHeaders.add(thisHeader);
			}
		}
		
		returnComparisonsMap.put(DUP_HEADERS_KEY, duplicateHeaders);
		returnComparisonsMap.put(ILLEGAL_HEADERS_KEY, illegalHeaders);
		returnComparisonsMap.put(ILLEGAL_CHARACTER_KEY, illConcatHeaders);
		returnComparisonsMap.put(ILLEGAL_START_CHARACTER_KEY, illealStartHeaders);

		return returnComparisonsMap;
	}
	
	public String[] upperCaseAllHeaders(String[] headers) {
		int size = headers.length;
		for(int headIdx = 0; headIdx < size; headIdx++) {
			headers[headIdx] = headers[headIdx].toUpperCase();
		}
		return headers;
	}
	
	public boolean isDuplicated(String checkHeader, String[] allHeaders) {
		checkHeader = checkHeader.toUpperCase();
		for(String currHeaders : allHeaders) {
			if(currHeaders == null) {
				continue;
			}
			if(checkHeader.equals(currHeaders.toUpperCase())) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean isDuplicated(String checkHeader, String[] allHeaders, int ignoreIndex) {
		checkHeader = checkHeader.toUpperCase();
		for(int colIdx = 0; colIdx < allHeaders.length; colIdx++) {
			if(colIdx == ignoreIndex) {
				continue;
			}
			
			String currHeaders = allHeaders[colIdx];
			if(currHeaders == null) {
				continue;
			}
			if(checkHeader.equals(currHeaders.toUpperCase())) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean isIllegalHeader(String checkHeader) {
		checkHeader = checkHeader.toUpperCase();
		if(prohibitedHeaders.contains(checkHeader)) {
			return true;
		}
		return false;
	}
	
	public boolean containsIllegalCharacter(String checkHeader) {
		// match any character not alpha, numeric, or underscore AND
		// match 2 or more consecutive underscores AND
		// match if starts with underscore AND
		// match if ends with underscore
		Pattern p = Pattern.compile("[^a-zA-Z0-9-_]|_{2,}|^_|_$|-");
		boolean hasIllegalChar = p.matcher(checkHeader).find();
		return hasIllegalChar;
	}
	
	public String removeIllegalCharacters(String checkHeader) {
		checkHeader = checkHeader.trim();
		checkHeader = checkHeader.replace("+", "");
		checkHeader = checkHeader.replace("@", "");
		checkHeader = checkHeader.replace("%", "");
		checkHeader = checkHeader.replace(";", "");
		checkHeader = checkHeader.replaceAll("[^a-zA-Z0-9]", "_");

		// need to replace 2 "__" with a single "_"
		while(checkHeader.contains("__")) {
			checkHeader = checkHeader.replace("__", "_");
		}
		
		if(checkHeader.startsWith("_")) {
			checkHeader = checkHeader.substring(1, checkHeader.length());
		}
		
		if(checkHeader.endsWith("_")) {
			checkHeader = checkHeader.substring(0, checkHeader.length()-1);
		}
		
		return checkHeader;
	}
	
	public boolean isIllegalStartCharacter(String checkHeader) {
		if(checkHeader.length() > 0) {
			char start = checkHeader.charAt(0);
			if(!Character.isLetter(start)) {
				return true;
			}
		}
		return false;
	}
	
	public String appendLetterAtBeginning(String origHeader) {
		return "A" + origHeader;
	}
	
	public String recursivelyFixHeaders(String origHeader, List<String> currCleanHeaders) {
		boolean isAltered = false;
		
		/*
		 * For the following 3 checks
		 * Just perform a single fix within each block
		 * And let the recursion deal with having to fix an issue that is arising
		 * due to a previous fix
		 * i.e. you made a header no longer illegal but now it is a duplicate, recurssion of
		 * this method will deal with that
		 */
		
		// first, clean illegal characters
		if(containsIllegalCharacter(origHeader)) {
			origHeader = removeIllegalCharacters(origHeader);
			isAltered = true;
		}
		
		// second, check if header is some kind of reserved word
		if(isIllegalHeader(origHeader)) {
			origHeader = appendNumOntoHeader(origHeader);
			isAltered = true;
		}
		
		// third, check if header starts with a digit
		if(isIllegalStartCharacter(origHeader)) {
			origHeader = appendLetterAtBeginning(origHeader);
			isAltered = true;
		}
		
		// final, check for duplications
		for(String currHead : currCleanHeaders) {
			if(origHeader.equalsIgnoreCase(currHead)) {
				origHeader = appendNumOntoHeader(origHeader);
				isAltered = true;
				break;
			}
		}
		
		// if we did alter the string at any point
		// we need to continue and re-run these checks again
		// until we have gone through without altering the string
		// and return the string
		if(isAltered) {
			origHeader = recursivelyFixHeaders(origHeader, currCleanHeaders);
		}
		
		return origHeader;
	}
	
	public String recursivelyFixHeaders(String origHeader, String[] currCleanHeaders) {
		boolean isAltered = false;
		
		/*
		 * For the following 3 checks
		 * Just perform a single fix within each block
		 * And let the recursion deal with having to fix an issue that is arising
		 * due to a previous fix
		 * i.e. you made a header no longer illegal but now it is a duplicate, recurssion of
		 * this method will deal with that
		 */
		
		// first, clean illegal characters
		if(containsIllegalCharacter(origHeader)) {
			origHeader = removeIllegalCharacters(origHeader);
			isAltered = true;
		}
		
		// second, check if header is some kind of reserved word
		if(isIllegalHeader(origHeader)) {
			origHeader = appendNumOntoHeader(origHeader);
			isAltered = true;
		}
		
		// third, check if header starts with a digit
		if(isIllegalStartCharacter(origHeader)) {
			origHeader = appendLetterAtBeginning(origHeader);
			isAltered = true;
		}
				
		// final, check for duplications
		for(String currHead : currCleanHeaders) {
			if(origHeader.equalsIgnoreCase(currHead)) {
				origHeader = appendNumOntoHeader(origHeader);
				isAltered = true;
				break;
			}
		}
		
		// if we did alter the string at any point
		// we need to continue and re-run these checks again
		// until we have gone through without altering the string
		// and return the string
		if(isAltered) {
			origHeader = recursivelyFixHeaders(origHeader, currCleanHeaders);
		}
		
		return origHeader;
	}
	
	public String appendNumOntoHeader(String origHeader) {
		int num = 0;
		if(origHeader.matches(".*_\\d+")) {
			String strNumbers = origHeader.substring(origHeader.lastIndexOf("_") + 1, origHeader.length());
			num = Integer.parseInt(strNumbers);
			
			// remove the existing appendage of the number
			origHeader = origHeader.substring(0, origHeader.lastIndexOf("_"));
		}
		origHeader = origHeader  + "_" + (++num);
		
		return origHeader;
	}
	
	public String[] cleanAndMatchColumnNumbers(String header1, String header2, List<String> otherColumns) {
		if(header1.equalsIgnoreCase(header2)) {
			throw new IllegalArgumentException("Cannot match the header to itself");
		}
		
		header1 = recursivelyFixHeaders(header1, otherColumns);
		header2 = recursivelyFixHeaders(header2, otherColumns);
		
		int header1Num = 0;
		int header2Num = 0;
		if(header1.matches(".*_\\d+")) {
			String strNumbers = header1.substring(header1.lastIndexOf("_") + 1, header1.length());
			header1Num = Integer.parseInt(strNumbers);
		}
		if(header2.matches(".*_\\d+")) {
			String strNumbers = header2.substring(header2.lastIndexOf("_") + 1, header2.length());
			header2Num = Integer.parseInt(strNumbers);
		}
		
		boolean hasAltered = false;
		if(header1Num != header2Num) {
			// we have to do another alteration
			// which requires to perform another check for uniqueness
			hasAltered = true;

			// make them match
			int maxNum = Math.max(header1Num, header2Num);
			if(maxNum == header1Num) {
				// update the header2 to be the larger
				String origHeader2 = header2.substring(0, header2.lastIndexOf("_"));
				header2 = origHeader2 + "_" + maxNum;
			} else {
				// update the header1 to be the larger
				String origHeader1 = header1.substring(0, header1.lastIndexOf("_"));
				header1 = origHeader1 + "_" + maxNum;
			}
		}
		
		if(hasAltered) {
			// gotta run through the routine again
			return cleanAndMatchColumnNumbers(header1, header2, otherColumns);
		}
		
		return new String[]{header1, header2};
	}
	
    /**
    * Takes an array of headers and validates each header against itself
    * and returns the clean new header list.
    * 
     * @param headers
    * @return
    */
    public String[] getCleanHeaders(String[] headers) {        
          int numCols = headers.length; 
          List<String> newUniqueHeaders = new Vector<String>(numCols);

          for(int colIdx = 0; colIdx < numCols; colIdx++) {
                 String origHeader = headers[colIdx];
                 // validate header against other clean headers
                 String newHeader = recursivelyFixHeaders(origHeader, newUniqueHeaders);
                 // add it to the unique headers list so it can be used to validate others
                 newUniqueHeaders.add(newHeader);
          }            
          return newUniqueHeaders.toArray(new String[] {} );
    }


}
