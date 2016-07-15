package prerna.poi.main;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
	
	private final static String DUP_HEADERS_KEY = "DUPLICATE_HEADERS";
	private final static String ILLEGAL_HEADERS_KEY = "ILLEGAL_HEADERS";

	// the constructor
	// responsible for loading in the prohibited headers
	// requires DIHelper
	private HeadersException() {
		// grab the giant string from 
		String prohibitedHeadersStr = DIHelper.getInstance().getProperty(Constants.PROBHIBITED_HEADERS);
		// the string is semicolon delimited
		String[] words = prohibitedHeadersStr.split(";");
		for(String word : words) {
			// keep everything upper case for simplicity in comparisons
			prohibitedHeaders.add(word.toUpperCase());
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
		errorMessage.append("There are issues with the headers in file = " + Utility.getOriginalFileName(fileName) + ".");
		
		// two tests
		// first one is if we have duplicate headers
		// second one is if we have illegal headers - based on some sql terms i found online...
		
		// for optimization to run through the headers only once, I combined the two tests
		Map<String, Set<String>> comparisons = runAllComparisons(headers);
		
		// duplicate headers will store which headers are duplicated
		Set<String> duplicateHeaders = comparisons.get(DUP_HEADERS_KEY);
		// illegal headers will store which headers are illegal
		Set<String> illegalHeaders = comparisons.get(ILLEGAL_HEADERS_KEY);

		if(!duplicateHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("\n");
			errorMessage.append("Unable to load when there are duplicate names for column headers.\n");
			errorMessage.append("Here are the following column names that are duplicated:\n");
			int dupCounter = 1;
			for(String dupHeader : duplicateHeaders) {
				errorMessage.append(dupCounter + ") " + dupHeader + "\n");
			}
		}
		
		if(!illegalHeaders.isEmpty()) {
			foundError = true;
			errorMessage.append("\n");
			errorMessage.append("Unable to load due to restrictions with column headers.\n");
			errorMessage.append("Here are the following column names that are not acceptable:\n");
			// cause i'm ill son
			int illCounter = 1;
			for(String illHeader : illegalHeaders) {
				errorMessage.append(illCounter + ") " + illHeader + "\n");
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
		}
		
		returnComparisonsMap.put(DUP_HEADERS_KEY, duplicateHeaders);
		returnComparisonsMap.put(ILLEGAL_HEADERS_KEY, illegalHeaders);
		
		return returnComparisonsMap;
	}
	
	public String[] upperCaseAllHeaders(String[] headers) {
		int size = headers.length;
		for(int headIdx = 0; headIdx < size; headIdx++) {
			headers[headIdx] = headers[headIdx].toUpperCase();
		}
		return headers;
	}
	
	/**
	 * Determine the duplicate values in an array
	 * @param headers				The headers to find the duplicates
	 * @return						Set containing the duplicates
	 */
	public Set<String> findDuplicateHeaders(String[] headers) {
		// store duplicate values.. and make it an ordered set
		Set<String> duplicateHeaders = new TreeSet<String>();
		
		// keep a list of the headers current seen
		Set<String> currHeadersProcessed = new HashSet<String>();
		
		int size = headers.length;
		for(int headIdx = 0; headIdx < size; headIdx++) {
			String thisHeader = headers[headIdx];
			if(currHeadersProcessed.contains(thisHeader)) {
				// we found a duplicate value!
				duplicateHeaders.add(thisHeader);
			} else {
				// add it to the set to see if we run into it again
				currHeadersProcessed.add(thisHeader);
			}
		}
		
		return duplicateHeaders;
	}
	
	public Set<String> findIllegalHeaders(String[] headers) {
		// store the illegal headers... make it an ordered set
		Set<String> illegalHeaders = new TreeSet<String>();

		// iterate through and find which headers are illegal
		int size = headers.length;
		for(int headIdx = 0; headIdx < size; headIdx++) {
			String thisHeader = headers[headIdx];
			if(prohibitedHeaders.contains(thisHeader)) {
				// we found an illegal value!
				illegalHeaders.add(thisHeader);
			}
		}
		
		return illegalHeaders;
	}
	
}
