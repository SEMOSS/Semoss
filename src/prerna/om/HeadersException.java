/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.om;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import prerna.util.Constants;
import prerna.util.Utility;

public class HeadersException {

	/*
	 * Object to clear the headers and determine any exceptions that are invalid for
	 * loading
	 * 
	 * Its a singleton since we need to read the giant list of values that are saved
	 * in RDF_Map which I do not want to do multiple times
	 */

	// the singleton object
	private static HeadersException singleton;

	// the list of prohibited words read through the RDF_MAP
	// we will store everything in upper case format
	private static Set<String> prohibitedHeaders = new HashSet<String>();

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
			String prohibitedHeadersStr = Utility.getDIHelperProperty(Constants.PROBHIBITED_HEADERS);
			// the string is comma delimited
			String[] words = prohibitedHeadersStr.split(",");
			for (String word : words) {
				// keep everything upper case for simplicity in comparisons
				prohibitedHeaders.add(word.toUpperCase());
			}
		} catch (Exception e) {
			System.err.println("DIHelper is not loaded. THIS SHOULD ONLY BE THE CASE DURING TESTING!");
		}
	}

	// singleton access point
	public static HeadersException getInstance() {
		if (singleton == null) {
			singleton = new HeadersException();
		}
		return singleton;
	}

	public boolean isDuplicated(String checkHeader, String[] allHeaders, int ignoreIndex) {
		checkHeader = checkHeader.toUpperCase();
		for (int colIdx = 0; colIdx < allHeaders.length; colIdx++) {
			if (colIdx == ignoreIndex) {
				continue;
			}

			String currHeaders = allHeaders[colIdx];
			if (currHeaders == null) {
				continue;
			}
			if (checkHeader.equals(currHeaders.toUpperCase())) {
				return true;
			}
		}

		return false;
	}

	public boolean isIllegalHeader(String checkHeader) {
		checkHeader = checkHeader.toUpperCase();
		if (prohibitedHeaders.contains(checkHeader)) {
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
		while (checkHeader.contains("__")) {
			checkHeader = checkHeader.replace("__", "_");
		}

		if (checkHeader.startsWith("_")) {
			checkHeader = checkHeader.substring(1, checkHeader.length());
		}

		if (checkHeader.endsWith("_")) {
			checkHeader = checkHeader.substring(0, checkHeader.length() - 1);
		}

		return checkHeader;
	}

	public boolean isIllegalStartCharacter(String checkHeader) {
		if (checkHeader.length() > 0) {
			char start = checkHeader.charAt(0);
			if (!Character.isLetter(start)) {
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

		// For the following checks just perform a single fix within each block
		// and let the recursion deal with having to fix an issue that is arising
		// due to a previous fix
		// i.e. you made a header no longer illegal but now it is a duplicate,
		// recursion of this method will deal with that

		// sanity check - in case we removed things and made it empty
		if (origHeader == null || origHeader.isEmpty()) {
			origHeader = "UNKNOWN_HEADER";
			isAltered = true;
		}

		// first, clean illegal characters
		if (containsIllegalCharacter(origHeader)) {
			origHeader = removeIllegalCharacters(origHeader);
			isAltered = true;
		}

		// second, check if header is some kind of reserved word
		if (isIllegalHeader(origHeader)) {
			origHeader = appendNumOntoHeader(origHeader);
			isAltered = true;
		}

		// third, check if header starts with a digit
		if (isIllegalStartCharacter(origHeader)) {
			origHeader = appendLetterAtBeginning(origHeader);
			isAltered = true;
		}

		// final, check for duplications
		for (String currHead : currCleanHeaders) {
			if (origHeader.equalsIgnoreCase(currHead)) {
				origHeader = appendNumOntoHeader(origHeader);
				isAltered = true;
				break;
			}
		}

		// if we did alter the string at any point
		// we need to continue and re-run these checks again
		// until we have gone through without altering the string
		// and return the string
		if (isAltered) {
			origHeader = recursivelyFixHeaders(origHeader, currCleanHeaders);
		}

		return origHeader;
	}

	public String recursivelyFixHeaders(String origHeader, String[] currCleanHeaders) {
		boolean isAltered = false;

		// For the following checks just perform a single fix within each block
		// and let the recursion deal with having to fix an issue that is arising
		// due to a previous fix
		// i.e. you made a header no longer illegal but now it is a duplicate,
		// recursion of this method will deal with that

		// sanity check - in case we removed things and made it empty
		if (origHeader == null || origHeader.isEmpty()) {
			origHeader = "UNKNOWN_HEADER";
			isAltered = true;
		}

		// first, clean illegal characters
		if (containsIllegalCharacter(origHeader)) {
			origHeader = removeIllegalCharacters(origHeader);
			isAltered = true;
		}

		// second, check if header is some kind of reserved word
		if (isIllegalHeader(origHeader)) {
			origHeader = appendNumOntoHeader(origHeader);
			isAltered = true;
		}

		// third, check if header starts with a digit
		if (isIllegalStartCharacter(origHeader)) {
			origHeader = appendLetterAtBeginning(origHeader);
			isAltered = true;
		}

		// final, check for duplications
		for (String currHead : currCleanHeaders) {
			if (origHeader.equalsIgnoreCase(currHead)) {
				origHeader = appendNumOntoHeader(origHeader);
				isAltered = true;
				break;
			}
		}

		// if we did alter the string at any point
		// we need to continue and re-run these checks again
		// until we have gone through without altering the string
		// and return the string
		if (isAltered) {
			origHeader = recursivelyFixHeaders(origHeader, currCleanHeaders);
		}

		return origHeader;
	}

	public String appendNumOntoHeader(String origHeader) {
		int num = 0;
		if (origHeader.matches(".*_\\d+")) {
			String strNumbers = origHeader.substring(origHeader.lastIndexOf("_") + 1, origHeader.length());
			num = Integer.parseInt(strNumbers);

			// remove the existing appendage of the number
			origHeader = origHeader.substring(0, origHeader.lastIndexOf("_"));
		}
		origHeader = origHeader + "_" + (++num);

		return origHeader;
	}

	public String[] cleanAndMatchColumnNumbers(String header1, String header2, List<String> otherColumns) {
		if (header1.equalsIgnoreCase(header2)) {
			throw new IllegalArgumentException("Cannot match the header to itself");
		}

		header1 = recursivelyFixHeaders(header1, otherColumns);
		header2 = recursivelyFixHeaders(header2, otherColumns);

		int header1Num = 0;
		int header2Num = 0;
		if (header1.matches(".*_\\d+")) {
			String strNumbers = header1.substring(header1.lastIndexOf("_") + 1, header1.length());
			header1Num = Integer.parseInt(strNumbers);
		}
		if (header2.matches(".*_\\d+")) {
			String strNumbers = header2.substring(header2.lastIndexOf("_") + 1, header2.length());
			header2Num = Integer.parseInt(strNumbers);
		}

		boolean hasAltered = false;
		if (header1Num != header2Num) {
			// we have to do another alteration
			// which requires to perform another check for uniqueness
			hasAltered = true;

			// make them match
			int maxNum = Math.max(header1Num, header2Num);
			if (maxNum == header1Num) {
				// update the header2 to be the larger
				String origHeader2 = header2.substring(0, header2.lastIndexOf("_"));
				header2 = origHeader2 + "_" + maxNum;
			} else {
				// update the header1 to be the larger
				String origHeader1 = header1.substring(0, header1.lastIndexOf("_"));
				header1 = origHeader1 + "_" + maxNum;
			}
		}

		if (hasAltered) {
			// gotta run through the routine again
			return cleanAndMatchColumnNumbers(header1, header2, otherColumns);
		}

		return new String[] { header1, header2 };
	}

	/**
	 * Takes an array of headers and validates each header against itself and
	 * returns the clean new header list.
	 * 
	 * @param headers
	 * @return
	 */
	public String[] getCleanHeaders(String[] headers) {
		int numCols = headers.length;
		List<String> newUniqueHeaders = new ArrayList<String>(numCols);

		for (int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = headers[colIdx];
			// validate header against other clean headers
			String newHeader = recursivelyFixHeaders(origHeader, newUniqueHeaders);
			// add it to the unique headers list so it can be used to validate others
			newUniqueHeaders.add(newHeader);
		}
		return newUniqueHeaders.toArray(new String[newUniqueHeaders.size()]);
	}

}
