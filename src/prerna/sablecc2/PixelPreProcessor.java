package prerna.sablecc2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.util.Utility;

public class PixelPreProcessor {

	/**
	 * Pre-process the pixel to encode values that would not be allowed based on the grammar
	 * @param expression
	 * @return
	 */
	public static String preProcessPixel(String expression, Map<String, String> encodedTextToOriginal) {
		expression = expression.trim();

		Map<String, String> encodeChanges = new HashMap<String, String>();
		
		// we need to be able to save insights
		// which have an embedded encode wtihin the recipe step
		// so to do this, i have added another encode operator
		// which is the master one
		Pattern p = Pattern.compile("<sEncode>.+?</sEncode>", Pattern.DOTALL);
		Matcher m = p.matcher(expression);
		while(m.find()) {
			String originalText = m.group(0);
			String encodedText = originalText.replace("<sEncode>", "").replace("</sEncode>", "");
			encodedText = Utility.encodeURIComponent(encodedText);
			encodeChanges.put(originalText, encodedText);
			// DO NOT DO BELOW BECAUSE IT UPDATES THE MATCHER
			// AND THEN CAN LEAD TO STACK OVERFLOW
//			expression = expression.replace(originalText, encodedText);
		}

		// if there are sEncode blocks
		// we will encode the expression to make a new one now
		for(String originalText : encodeChanges.keySet()) {
			expression = expression.replace(originalText, encodeChanges.get(originalText));
		}
		
		// <encode> </encode>
		String newExpression = encodeExpression(expression, encodedTextToOriginal);
		if(newExpression != null) {
			expression = newExpression;
		}

		return expression;
	}

	private static String encodeExpression(String expression, Map<String, String> encodedTextToOriginal) {
		// find all <encode> positions
		List<Integer> encodeList = new ArrayList<Integer>();
		int curEncodeIndex = expression.indexOf("<encode>");
		while(curEncodeIndex >= 0) {
			encodeList.add(new Integer(curEncodeIndex));
			curEncodeIndex = expression.indexOf("<encode>", curEncodeIndex+1);
		}

		// find all </encode> positions
		List<Integer> slashEncodeList = new ArrayList<Integer>();
		int curSlashIndex = expression.indexOf("</encode>");
		while(curSlashIndex >= 0) {
			slashEncodeList.add(new Integer(curSlashIndex));
			curSlashIndex = expression.indexOf("</encode>", curSlashIndex+1);
		}
		
		List<Integer[]> encodeBlocks = encodeBlock(encodeList, slashEncodeList);
		// if we have an inner encode block
		// we need to remove it from this list
		List<Integer> indexToRemove = new ArrayList<Integer>();
		int totalEncodeBlocks = encodeBlocks.size();
		for(int i = 0; i < totalEncodeBlocks; i++) {
			Integer[] firstEncodeBlock = encodeBlocks.get(i);
			Integer x1 = firstEncodeBlock[0];
			Integer y1 = firstEncodeBlock[1];

			for(int j = 0; j < totalEncodeBlocks; j++) {
				if(i == j) {
					continue;
				}
				// if we already know this block is no good
				// just skip it
				if(indexToRemove.contains(new Integer(j))) {
					continue;
				}
				Integer[] secondEncodeBlock = encodeBlocks.get(j);
				Integer x2 = secondEncodeBlock[0];
				Integer y2 = secondEncodeBlock[1];
				
				if(x2.intValue() > x1.intValue() && y1.intValue() > y2.intValue()) {
					// the secondEncodeBlock is completely within the firstEncodeBlock
					indexToRemove.add(new Integer(j));
				}
			}
		}
		
		// loop through and remove the encapsulated blocks
		if(!indexToRemove.isEmpty()) {
			Collections.sort(indexToRemove);
			for(int i = indexToRemove.size(); i > 0; i--) {
				encodeBlocks.remove(indexToRemove.get(i-1).intValue());
			}
		}
		
		int continueSize = "</encode>".length();
		
		// because the index we actually care about will change
		// as we encode
		// we need to actually keep the list of string 
		List<String> originalStrings = new ArrayList<String>();
		for(Integer[] range : encodeBlocks) {
			originalStrings.add( expression.substring(range[0].intValue(), range[1].intValue()+continueSize));
		}
		
		for(String originalText : originalStrings) {
			String encodedText = originalText.substring("<encode>".length(), originalText.length() - "</encode>".length());
			encodedText = Utility.encodeURIComponent(encodedText);
			encodedTextToOriginal.put(encodedText, originalText);
			expression = expression.replace(originalText, encodedText);
		}
		
		return expression;
	}
	
	/**
	 * Capture all the encode segments
	 * Take into consideration encode blocks within encode blocks
	 * @param encodeList
	 * @param slashEncodeList
	 * @return
	 */
	private static List<Integer[]> encodeBlock(List<Integer> encodeList, List<Integer> slashEncodeList) {
		List<Integer[]> encodes = new ArrayList<Integer[]>();
		int encodePreRunSize = encodeList.size();
		int slashEncodePreRunSize = slashEncodeList.size();
		
		boolean findEncodeBlocks = true;
		while(findEncodeBlocks) {
			Integer[] newEncodeBlock = getNextEncodeBlock(encodeList, slashEncodeList);
			if(newEncodeBlock != null) {
				encodes.add(newEncodeBlock);
			}
			
			// if the size has changed from the preRun, we will continue to find
			// if it hasn't, we are done
			int newEncodeSize = encodeList.size();
			int newSlashEncodeSize = slashEncodeList.size();
			if(newEncodeSize == encodePreRunSize && newSlashEncodeSize == slashEncodePreRunSize) {
				// i guess we are done
				findEncodeBlocks = false;
			} else {
				// we are not done
				// but update the sizes now
				encodePreRunSize = newEncodeSize;
				slashEncodePreRunSize = newSlashEncodeSize;
			}
		}
		
		return encodes;
	}
	
	/**
	 * Method used to logically find the beginning and end of each encode block segment
	 * @param encodeList
	 * @param slashEncodeList
	 * @return
	 */
	private static Integer[] getNextEncodeBlock(List<Integer> encodeList, List<Integer> slashEncodeList) {
		// logic is as follows
		// find the largest encode value
		// find the smallest slash encode value that is larger than the encode value
		// remove these from the list
		
		// thankfully, the 2 lists are already ordered based on how we get the values
		if(encodeList.size() == 0 || slashEncodeList.size() == 0) {
			return null;
		}
		// here, we actually remove the value
		Integer encodeIndexValue = encodeList.remove(encodeList.size()-1);
		// now loop through the slash encode list to get the smallest one that is larger than this value
		Integer slashEncodeIndexValue = null;
		for(int i = 0; i < slashEncodeList.size(); i++) {
			Integer tempSlashEncodeIndexValue = slashEncodeList.get(i);
			if(tempSlashEncodeIndexValue > encodeIndexValue) {
				// we found the one we want!
				slashEncodeIndexValue = tempSlashEncodeIndexValue;
				// and now, remove the value and leave
				slashEncodeList.remove(i);
				break;
			}
		}
		
		// if we didn't find a possible match, return null
		if(encodeIndexValue == null || slashEncodeIndexValue == null) {
			return null;
		}
		
		// return the pair
		return new Integer[]{encodeIndexValue, slashEncodeIndexValue};
	}
	
}
