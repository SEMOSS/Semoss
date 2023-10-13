package prerna.reactor.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpMatch extends OpBasic {
	
	public OpMatch() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.ARRAY.getKey(), "matchType"};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		int offset = 0;
		if (this.curRow.size() == 3) {
			offset = 1;
		}
		boolean comparingNumbers = false;
		
		boolean isString = this.curRow.getNoun(0).getNounType().equals(PixelDataType.CONST_STRING);
		// figure out
		// if sort needs to do a number sort
		// or do a string sort
		// if we have 1 string -> assumption is all are string values
		// return -1 if match not found
		List<NounMetadata> intGrs = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		
		if (intGrs != null && intGrs.size() > offset) {
			comparingNumbers = true;
		}
		if (!comparingNumbers) {
			List<NounMetadata> doubleGrs = this.curRow.getNounsOfType(PixelDataType.CONST_DECIMAL);
			if (doubleGrs != null && doubleGrs.size() > 0) {
				comparingNumbers = true;
			}
		}

		Object objInput = values[0];
		int index = 1; // default value of index is 1
		if (values.length == 3) {
			index = (int) (values[2]);
		}

		List<Object> objArrList = new ArrayList<Object>();

		for (Object objArr : (Object[]) values[1]) {
			Object val = objArr;
			objArrList.add(val);
		}

		int matchValIndex = -1;

		if (!comparingNumbers && isString) {
			matchValIndex = evaluateStrings(objArrList, matchValIndex, index, objInput);
		} else {
			matchValIndex = evaluateIntegers(objArrList, matchValIndex, index, objInput);
		}

		NounMetadata match = new NounMetadata(matchValIndex, PixelDataType.CONST_INT);
		return match;
	}

	public int evaluateIntegers(List<Object> objArrList, int matchValIndex, int index, Object objInput) {
		List<Integer> intArrlist = objArrList.stream().map(object -> (Integer.parseInt(Objects.toString(object, null))))
				.collect(Collectors.toList());

		NavigableSet<Integer> navSet = new TreeSet<Integer>();

		NavigableSet<Integer> reverseNavSet = new TreeSet<Integer>();

		int inputVal = (int) objInput;

		if (index == 0) {
			final int inputVal1 = inputVal;
			if (intArrlist.contains(inputVal)) {
				Integer firstInputVal = intArrlist.stream().filter(str -> str.equals(inputVal1)).findFirst().get();
				Integer matchVal3 = firstInputVal;
				matchValIndex = intArrlist.indexOf(matchVal3) + 1;
			} else {
				matchValIndex = -1;
			}

		} else if (index == 1) { // Elements should be in ascending order
			Collections.sort(intArrlist);
			navSet.addAll(intArrlist);
			int matchVal1 = navSet.floor(inputVal);
			matchValIndex = intArrlist.indexOf(matchVal1)+1;
		} else if (index == -1) { // Elements should be in descending order
			reverseNavSet.addAll(intArrlist);
			reverseNavSet = reverseNavSet.descendingSet();
			int matchVal2 = reverseNavSet.ceiling(inputVal);
			matchValIndex = intArrlist.indexOf(matchVal2) + 1;
		}
		return matchValIndex;

	}

	public int evaluateStrings(List<Object> objArrList, int matchValIndex, int index, Object objInput) {

		List<String> strArrlist = objArrList.stream().map(object -> Objects.toString(object, null))
				.collect(Collectors.toList());

		String inputVal = objInput.toString();
		inputVal = inputVal.replaceAll("\\*", "\\\\w*").replaceAll("\\?", "\\\\w?");
		boolean regexCheck = false;
		NavigableSet<String> navSet = new TreeSet<String>();
		for (int i = 0; i < objArrList.size(); i++) {
			if (objArrList.get(i).toString().matches(inputVal)) {
				regexCheck = true;
				inputVal = objArrList.get(i).toString();
				break;
			}
		}
		if (regexCheck) {
			if (index == 0) { // Exact match
				final String inputVal1 = inputVal;
				if (strArrlist.contains(inputVal)) {
					String firstInputVal = strArrlist.stream().filter(str -> str.equalsIgnoreCase(inputVal1))
							.findFirst().get();
					String matchVal3 = firstInputVal;
					matchValIndex = strArrlist.indexOf(matchVal3) + 1;
				} else {
					matchValIndex = -1;
				}
			} else if (index == 1) { // Elements should be in ascending order
				Collections.sort(strArrlist);
				navSet.addAll(strArrlist);
				String matchVal1 = navSet.floor(inputVal.toString());
				matchValIndex = strArrlist.indexOf(matchVal1) + 1;

			} else if (index == -1) { // Elements should be in descending order

				NavigableSet<String> reverseNavSet = new TreeSet<String>();
				reverseNavSet.addAll(strArrlist);
				reverseNavSet = navSet.descendingSet();

				String matchVal2 = reverseNavSet.ceiling(inputVal.toString());
				matchValIndex = strArrlist.indexOf(matchVal2) + 1;
			}
		} else {
			matchValIndex = -1;
		}

		return matchValIndex;
	}

	@Override
	public String getReturnType() {
		return "int";
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("matchType")) {
			return "The type of match, either 0 (first matching value), 1 (largest value less than or equal to), or -1 (smallest value greater than or equal to)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
