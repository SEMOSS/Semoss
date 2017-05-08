package prerna.sablecc2.reactor.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpSumIf extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {

		int rowSize = this.curRow.size();
		double sumIfVal = 0;
		boolean isInteger = false;
		boolean isExpression = false;
		String expressionType = null;
		boolean isEquals = false; // verify and remove if not needed

		List<Object> rangeList = new ArrayList<Object>();
		List<Object> sumRangeList = new ArrayList<Object>();

		Object criteriaObj = values[1];
		String criteria = values[1].toString();
		if (criteriaObj != null) {
			if (criteriaObj instanceof Integer) {
				isInteger = true;
			} else if (criteriaObj instanceof String) {
				isInteger = false;
			}
		}

		if (criteria.contains(">=")) {
			criteria = criteria.replace(">=", "").trim();
			expressionType = ">=";
			isInteger = true;
			isExpression = true;
			isEquals = true;
		} else if (criteria.contains(">")) {
			criteria = criteria.replace(">", "").trim();
			expressionType = ">";
			isInteger = true;
			isExpression = true;
			isEquals = true;
		} else if (criteria.contains("<=")) {
			criteria = criteria.replace("<=", "").trim();
			expressionType = "<=";
			isInteger = true;
			isExpression = true;
			isEquals = true;
		} else if (criteria.contains("<")) {
			criteria = criteria.replace("<", "").trim();
			expressionType = "<";
			isInteger = true;
			isExpression = true;
			isEquals = true;
		} else if (criteria.contains("<>")) {
			criteria = criteria.replace("<>", "").trim();
			expressionType = "<>";
			isInteger = true;
			isExpression = true;
			isEquals = true;
		} else {
			isExpression = false;
		}

		for (Object obj : (Object[]) values[0]) {
			rangeList.add(obj);
		}
		if (rowSize == 3) {
			for (Object obj : (Object[]) values[2]) {
				sumRangeList.add(obj);
			}
		}
		if (isInteger) {
			sumIfVal = sumOfIntCriteria(rangeList, sumRangeList, criteria, sumIfVal, isExpression, expressionType,
					rowSize);
		}

		if (rowSize == 3 && !isInteger) { // String criteria
			sumIfVal = sumIfStrCriteria(rangeList, sumRangeList, criteria, sumIfVal);
		}

		NounMetadata sumIfValue = new NounMetadata(sumIfVal, PkslDataTypes.CONST_DECIMAL);
		System.out.println("sumIfValue..." + sumIfVal);
		return sumIfValue;
	}

	public double sumIfStrCriteria(List<Object> rangeList, List<Object> sumRangeList, String criteria,
			double sumIfVal) {

		List<String> strArrlist = rangeList.stream().map(object -> Objects.toString(object, null))
				.collect(Collectors.toList());
		List<Integer> intArrlist = sumRangeList.stream()
				.map(object -> (Integer.parseInt(Objects.toString(object, null)))).collect(Collectors.toList());
		criteria = criteria.replaceAll("\\*", "\\\\w*").replaceAll("\\?", "\\\\w?");

		for (int i = 0; i < strArrlist.size(); i++) {
			if (strArrlist.get(i).matches(criteria)) {
				sumIfVal += ((Number) intArrlist.get(i)).doubleValue();
			}
		}
		System.out.println("+++++" + sumIfVal);
		return sumIfVal;
	}

	public double sumOfIntCriteria(List<Object> rangeList, List<Object> sumRangeList, String criteria, double sumIfVal,
			boolean isExpression, String expressionType, int rowSize) {

		if (rowSize == 2) { // integer only
			System.out.println("rangeList=====" + rangeList.size());

			List<Integer> intArrlist = rangeList.stream()
					.map(object -> (Integer.parseInt(Objects.toString(object, null)))).collect(Collectors.toList());
			for (int i = 0; i < intArrlist.size(); i++) {

				if (!isExpression && (intArrlist.get(i).equals(Integer.valueOf(criteria)))) {
					sumIfVal += intArrlist.get(i);
				} else if (isExpression) {
					sumIfVal = expressionEval(expressionType, intArrlist.get(i), Integer.valueOf(criteria), sumIfVal, 0,
							2);
				}
			}
		}

		if (rowSize == 3) { // integer only
			List<Integer> intArrlist = rangeList.stream()
					.map(object -> (Integer.parseInt(Objects.toString(object, null)))).collect(Collectors.toList());

			List<Integer> intArrlist2 = sumRangeList.stream()
					.map(object -> (Integer.parseInt(Objects.toString(object, null)))).collect(Collectors.toList());

			for (int i = 0; i < intArrlist.size(); i++) {
				if (!isExpression && (intArrlist.get(i).equals(Integer.valueOf(criteria)))) {
					sumIfVal += ((Number) intArrlist2.get(i)).doubleValue();
				}else if (isExpression){
					sumIfVal = expressionEval(expressionType, intArrlist.get(i), Integer.valueOf(criteria), sumIfVal, intArrlist2.get(i), 3);
				}
			}
		}
		return sumIfVal;
	}

	public double expressionEval(String expressionType, int intFromList, int intCriteria, double totalSum,
			int intFromList2, int rowSize) {

		switch (expressionType) {

		case ">":
			if ((intFromList > intCriteria)) {
				if (rowSize == 2) {
					totalSum += intFromList;
				} else {
					totalSum += intFromList2;
				}
			}
			break;
		case "<":
			if ((intFromList < intCriteria)) {
				if (rowSize == 2) {
					totalSum += intFromList;
				} else {
					totalSum += intFromList2;
				}
			}
			break;
		case ">=":
			if ((intFromList >= intCriteria)) {
				if (rowSize == 2) {
					totalSum += intFromList;
				} else {
					totalSum += intFromList2;
				}
			}
			break;
		case "<=":
			if ((intFromList <= intCriteria)) {
				if (rowSize == 2) {
					totalSum += intFromList;
				} else {
					totalSum += intFromList2;
				}
			}
			expressionType = ">=";
			break;
		case "<>":
			if ((intFromList != intCriteria)) {
				if (rowSize == 2) {
					totalSum += intFromList;
				} else {
					totalSum += intFromList2;
				}
			}
			expressionType = "<>";
			break;
		default:

			break;
		}
		return totalSum;
	}

}

// verify below code, use it if needed or elsedelete it..(Not needed)
/*
 * if(isExpression){ strArr = criteria.split(">"); for(int i=0 ; i<strArr.length
 * ; i++){ try{ num = Integer.parseInt(strArr[i].trim()); }catch (Exception e) {
 * continue; } }
 * 
 * }
 */
