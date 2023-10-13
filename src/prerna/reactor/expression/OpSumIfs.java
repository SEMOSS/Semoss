package prerna.reactor.expression;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpSumIfs extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {

		int rowSize = this.curRow.size();
		double sumIfsVal = 0;
		boolean isInteger = false;
		boolean isExpression = false;
		String expressionType = null;
		List<Object> sumRangeList = new ArrayList<Object>();
		List<Object> criteriaObjList = new ArrayList<Object>();
		List<Object[]> criteriaRangeObjLists = new ArrayList<Object[]>();
		String criteria = values[2].toString(); // Single criteria
		boolean isValidInput = true;

		if (rowSize >= 3) {
			for (Object obj : (Object[]) values[0]) {
				sumRangeList.add(obj);
			}
			for (int j = 0; j < rowSize; j++) {
				if (rowSize >= 3 && (j > 1) && (j % 2) == 0) {
					criteriaObjList.add(values[j]);
					if (((Object[]) (values[j - 1])).length != ((Object[]) (values[0])).length) {
						isValidInput = false;
						sumIfsVal = 0;
						break;
					} else {
						criteriaRangeObjLists.add((Object[]) (values[j - 1]));
					}
				}
			}
		} else {
			isValidInput = false;
		}
		if (isValidInput) {
			// This list contain indices that satisfy each criteria
			List<Integer> indexMatchList = new ArrayList<Integer>();
			// This list contain matching indices that satisfy each criteria
			List<Integer> indexMatchListFinal = new ArrayList<Integer>();

			// get index list for each criteria
			for (int i = 0; i < criteriaObjList.size(); i++) {
				if (((Object) criteriaObjList.get(i)) != null) {
					if (((Object) criteriaObjList.get(i)) instanceof Integer) {
						isInteger = true;
					} else if (((Object) criteriaObjList.get(i)) instanceof String) {
						isInteger = false;
					}
				}
				criteria = ((Object) criteriaObjList.get(i)).toString();

				if (criteria.contains(">=")) {
					criteria = criteria.replace(">=", "").trim();
					expressionType = ">=";
					isInteger = true;
					isExpression = true;

				} else if (criteria.contains(">")) {
					criteria = criteria.replace(">", "").trim();
					expressionType = ">";
					isInteger = true;
					isExpression = true;

				} else if (criteria.contains("<=")) {
					criteria = criteria.replace("<=", "").trim();
					expressionType = "<=";
					isInteger = true;
					isExpression = true;

				} else if (criteria.contains("<")) {
					criteria = criteria.replace("<", "").trim();
					expressionType = "<";
					isInteger = true;
					isExpression = true;

				} else if (criteria.contains("<>")) {
					criteria = criteria.replace("<>", "").trim();
					expressionType = "<>";
					isInteger = true;
					isExpression = true;

				} else {
					isExpression = false;
				}

				List<Object> criteriaRangeList = new ArrayList<Object>();

				for (Object obj : (Object[]) criteriaRangeObjLists.get(i)) {
					criteriaRangeList.add(obj);
				}
				// Integer criteria
				if (isInteger) {
					indexMatchListFinal = getIndexListForIntegers(criteriaRangeList, criteria.replace("=", " ").trim(),
							indexMatchList, indexMatchListFinal, isExpression, expressionType);
				}
				// String criteria
				if (rowSize >= 3 && !isInteger) {
					indexMatchListFinal = getIndexListForString(criteriaRangeList, criteria.replace("=", " ").trim(),
							indexMatchList, indexMatchListFinal);
				}
			}

			Map<Object, Long> finalIndicesMap = indexMatchListFinal.stream()
					.collect(Collectors.groupingBy(obj -> obj, Collectors.counting()));

			List<Entry<Object, Long>> finalIndicesList = finalIndicesMap.entrySet().stream()
					.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toList());

			// Find maximum occurrence of elements in list that are equal to
			// criteria size.
			for (int i = 0; i < finalIndicesList.size(); i++) {
				if (finalIndicesList.get(i).getValue() == criteriaObjList.size()) {
					sumIfsVal += ((Number) sumRangeList.get(((Number) finalIndicesList.get(i).getKey()).intValue()))
							.doubleValue();
				}
			}
		}
		NounMetadata sumIfsValue = new NounMetadata(sumIfsVal, PixelDataType.CONST_DECIMAL);
		System.out.println("sumIfsValue..." + sumIfsVal);
		return sumIfsValue;
	}

	private List<Integer> getIndexListForIntegers(List<Object> criteriaRangeList, String criteria,
			List<Integer> indexMatchList, List<Integer> indexMatchListFinal, boolean isExpression,
			String expressionType) {

		List<Integer> intArrlist2 = criteriaRangeList.stream()
				.map(object -> (Integer.parseInt(Objects.toString(object, null)))).collect(Collectors.toList());

		for (int j = 0; j < intArrlist2.size(); j++) {
			if (!isExpression && !indexMatchListFinal.isEmpty()
					&& (intArrlist2.get(j).equals(Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			} else if (isExpression) {
				indexMatchList = getIndexListForExpressions(intArrlist2.get(j), Integer.valueOf(criteria),
						indexMatchList, isExpression, expressionType, j);
			}
		}
		indexMatchListFinal = indexMatchList;
		return indexMatchListFinal;
	}

	public List<Integer> getIndexListForString(List<Object> criteriaRangeList, String criteria,
			List<Integer> indexMatchList, List<Integer> indexMatchListFinal) {

		List<String> strArrlist = criteriaRangeList.stream().map(object -> Objects.toString(object, null))
				.collect(Collectors.toList());
		criteria = criteria.replaceAll("\\*", "\\\\w*").replaceAll("\\?", "\\\\w?");
		for (int j = 0; j < strArrlist.size(); j++) {
			if (strArrlist.get(j).matches(criteria) || strArrlist.get(j).equalsIgnoreCase(criteria)) {
				indexMatchList.add(j);
			}
		}
		indexMatchListFinal = indexMatchList;
		return indexMatchListFinal;
	}

	public List<Integer> getIndexListForExpressions(int intArrlist2, int criteria, List<Integer> indexMatchList,
			boolean isExpression, String expressionType, int j) {

		switch (expressionType) {

		case ">":
			if ((intArrlist2 > (Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			}
			break;
		case "<":
			if ((intArrlist2 < (Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			}
			break;
		case ">=":
			if ((intArrlist2 >= (Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			}
			break;
		case "<=":
			if ((intArrlist2 <= (Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			}
			break;
		case "<>":
			if ((intArrlist2 != (Integer.valueOf(criteria)))) {
				indexMatchList.add(j);
			}
			break;
		default:

			break;
		}
		return indexMatchList;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "double";
	}

}
