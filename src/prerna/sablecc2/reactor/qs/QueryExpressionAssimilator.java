package prerna.sablecc2.reactor.qs;

import prerna.query.querystruct.IQuerySelector;
import prerna.query.querystruct.QueryArithmeticSelector;
import prerna.query.querystruct.QueryColumnSelector;
import prerna.query.querystruct.QueryConstantSelector;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class QueryExpressionAssimilator extends QueryStructReactor {

	private String mathExpr;
	
	@Override
	QueryStruct2 createQueryStruct() {
		GenRowStruct qsInputs = this.getCurRow();
		IQuerySelector leftSelector = getSelector(qsInputs.getNoun(0));
		IQuerySelector rightSelector = getSelector(qsInputs.getNoun(1));
		// so i gotta do some fun stuff here
		// lets say you do the following
		// 2 + 5 * 6 / 4
		// since the expression is parsed left to right
		// we will end up creating ( (2+5) * 6 ) / 4;
		// but we actually want 2 + ((5*6) / 4)
		// so i will do some processing based on the type of expression
		// to get the correct math order of operations
		if(this.mathExpr.equals("*") || this.mathExpr.equals("/")) {
			// this is only the case when we have a mult/div operation
			// and the expression must be a plus/minus
			if(leftSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
				QueryArithmeticSelector leftArithSelector = (QueryArithmeticSelector) leftSelector;
				String leftSideMathExpression = leftArithSelector.getMathExpr();
				if(leftSideMathExpression.equals("-") || leftSideMathExpression.equals("+")) {
					// we gotta break this apart to form correctly
					IQuerySelector leftOfLeftSelector = leftArithSelector.getLeftSelector();
					IQuerySelector rightOfLeftSelector = leftArithSelector.getRightSelector();
					
					// we will do a nice switch
					// (leftOfLeftSelector + rightOfLeftSelector) * rightSelector -> leftOfLeftSelector + (rightOfLeftSelector * rightSelector)
					QueryArithmeticSelector newRightSelector = new QueryArithmeticSelector();
					newRightSelector.setLeftSelector(rightOfLeftSelector);
					newRightSelector.setRightSelector(rightSelector);
					newRightSelector.setMathExpr(this.mathExpr);
					
					QueryArithmeticSelector newSelector = new QueryArithmeticSelector();
					newSelector.setLeftSelector(leftOfLeftSelector);
					newSelector.setRightSelector(newRightSelector);
					newSelector.setMathExpr(leftSideMathExpression);
					qs.addSelector(newSelector);
					return qs;
				}
			} else if(rightSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
				QueryArithmeticSelector rightArithSelector = (QueryArithmeticSelector) rightSelector;
				String rightSideMathExpression = rightArithSelector.getMathExpr();
				if(rightSideMathExpression.equals("-") || rightSideMathExpression.equals("+")) {
					// we gotta break this apart to form correctly
					IQuerySelector leftOfRightSelector = rightArithSelector.getLeftSelector();
					IQuerySelector rightOfRightSelector = rightArithSelector.getRightSelector();
					
					// we will do a nice switch
					// leftSelector * (leftOfRightSelector + rightOfRightSelector) -> (leftSelector * leftOfRightSelector) + rightOfRightSelector
					QueryArithmeticSelector newLeftSelector = new QueryArithmeticSelector();
					newLeftSelector.setLeftSelector(leftSelector);
					newLeftSelector.setRightSelector(leftOfRightSelector);
					newLeftSelector.setMathExpr(this.mathExpr);
					
					QueryArithmeticSelector newSelector = new QueryArithmeticSelector();
					newSelector.setLeftSelector(newLeftSelector);
					newSelector.setRightSelector(rightOfRightSelector);
					newSelector.setMathExpr(rightSideMathExpression);
					qs.addSelector(newSelector);
					return qs;
				}
			}
		}

		QueryArithmeticSelector newSelector = new QueryArithmeticSelector();
		newSelector.setLeftSelector(leftSelector);
		newSelector.setRightSelector(rightSelector);
		newSelector.setMathExpr(this.mathExpr);
		qs.addSelector(newSelector);
		return qs;
	}
	
	public IQuerySelector getSelector(NounMetadata input) {
		PixelDataType nounType = input.getNounType();
		if(nounType == PixelDataType.QUERY_STRUCT) {
			// remember, if it is an embedded selector
			// we return a full QueryStruct even if it has just one selector
			// inside of it
			return ((QueryStruct2) input.getValue()).getSelectors().get(0);
		} else if(nounType == PixelDataType.COLUMN) {
			String thisSelector = input.getValue() + "";
			if(thisSelector.contains("__")){
				String[] selectorSplit = thisSelector.split("__");
				return getColumnSelector(selectorSplit[0], selectorSplit[1]);
			}
			else {
				return getColumnSelector(thisSelector, null);
			}
		} else {
			// we have a constant...
			QueryConstantSelector cSelect = new QueryConstantSelector();
			cSelect.setConstant(input.getValue());
			return cSelect;
		}
	}

	public IQuerySelector getColumnSelector(String table, String column) {
		QueryColumnSelector selector = new QueryColumnSelector();
		selector.setTable(table);
		if(column == null) {
			selector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		} else {
			selector.setColumn(column);
		}
		return selector;
	}
	
	public void setMathExpr(String mathExpr) {
		this.mathExpr = mathExpr;
	}
}
