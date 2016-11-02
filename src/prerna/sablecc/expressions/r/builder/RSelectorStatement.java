package prerna.sablecc.expressions.r.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.AbstractExpressionSelectorStatement;
import prerna.sablecc.expressions.IExpressionSelector;

public class RSelectorStatement extends AbstractExpressionSelectorStatement{

	/*
	 * Only need to implement the toString method
	 */
	
	@Override
	public String toString() {
		int colId = 0;
		StringBuilder builder = new StringBuilder("{");
		StringBuilder outputNames = new StringBuilder("list(");
		for(IExpressionSelector selector : selectors) {
			IRExpressionSelector rSelector = (IRExpressionSelector) selector;
			String tempName = "V" + colId;
			if(builder.length() == 1) {
				builder.append(tempName).append("=").append(rSelector);
				outputNames.append(rSelector.getName()).append("=").append(tempName);
			} else {
				builder.append(" ; ").append(tempName).append("=").append(rSelector);
				outputNames.append(" , ").append(rSelector.getName()).append("=").append(tempName);
			}
			// update the col id so everything is unique
			colId++;
		}
		outputNames.append(")");
		builder.append(" ; ").append(outputNames.toString()).append(" } ");
		
		return builder.toString();
	}
	
	/**
	 * Get the list of names for the selectors
	 * @return
	 */
	public List<String> getSelectorNames() {
		List<String> selectorNames = new Vector<String>();
		for(IExpressionSelector selector : selectors) {
			IRExpressionSelector rSelector = (IRExpressionSelector) selector;
			selectorNames.add(rSelector.getName());
		}
		return selectorNames;
	}

}
