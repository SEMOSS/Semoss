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
			String tempName = "V" + colId;
			if(builder.length() == 1) {
				builder.append(tempName).append("=").append(selector);
				outputNames.append(selector.getName()).append("=").append(tempName);
			} else {
				builder.append(" ; ").append(tempName).append("=").append(selector);
				outputNames.append(" , ").append(selector.getName()).append("=").append(tempName);
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
			selectorNames.add(selector.getName());
		}
		return selectorNames;
	}

}
