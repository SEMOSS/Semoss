package prerna.sablecc.expressions.r;

import prerna.ds.R.RDataTable;
import prerna.sablecc.expressions.r.builder.IRExpressionSelector;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.sablecc.expressions.r.builder.RMathSelector;

public abstract class RBasicMathReactor extends AbstractRBaseReducer {

	public RExpressionBuilder process(RDataTable frame, RExpressionBuilder builder) {
		IRExpressionSelector previousSelector = (IRExpressionSelector) builder.getLastSelector();
		RMathSelector newSelector = new RMathSelector(previousSelector, this.mathRoutine, this.pkqlMathRoutine);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
}
