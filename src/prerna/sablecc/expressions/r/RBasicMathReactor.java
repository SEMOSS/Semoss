package prerna.sablecc.expressions.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.sablecc.expressions.r.builder.RMathSelector;

public abstract class RBasicMathReactor extends AbstractRBaseReducer {

	public RExpressionBuilder process(RDataTable frame, RExpressionBuilder builder) {
		IExpressionSelector previousSelector = builder.getLastSelector();
		RMathSelector newSelector = new RMathSelector(previousSelector, this.mathRoutine, this.pkqlMathRoutine);
		builder.replaceSelector(previousSelector, newSelector);
		return builder;
	}
}
