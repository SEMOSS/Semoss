package prerna.query.querystruct.evaluator;

import java.util.ArrayList;
import java.util.List;

import prerna.reactor.expression.OpMedian;

public class QueryMedianExpression implements IQueryStructExpression {

	private List<Double> medians = new ArrayList<Double>();
	
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			this.medians.add( ((Number) obj).doubleValue() );
		}
	}
	
	@Override
	public Object getOutput() {
		int size = this.medians.size();
		double[] dblArray = new double[size];
		for(int i = 0; i < size; i++) {
			dblArray[i] = medians.get(i);
		}
		
		return OpMedian.performComp(dblArray);
	}

}
