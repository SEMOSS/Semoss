package prerna.sablecc2.reactor.task.lambda.flatmap;

import java.util.List;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.AbstractTaskOperation;

public class FlatMapLambdaTask extends AbstractTaskOperation {

	private IFlatMapLambda lambda;
	
	// need to account for many rows being created 
	// by the lambda
	private List<IHeadersDataRow> lambdaResults;
	private IHeadersDataRow innerRow;
	private int curIndex = 0;
	
	@Override
	public IHeadersDataRow next() {
		if(lambdaResults == null || curIndex >= lambdaResults.size()) {
			// we need to run the inner row through the lambda
			// and start to use those results
			IHeadersDataRow row = this.innerTask.next();
			lambdaResults = lambda.process(row);
			curIndex = 0;
		}
		
		IHeadersDataRow row = lambdaResults.get(curIndex);
		curIndex++;
		return row;
	}
	
	@Override
	public boolean hasNext() {
		// if we still have results to go through for the 
		// previous lambda process
		if(lambdaResults != null && curIndex < lambdaResults.size()) {
			return true;
		}
		
		// reset the index
		curIndex = 0;
		// else we need to see if the inner task
		// has more results
		boolean hasNext = this.innerTask.hasNext();
		if(hasNext) {
			innerRow = this.innerTask.next();
			lambdaResults = lambda.process(innerRow);
			if(lambdaResults == null || lambdaResults.isEmpty()) {
				// this result returned back nothing from the lambda
				// repeat for the next row of the inner task
				return hasNext();
			}
		}
		return hasNext;
	}

	public void setLambda(IFlatMapLambda lambda) {
		this.lambda = lambda;
	}
	
}
