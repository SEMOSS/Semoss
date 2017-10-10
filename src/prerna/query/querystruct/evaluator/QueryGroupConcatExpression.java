package prerna.query.querystruct.evaluator;

public class QueryGroupConcatExpression implements IQueryStructExpression {

	private StringBuilder concat = new StringBuilder();
	private boolean first = true;
	@Override
	public void processData(Object obj) {
		if(first) {
			this.concat.append(obj);
			first = false;
		} else {
			this.concat.append(", ").append(obj);
		}
	}
	
	@Override
	public Object getOutput() {
		return this.concat.toString();
	}

}
