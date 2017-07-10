package prerna.query.interpreters;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter2 {

	protected int performCount;
	protected QueryStruct2 qs;
	
	@Override
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
		this.performCount = qs.getPerformCount();
	}

	@Override
	public void setPerformCount(int performCount) {
		this.performCount = performCount;
	}

	@Override
	public int isPerformCount() {
		return this.performCount;
	}
}
