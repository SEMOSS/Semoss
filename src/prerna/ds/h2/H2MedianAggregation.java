package prerna.ds.h2;

public class H2MedianAggregation implements org.h2.api.AggregateFunction {

	java.util.LinkedList<Double> values = new java.util.LinkedList<Double>();

	@Override
	public void init(java.sql.Connection cnctn) throws java.sql.SQLException {
		// what do i do for this????
	}

	@Override
	public int getType(int[] ints) throws java.sql.SQLException {
		return java.sql.Types.DOUBLE;
	}

	@Override
	public void add(Object o) throws java.sql.SQLException {
		this.values.add( ((Number)o).doubleValue() );
	}

	@Override
	public Object getResult() throws java.sql.SQLException {
		// Sort list
		java.util.Collections.sort(this.values);

		// Return median
		int size = this.values.size();
		if (size > 0){
			int pos = ((int) size/2);
			// Odd size
			if ((size%2) == 1 ) return this.values.get(pos);
			// Even size
			else return ( this.values.get(pos-1) + this.values.get(pos) ) / 2;
		}
		else {
			return null;
		}
	}
}