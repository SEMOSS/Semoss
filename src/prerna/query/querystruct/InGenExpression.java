package prerna.query.querystruct;

import java.util.ArrayList;
import java.util.List;

public class InGenExpression extends GenExpression {
	
	// false = in
	// true = not in
	private boolean isNot = false;
	
	// sets the condition
	public List <GenExpression> inList = new ArrayList<GenExpression>();
	
	public void setIsNot(boolean negate) {
		this.isNot = negate;
	}
	
	public boolean isNot() {
		return this.isNot;
	}
}
