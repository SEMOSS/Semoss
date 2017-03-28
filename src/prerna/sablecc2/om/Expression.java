package prerna.sablecc2.om;

public class Expression {
	
	private String expression = null;
	private String[] inputs = null;
	
	public Expression(String expression, String [] inputs)
	{
		this.expression = expression;
		this.inputs = inputs;
	}
	
	public String getExpression() {
		return expression;
	}
	
	public String[] getInputs() {
		return inputs;
	}

}
