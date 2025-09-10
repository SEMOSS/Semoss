package prerna.reactor.playwright;

public record VariableRecord(String label, String text, boolean isPassword) 
{
	
	public VariableRecord(String label, String text) {
		this(label, text, false);
	}
	
}


