package prerna.sablecc2.reactor.export;

public abstract class AbstractFormatter implements Formatter {

	protected String name;
	
	@Override
	public void setIdentifier(String name) {
		this.name = name;
	}
	
	@Override
	public String getIdentifier() {
		return this.name;
	}
}
