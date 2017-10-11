package prerna.query.querystruct.selectors;

public abstract class AbstractQuerySelector implements IQuerySelector {

	protected String alias;
	
	/**
	 * Default constructor
	 */
	public AbstractQuerySelector() {
		// we want the alias to be an empty string
		// since we dont want to get null pointers 
		// when we to the equals when we merge selectors
		this.alias = "";
	}

	@Override
	public void setAlias(String alias) {
		this.alias = alias;
	}
}
