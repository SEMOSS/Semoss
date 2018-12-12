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
		// cannot have __ in the alias
		if (alias != null) {
			if (alias.contains("__")) {
				this.alias = alias.split("__")[1];
			} else {
				this.alias = alias;
			}
		}
	}
	
	@Override
	public String toString() {
		return this.getQueryStructName();
	}
}
