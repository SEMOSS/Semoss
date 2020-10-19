package prerna.query.querystruct;

public class SubselectQueryStruct extends SelectQueryStruct {

	// everything is the same. Just need an additional alias
	String alias = null;

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}
	
}
