package prerna.util.edi.impl.ghx.po850.writer.loop.po1loop;

public class PO850PO1QualifierAndVal {

	public String qualifier;
	public String value;
	
	PO850PO1QualifierAndVal(String qualifier, String value) {
		this.qualifier = qualifier;
		this.value = value;
	}

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
