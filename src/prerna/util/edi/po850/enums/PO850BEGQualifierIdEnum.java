package prerna.util.edi.po850.enums;

public enum PO850BEGQualifierIdEnum {

	NE("NE", "NEW ORDER"),
	ST("ST", "STANDING ORDER"),
	SA("SA", "STANDALONE PO"),
	BK("BK", "BLANKET PO"),
	;
	
	private String id;
	// this is not to be used, just for human readability
	private String description;
	
	PO850BEGQualifierIdEnum(String id, String description) {
		this.id = id;
		this.description = description;
	}
	
	public String getId() {
		return this.id;
	}
}
