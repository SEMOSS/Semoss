package prerna.util.edi.po850.enums;

public enum PO850PO1QualifierIdEnum {

	SK("SK", "STOCK KEEPING UNIT"),
	UP("UP", "UPC CODE"),
	VC("VC", "VENDORS CATALOG NUMBER"),
	VN("VN", "VENDORS ITEM NUMBER"),
	BO("BO", "BUYERS CODE"),
	IN("IN", "BUYERS ITEM NUMBER")
	;
	
	private String id;
	// this is not to be used, just for human readability
	private String description;
	
	PO850PO1QualifierIdEnum(String id, String description) {
		this.id = id;
		this.description = description;
	}
	
	public String getId() {
		return this.id;
	}
}
