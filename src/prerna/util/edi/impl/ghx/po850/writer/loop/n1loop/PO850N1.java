package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N1 implements IX12Format {

	private String n101 = ""; // entity identifier code - ST=ShipTo, SE=Selling Party, BT=BillToParty, BY=BuyingParty, VN=Vendor
	private String n102 = ""; // name for n101
	private String n103 = ""; // identification code qualifier - 91=assigned by seller, 92=assigned by buyer
	private String n104 = ""; // identification code for n103
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "N1" 
				+ elementDelimiter + n101
				+ elementDelimiter + n102
				+ elementDelimiter + n103
				+ elementDelimiter + n104
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getN101() {
		return n101;
	}

	public PO850N1 setN101(String n101) {
		this.n101 = n101;
		return this;
	}
	
	public PO850N1 setEntityCode(String n101) {
		return setN101(n101);
	}

	public String getN102() {
		return n102;
	}

	public PO850N1 setN102(String n102) {
		this.n102 = n102;
		return this;
	}
	
	public PO850N1 setName(String n102) {
		return setN102(n102);
	}

	public String getN103() {
		return n103;
	}

	public PO850N1 setN103(String n103) {
		this.n103 = n103;
		return this;
	}
	
	public PO850N1 setIdentificationCodeQualifier(String n103) {
		return setN103(n103);
	}

	public String getN104() {
		return n104;
	}

	public PO850N1 setN104(String n104) {
		this.n104 = n104;
		return this;
	}

	public PO850N1 setIdentificationCode(String n104) {
		return setN104(n104);
	}
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850N1 n1 = new PO850N1()
			.setEntityCode("ST") // 1 - ship to
			.setName("Anchorage VA Medical Center") // 2 - name
			.setIdentificationCodeQualifier("91") // 3 - 91=assigned by seller
			.setIdentificationCode("DEMO-ID")
			;
		
		System.out.println(n1.generateX12("^", "~\n"));
	}
	
}
