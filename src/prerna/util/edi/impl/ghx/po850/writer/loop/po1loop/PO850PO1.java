package prerna.util.edi.impl.ghx.po850.writer.loop.po1loop;

import prerna.util.edi.IX12Format;

public class PO850PO1 implements IX12Format {

	private String po101 = ""; // unique id within loop
	private String po102 = ""; // quantity ordered
	private String po103 = ""; // unit of basis for measurement CA=case, EA=each, etc.
	private String po104 = ""; // unit price
	private String po105 = ""; // basis of unit price PE
	private String po106 = "VC"; // product qualifier 
	private String po107 = ""; // product identifier

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "PO1" 
				+ elementDelimiter + po101
				+ elementDelimiter + po102
				+ elementDelimiter + po103
				+ elementDelimiter + po104
				+ elementDelimiter + po105
				+ elementDelimiter + po106
				+ elementDelimiter + po107
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter
	
	public String getPo101() {
		return po101;
	}

	public PO850PO1 setPo101(String po101) {
		this.po101 = po101;
		return this;
	}
	
	public PO850PO1 setUniqueId(String po101) {
		return setPo101(po101);
	}

	public String getPo102() {
		return po102;
	}

	public PO850PO1 setPo102(String po102) {
		this.po102 = po102;
		return this;
	}
	
	public PO850PO1 setQuantityOrdered(String po102) {
		return setPo102(po102);
	}

	public String getPo103() {
		return po103;
	}

	public PO850PO1 setPo103(String po103) {
		this.po103 = po103;
		return this;
	}
	
	public PO850PO1 setUnitOfMeasure(String po103) {
		return setPo103(po103);
	}

	public String getPo104() {
		return po104;
	}

	public PO850PO1 setPo104(String po104) {
		this.po104 = po104;
		return this;
	}
	
	public PO850PO1 setUnitPrice(String po104) {
		return setPo104(po104);
	}

//	public String getPo105() {
//		return po105;
//	}
//
//	public PO850PO1 setPo105(String po105) {
//		this.po105 = po105;
//		return this;
//	}

//	public String getPo106() {
//		return po106;
//	}
//
//	public PO850PO1 setPo106(String po106) {
//		this.po106 = po106;
//		return this;
//	}

	public String getPo107() {
		return po107;
	}

	public PO850PO1 setPo107(String po107) {
		this.po107 = po107;
		return this;
	}
	
	public PO850PO1 setProductId(String po107) {
		return setPo107(po107);
	}
	


	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850PO1 po1 = new PO850PO1()
			.setUniqueId("1") // 1 - unique id 
			.setQuantityOrdered("10") // 2 - quantity ordered
			.setUnitOfMeasure("BX") // 3 - unit measurement
			.setUnitPrice("27.50") // 4 unit price (not total)
			.setProductId("BXTS1040") // product id
			;
		
		System.out.println(po1.generateX12("^", "~\n"));
	}
	
	
}
