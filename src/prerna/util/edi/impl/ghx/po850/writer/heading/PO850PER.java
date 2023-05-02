package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IX12Format;

public class PO850PER implements IX12Format {

	private String per01 = ""; // contact function code - BD=Buyer Name
	private String per02 = ""; // name for per01
	private String per03 = "TE"; // telephone key
	private String per04 = ""; // telephone value
	private String per05 = "EM"; // email key
	private String per06 = ""; // email value
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "PER" 
				+ elementDelimiter + per01
				+ elementDelimiter + per02
				+ elementDelimiter + per03
				+ elementDelimiter + per04
				+ elementDelimiter + per05
				+ elementDelimiter + per06
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter
	
	public String getPer01() {
		return per01;
	}

	public PO850PER setPer01(String per01) {
		this.per01 = per01;
		return this;
	}
	
	public PO850PER setContactFunctionCode(String per01) {
		return setPer01(per01);
	}

	public String getPer02() {
		return per02;
	}

	public PO850PER setPer02(String per02) {
		this.per02 = per02;
		return this;
	}
	
	public PO850PER setContactName(String per02) {
		return setPer02(per02);
	}

//	public String getPer03() {
//		return per03;
//	}
//
//	public PO850PER setPer03(String per03) {
//		this.per03 = per03;
//		return this;
//	}

	public String getPer04() {
		return per04;
	}

	public PO850PER setPer04(String per04) {
		this.per04 = per04;
		return this;
	}
	
	public PO850PER setTelephone(String per04) {
		this.per04 = per04;
		return this;
	}

//	public String getPer05() {
//		return per05;
//	}
//
//	public PO850PER setPer05(String per05) {
//		this.per05 = per05;
//		return this;
//	}

	public String getPer06() {
		return per06;
	}

	public PO850PER setPer06(String per06) {
		this.per06 = per06;
		return this;
	}
	
	public PO850PER setEmail(String per06) {
		this.per06 = per06;
		return this;
	}

	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850PER per = new PO850PER()
			.setContactFunctionCode("NE") // 1 - NE=NewOrder, BD=Bidding
			.setContactName("Maher Khalil") // 2
			.setTelephone("(202)222-2222") // 4
			.setEmail("mahkhalil@deloitte.com") // 6
			;
		
		System.out.println(per.generateX12("^", "~\n"));
	}
}
