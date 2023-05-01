package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850PER implements IX12Format {

	private String per01 = "";
	private String per02 = "";
	private String per03 = "TE";
	private String per04 = "";
	private String per05 = "EM";
	private String per06 = "";
	
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

	public String getPer02() {
		return per02;
	}

	public PO850PER setPer02(String per02) {
		this.per02 = per02;
		return this;
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

}
