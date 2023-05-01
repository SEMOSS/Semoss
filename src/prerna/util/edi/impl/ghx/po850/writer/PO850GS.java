package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850GS implements IX12Format {

	private String gs01 = "PO";
	private String gs02 = "";
	private String gs03 = "";
	private String gs04 = "";
	private String gs05 = "";
	private String gs06 = "";
	private String gs07 = "";
	private String gs08 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "GS" 
				+ elementDelimiter + gs01
				+ elementDelimiter + gs02
				+ elementDelimiter + gs03
				+ elementDelimiter + gs04
				+ elementDelimiter + gs05
				+ elementDelimiter + gs06
				+ elementDelimiter + gs07
				+ elementDelimiter + gs08
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	
//	public String getGs01() {
//		return gs01;
//	}
//
//	public PO850GS setGs01(String gs01) {
//		this.gs01 = gs01;
//	}

	public String getGs02() {
		return gs02;
	}

	public PO850GS setGs02(String gs02) {
		this.gs02 = gs02;
		return this;
	}

	public String getGs03() {
		return gs03;
	}

	public PO850GS setGs03(String gs03) {
		this.gs03 = gs03;
		return this;
	}

	public String getGs04() {
		return gs04;
	}

	public PO850GS setGs04(String gs04) {
		this.gs04 = gs04;
		return this;
	}

	public String getGs05() {
		return gs05;
	}

	public PO850GS setGs05(String gs05) {
		this.gs05 = gs05;
		return this;
	}

	public String getGs06() {
		return gs06;
	}

	public PO850GS setGs06(String gs06) {
		this.gs06 = gs06;
		return this;
	}

	public String getGs07() {
		return gs07;
	}

	public PO850GS setGs07(String gs07) {
		this.gs07 = gs07;
		return this;
	}

	public String getGs08() {
		return gs08;
	}

	public PO850GS setGs08(String gs08) {
		this.gs08 = gs08;
		return this;
	}

}
