package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850BEG implements IX12Format {

	private String beg01 = "PO";
	private String beg02 = "";
	private String beg03 = "";
	private String beg04 = "";
	private String beg05 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "BEG" 
				+ elementDelimiter + beg01
				+ elementDelimiter + beg02
				+ elementDelimiter + beg03
				+ elementDelimiter + beg04
				+ elementDelimiter + beg05
				+ segmentDelimiter;
		
		return builder;
	}
	
	// setters/getter

	public String getBeg01() {
		return beg01;
	}

	public PO850BEG setBeg01(String beg01) {
		this.beg01 = beg01;
		return this;
	}

	public String getBeg02() {
		return beg02;
	}

	public PO850BEG setBeg02(String beg02) {
		this.beg02 = beg02;
		return this;
	}

	public String getBeg03() {
		return beg03;
	}

	public PO850BEG setBeg03(String beg03) {
		this.beg03 = beg03;
		return this;
	}

	public String getBeg04() {
		return beg04;
	}

	public PO850BEG setBeg04(String beg04) {
		this.beg04 = beg04;
		return this;
	}

	public String getBeg05() {
		return beg05;
	}

	public PO850BEG setBeg05(String beg05) {
		this.beg05 = beg05;
		return this;
	}

}
