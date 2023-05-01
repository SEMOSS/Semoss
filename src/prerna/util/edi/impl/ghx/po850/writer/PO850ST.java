package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850ST implements IX12Format {

	private String st01 = "850";
	private String st02 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "ST" 
				+ elementDelimiter + st01
				+ elementDelimiter + st02
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getSt02() {
		return st02;
	}

	public PO850ST setSt02(String st02) {
		this.st02 = st02;
		return this;
	}

}