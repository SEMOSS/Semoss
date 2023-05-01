package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850REF implements IX12Format {

	private String ref01 = "";
	private String ref02 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "REF" 
				+ elementDelimiter + ref01
				+ elementDelimiter + ref02
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getRef01() {
		return ref01;
	}

	public PO850REF setRef01(String ref01) {
		this.ref01 = ref01;
		return this;
	}

	public String getRef02() {
		return ref02;
	}

	public PO850REF setRef02(String ref02) {
		this.ref02 = ref02;
		return this;
	}

}