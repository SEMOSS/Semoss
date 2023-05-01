package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N3 implements IX12Format {

	private String n301 = "";
	private String n302 = "";
	private String n303 = "";
	private String n304 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "N3" 
				+ elementDelimiter + n301
				+ elementDelimiter + n302
				+ elementDelimiter + n303
				+ elementDelimiter + n304
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getN301() {
		return n301;
	}

	public PO850N3 setN301(String n301) {
		this.n301 = n301;
		return this;
	}

	public String getN302() {
		return n302;
	}

	public PO850N3 setN302(String n302) {
		this.n302 = n302;
		return this;
	}

	public String getN303() {
		return n303;
	}

	public PO850N3 setN303(String n303) {
		this.n303 = n303;
		return this;
	}

	public String getN304() {
		return n304;
	}

	public PO850N3 setN304(String n304) {
		this.n304 = n304;
		return this;
	}

}
