package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N1 implements IX12Format {

	private String n101 = "";
	private String n102 = "";
	private String n103 = "";
	private String n104 = "";
	
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

	public String getN102() {
		return n102;
	}

	public PO850N1 setN102(String n102) {
		this.n102 = n102;
		return this;
	}

	public String getN103() {
		return n103;
	}

	public PO850N1 setN103(String n103) {
		this.n103 = n103;
		return this;
	}

	public String getN104() {
		return n104;
	}

	public PO850N1 setN104(String n104) {
		this.n104 = n104;
		return this;
	}

}
