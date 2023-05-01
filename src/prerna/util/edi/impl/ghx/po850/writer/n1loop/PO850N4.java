package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N4 implements IX12Format {

	private String n401 = "";
	private String n402 = "";
	private String n403 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "N4" 
				+ elementDelimiter + n401
				+ elementDelimiter + n402
				+ elementDelimiter + n403
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getN401() {
		return n401;
	}

	public PO850N4 setN401(String n401) {
		this.n401 = n401;
		return this;
	}

	public String getN402() {
		return n402;
	}

	public PO850N4 setN402(String n402) {
		this.n402 = n402;
		return this;
	}

	public String getN403() {
		return n403;
	}

	public PO850N4 setN403(String n403) {
		this.n403 = n403;
		return this;
	}

}
