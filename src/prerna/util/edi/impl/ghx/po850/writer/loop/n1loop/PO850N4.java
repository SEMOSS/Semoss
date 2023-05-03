package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N4 implements IX12Format {

	private String n401 = ""; // city name
	private String n402 = ""; // state id
	private String n403 = ""; // postal code
	private String n404 = ""; // country code
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "N4" 
				+ elementDelimiter + n401
				+ elementDelimiter + n402
				+ elementDelimiter + n403
				+ elementDelimiter + n404
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
	
	public PO850N4 setCity(String n401) {
		return setN401(n401);
	}

	public String getN402() {
		return n402;
	}

	public PO850N4 setN402(String n402) {
		if(n402 == null || n402.length() != 2) {
			throw new IllegalArgumentException("N402 State must be 2 digits");
		}
		this.n402 = n402;
		return this;
	}
	
	public PO850N4 setState(String n402) {
		return setN402(n402);
	}

	public String getN403() {
		return n403;
	}

	public PO850N4 setN403(String n403) {
		this.n403 = n403;
		return this;
	}

	public PO850N4 setZip(String n403) {
		return setN403(n403);
	}
	
	public String getN404() {
		return n404;
	}

	public PO850N4 setN404(String n404) {
		if(n404 == null || n404.length() != 2) {
			throw new IllegalArgumentException("N404 Country Code must be 2 digits");
		}
		this.n404 = n404;
		return this;
	}
	
	public PO850N4 setCountryCode(String n404) {
		return setN404(n404);
	}
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850N4 n4 = new PO850N4()
			.setCity("Anchorage")
			.setState("AK")
			.setZip("99504")
			.setCountryCode("US")
			;
		
		System.out.println(n4.generateX12("^", "~\n"));
	}
	
}
