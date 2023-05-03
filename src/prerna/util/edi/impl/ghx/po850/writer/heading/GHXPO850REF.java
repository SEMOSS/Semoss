package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.po850.IPO850REF;

public class GHXPO850REF implements IPO850REF {

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

	@Override
	public String getRef01() {
		return ref01;
	}

	@Override
	public GHXPO850REF setRef01(String ref01) {
		this.ref01 = ref01;
		return this;
	}
	
	@Override
	public GHXPO850REF setReferenceIdQualifier(String ref01) {
		return setRef01(ref01);
	}

	@Override
	public String getRef02() {
		return ref02;
	}

	@Override
	public GHXPO850REF setRef02(String ref02) {
		this.ref02 = ref02;
		return this;
	}
	
	@Override
	public GHXPO850REF setReferenceId(String ref02) {
		return setRef02(ref02);
	}

	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850REF ref = new GHXPO850REF()
			.setReferenceIdQualifier("ZZ") // 1
			.setReferenceId("NCRT-Demo") // 2
			;
		
		System.out.println(ref.generateX12("^", "~\n"));
	}
	
}