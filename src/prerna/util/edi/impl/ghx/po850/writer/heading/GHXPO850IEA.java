package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IPO850IEA;

public class GHXPO850IEA implements IPO850IEA {

	public String iea01 = "1"; // number of included GS segments
	public String iea02 = ""; // interchange control number - must match ISA13
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {

		String builder = "IEA" 
				+ elementDelimiter + iea01
				+ elementDelimiter + iea02
				+ segmentDelimiter
				;
		
		return builder;
	}

	// setters/getter
	
	@Override
	public String getIea01() {
		return iea01;
	}
	
	@Override
	public GHXPO850IEA setIea01(String iea01) {
		this.iea01 = iea01;
		return this;
	}
	
	@Override
	public GHXPO850IEA setTotalGroups(String iea01) {
		return setIea01(iea01);
	}

	@Override
	public String getIea02() {
		return iea02;
	}

	@Override
	public GHXPO850IEA setIea02(String iea02) {
		if(iea02 == null || iea02.length() != 9) {
			throw new IllegalArgumentException("IEA02 Interchange Control Number must be 9 digits");
		}
		this.iea02 = iea02;
		return this;
	}

	@Override
	public GHXPO850IEA setInterchangeControlNumber(String iea02) {
		return setIea02(iea02);
	}
	
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850IEA iea = new GHXPO850IEA()
			.setInterchangeControlNumber("123456789") // 02
			;
		
		System.out.println(iea.generateX12("^", "~\n"));
	}
	
	
}
