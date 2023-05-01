package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IX12Format;

public class PO850IEA implements IX12Format {

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
	
	// only allowing 1 GS segment per transaction
	
//	public String getIea01() {
//		return iea01;
//	}
//
//	public void setIea01(String iea01) {
//		this.iea01 = iea01;
//	}

	public String getIea02() {
		return iea02;
	}

	public PO850IEA setIea02(String iea02) {
		if(iea02 == null || iea02.length() != 9) {
			throw new IllegalArgumentException("IEA02 Interchange Control Number must be 9 digits");
		}
		this.iea02 = iea02;
		return this;
	}

	public PO850IEA setInterchangeControlNumber(String iea02) {
		return setIea02(iea02);
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850IEA iea = new PO850IEA()
			.setInterchangeControlNumber("123456789") // 02
			;
		
		System.out.println(iea.generateX12("^", "~\n"));
	}
	
	
}
