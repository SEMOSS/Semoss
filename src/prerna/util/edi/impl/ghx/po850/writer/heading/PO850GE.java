package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IX12Format;

public class PO850GE  implements IX12Format {

	public String ge01 = "1"; // number of included ST segments
	public String ge02 = ""; // group control number - must match GS06
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {

		String builder = "GE" 
				+ elementDelimiter + ge01
				+ elementDelimiter + ge02
				+ segmentDelimiter
				;
		
		return builder;
	}

	// setters/getter
	
	public String getGe01() {
		return ge01;
	}

	public PO850GE setGe01(String ge01) {
		this.ge01 = ge01;
		return this;
	}
	
	public PO850GE setNumberOfTransactions(String ge01) {
		return setGe01(ge01);
	} 

	public String getGe02() {
		return ge02;
	}

	public PO850GE setGe02(String ge02) {
		if(ge02 == null || ge02.length() < 3) {
			throw new IllegalArgumentException("GS06 Group Control Number must be at least 3 digits");
		}
		this.ge02 = ge02;
		return this;
	}
	
	public PO850GE setGroupControlNumber(String ge02) {
		return setGe02(ge02);
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850GE ge = new PO850GE()
			.setGroupControlNumber("001") // 02
			;
		
		System.out.println(ge.generateX12("^", "~\n"));
	}

	
}
