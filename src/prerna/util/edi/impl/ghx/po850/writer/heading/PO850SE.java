package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IX12Format;

public class PO850SE implements IX12Format {

	private String se01 = ""; // number of transactions in segment
	private String se02 = ""; // transaction control number - must match se02
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "SE" 
				+ elementDelimiter + se01
				+ elementDelimiter + se02
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getSe01() {
		return se01;
	}

	public PO850SE setSe01(String se01) {
		this.se01 = se01;
		return this;
	}
	
	public PO850SE setTotalSegments(String se01) {
		return setSe01(se01);
	}

	public String getSe02() {
		return se02;
	}

	public PO850SE setSe02(String se02) {
		if(se02 == null || se02.length() < 4 || se02.length() > 9) {
			throw new IllegalArgumentException("SE02 transaction control number must be between 4 and 9 digits long");
		}
		this.se02 = se02;
		return this;
	}
	
	public PO850SE setTransactionSetControlNumber(String st02) {
		return setSe02(st02);
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850SE se = new PO850SE()
			.setTotalSegments("10")
			.setTransactionSetControlNumber("0001") // 2
			;
		
		System.out.println(se.generateX12("^", "~\n"));
	}
	
}