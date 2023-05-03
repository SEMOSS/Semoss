package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IPO850SE;

public class GHXPO850SE implements IPO850SE {

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

	@Override
	public String getSe01() {
		return se01;
	}

	@Override
	public GHXPO850SE setSe01(String se01) {
		this.se01 = se01;
		return this;
	}
	
	@Override
	public GHXPO850SE setTotalSegments(String se01) {
		return setSe01(se01);
	}

	@Override
	public String getSe02() {
		return se02;
	}

	@Override
	public GHXPO850SE setSe02(String se02) {
		if(se02 == null || se02.length() < 4 || se02.length() > 9) {
			throw new IllegalArgumentException("SE02 transaction control number must be between 4 and 9 digits long");
		}
		this.se02 = se02;
		return this;
	}
	
	@Override
	public GHXPO850SE setTransactionSetControlNumber(String st02) {
		return setSe02(st02);
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850SE se = new GHXPO850SE()
			.setTotalSegments("10")
			.setTransactionSetControlNumber("0001") // 2
			;
		
		System.out.println(se.generateX12("^", "~\n"));
	}
	
}