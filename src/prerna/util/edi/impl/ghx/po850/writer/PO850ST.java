package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850ST implements IX12Format {

	private String st01 = "850";
	private String st02 = ""; // transaction control number - must match se02
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "ST" 
				+ elementDelimiter + st01
				+ elementDelimiter + st02
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	public String getSt02() {
		return st02;
	}

	public PO850ST setSt02(String st02) {
		if(st02 == null || st02.length() < 4 || st02.length() > 9) {
			throw new IllegalArgumentException("ST02 transaction control number must be between 4 and 9 digits long");
		}
		this.st02 = st02;
		return this;
	}

	public PO850ST setTransactionSetControlNumber(String st02) {
		return setSt02(st02);
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850ST st = new PO850ST()
			.setTransactionSetControlNumber("0001") // 2
			;
		
		System.out.println(st.generateX12("^", "~\n"));
	}
	
}