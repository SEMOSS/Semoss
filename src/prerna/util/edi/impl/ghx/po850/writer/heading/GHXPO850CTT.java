package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IPO850CTT;

public class GHXPO850CTT implements IPO850CTT {

	private int ctt01 = -1; // number of po1 segments
	private int ctt02 = -1; // sum of values of quantities in PO102 for all segments
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(this.ctt01 < 0) {
			throw new IllegalArgumentException("CTT01 cannot be negative");
		}
		if(this.ctt02 < 0) {
			throw new IllegalArgumentException("CTT02 cannot be negative");
		}
		
		String builder = "CTT" 
				+ elementDelimiter + ctt01
				+ elementDelimiter + ctt02
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	@Override
	public int getCtt01() {
		return ctt01;
	}

	@Override
	public GHXPO850CTT setCtt01(int ctt01) {
		this.ctt01 = ctt01;
		return this;
	}
	
	@Override
	public GHXPO850CTT setNumPO1Segments(int ctt01) {
		return setCtt01(ctt01);
	}

	@Override
	public int getCtt02() {
		return ctt02;
	}

	@Override
	public GHXPO850CTT setCtt02(int ctt02) {
		this.ctt02 = ctt02;
		return this;
	}
	
	@Override
	public GHXPO850CTT setSumPO102Qualities(int ctt02) {
		return setCtt02(ctt02);
	}
	
	
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850CTT se = new GHXPO850CTT()
			.setNumPO1Segments(1) // 1
			.setSumPO102Qualities(10) // 2
			;
		
		System.out.println(se.generateX12("^", "~\n"));
	}


	
}