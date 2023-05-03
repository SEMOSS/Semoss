package prerna.util.edi.impl.ghx.po850.writer.heading;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import prerna.util.edi.po850.IPO850BEG;
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;

public class GHXPO850BEG implements IPO850BEG {

	private String beg01 = ""; // transaction set purpose code
	private String beg02 = PO850BEGQualifierIdEnum.NE.getId(); // purchase order type code, NE=New Order
	private String beg03 = ""; // purchase order number
	private String beg04 = ""; // not used...
	private String beg05 = ""; // date CCYYMMDD
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "BEG" 
				+ elementDelimiter + beg01
				+ elementDelimiter + beg02
				+ elementDelimiter + beg03
				+ elementDelimiter + beg04
				+ elementDelimiter + beg05
				+ segmentDelimiter;
		
		return builder;
	}
	
	// setters/getter

	@Override
	public String getBeg01() {
		return beg01;
	}

	@Override
	public GHXPO850BEG setBeg01(String beg01) {
		this.beg01 = beg01;
		return this;
	}

	@Override
	public GHXPO850BEG setTransactionPurposeCode(String beg01) {
		return setBeg01(beg01);
	} 
	
	@Override
	public String getBeg02() {
		return beg02;
	}

	@Override
	public GHXPO850BEG setBeg02(PO850BEGQualifierIdEnum beg02) {
		this.beg02 = beg02.getId();
		return this;
	}
	
	@Override
	public GHXPO850BEG setPurchaseOrderTypeCode(PO850BEGQualifierIdEnum beg02) {
		return setBeg02(beg02);
	}

	@Override
	public String getBeg03() {
		return beg03;
	}

	@Override
	public GHXPO850BEG setBeg03(String beg03) {
		this.beg03 = beg03;
		return this;
	}
	
	@Override
	public GHXPO850BEG setPurchaseOrderNumber(String beg03) {
		return setBeg03(beg03);
	}

//	public String getBeg04() {
//		return beg04;
//	}
//
//	public PO850BEG setBeg04(String beg04) {
//		this.beg04 = beg04;
//		return this;
//	}

	@Override
	public GHXPO850BEG setDateAndTime() {
		LocalDateTime now = LocalDateTime.now();
		return setDateAndTime(now);
	}
	
	@Override
	public GHXPO850BEG setDateAndTime(LocalDateTime now) {
		String beg05 = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		setBeg05(beg05);
		return this;
	}
	
	@Override
	public String getBeg05() {
		return beg05;
	}

	@Override
	public GHXPO850BEG setBeg05(String beg05) {
		if(beg05 == null || beg05.length() != 8) {
			throw new IllegalArgumentException("BEG05 Date must be 8 digit date CCYYMMDD");
		}
		this.beg05 = beg05;
		return this;
	}

	@Override
	public GHXPO850BEG setDate(String beg05) {
		return setBeg05(beg05);
	}
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850BEG beg = new GHXPO850BEG()
			.setTransactionPurposeCode("00") // 1
			.setPurchaseOrderNumber("RequestID") // 3
			.setDateAndTime()
			;
		
		System.out.println(beg.generateX12("^", "~\n"));
	}
}
