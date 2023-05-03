package prerna.util.edi.impl.ghx.po850.writer.heading;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import prerna.util.edi.IPO850ISA;

public class GHXPO850ISA implements IPO850ISA {

	private String isa01 = ""; // authorization information qualifier
	private String isa02 = ""; // authorization information
	private String isa03 = ""; // security information qualifier
	private String isa04 = ""; // security information
	private String isa05 = ""; // sender interchange id qualifier
	private String isa06 = ""; // sender id
	private String isa07 = ""; // receiver interchange id qualifier
	private String isa08 = ""; // receiver id
	private String isa09 = ""; // date (YYMMDD)
	private String isa10 = ""; // time (HHMM)
	private String isa11 = "U"; // interchange control standards identifier - always U for EDI 00401
	private String isa12 = "00401"; // interchange control version number
	private String isa13 = ""; // interchange control number - must match IEA02
	private String isa14 = ""; // acknowledgement requested
	private String isa15 = ""; // interchange usage indicator (T - test, P production)
	private String isa16 = ""; // componenent element separator
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(this.isa16 == null) {
			this.isa16 = segmentDelimiter;
		}

		// isa16 has the element delimiter, subelement delimiter, and the segment delimiter
		// so dont need to readd at end
		
		String builder = "ISA" 
				+ elementDelimiter + formatToFixedLength(isa01, IPO850ISA.SIZE_ISA01)
				+ elementDelimiter + formatToFixedLength(isa02, IPO850ISA.SIZE_ISA02)
				+ elementDelimiter + formatToFixedLength(isa03, IPO850ISA.SIZE_ISA03)
				+ elementDelimiter + formatToFixedLength(isa04, IPO850ISA.SIZE_ISA04)
				+ elementDelimiter + formatToFixedLength(isa05, IPO850ISA.SIZE_ISA05)
				+ elementDelimiter + formatToFixedLength(isa06, IPO850ISA.SIZE_ISA06)
				+ elementDelimiter + formatToFixedLength(isa07, IPO850ISA.SIZE_ISA07)
				+ elementDelimiter + formatToFixedLength(isa08, IPO850ISA.SIZE_ISA08)
				+ elementDelimiter + formatToFixedLength(isa09, IPO850ISA.SIZE_ISA09)
				+ elementDelimiter + formatToFixedLength(isa10, IPO850ISA.SIZE_ISA10)
				+ elementDelimiter + formatToFixedLength(isa11, IPO850ISA.SIZE_ISA11)
				+ elementDelimiter + formatToFixedLength(isa12, IPO850ISA.SIZE_ISA12)
				+ elementDelimiter + formatToFixedLength(isa13, IPO850ISA.SIZE_ISA13)
				+ elementDelimiter + formatToFixedLength(isa14, IPO850ISA.SIZE_ISA14)
				+ elementDelimiter + formatToFixedLength(isa15, IPO850ISA.SIZE_ISA15)
				+ isa16;
		
		return builder;
	}

	@Override
	public String formatToFixedLength(String val, int size) {
		while(val.length() < size) {
			val += " ";
		}
		return val;
	}
	
	// setters/getter

	@Override
	public String getIsa01() {
		return isa01;
	}

	@Override
	public GHXPO850ISA setIsa01(String isa01) {
		// possible values include 00 - 06
		this.isa01 = isa01;
		return this;
	}

	@Override
	public GHXPO850ISA setAuthorizationInfoQualifier(String isa01) {
		// possible values include 00 - 06
		this.isa01 = isa01;
		return this;
	} 

	@Override
	public String getIsa02() {
		return isa02;
	}

	@Override
	public GHXPO850ISA setIsa02(String isa02) {
		this.isa02 = isa02;
		return this;
	}

	@Override
	public String getIsa03() {
		return isa03;
	}

	@Override
	public GHXPO850ISA setIsa03(String isa03) {
		this.isa03 = isa03;
		return this;
	}

	@Override
	public GHXPO850ISA setSecurityInformationQualifier(String isa03) {
		this.isa03 = isa03;
		return this;
	}

	@Override
	public String getIsa04() {
		return isa04;
	}

	@Override
	public GHXPO850ISA setIsa04(String isa04) {
		this.isa04 = isa04;
		return this;
	}

	@Override
	public String getIsa05() {
		return isa05;
	}

	@Override
	public GHXPO850ISA setIsa05(String isa05) {
		this.isa05 = isa05;
		return this;
	}

	@Override
	public GHXPO850ISA setSenderIdQualifier(String isa05) {
		this.isa05 = isa05;
		return this;
	}

	@Override
	public String getIsa06() {
		return isa06;
	}

	@Override
	public GHXPO850ISA setIsa06(String isa06) {
		this.isa06 = isa06;
		return this;
	}

	@Override
	public GHXPO850ISA setSenderId(String isa06) {
		this.isa06 = isa06;
		return this;
	}

	@Override
	public String getIsa07() {
		return isa07;
	}

	@Override
	public GHXPO850ISA setIsa07(String isa07) {
		this.isa07 = isa07;
		return this;
	}

	@Override
	public GHXPO850ISA setReceiverIdQualifier(String isa07) {
		this.isa07 = isa07;
		return this;
	}

	@Override
	public String getIsa08() {
		return isa08;
	}

	@Override
	public GHXPO850ISA setIsa08(String isa08) {
		this.isa08 = isa08;
		return this;
	}

	@Override
	public GHXPO850ISA setReceiverId(String isa08) {
		this.isa08 = isa08;
		return this;
	}

	@Override
	public GHXPO850ISA setDateAndTime() {
		LocalDateTime now = LocalDateTime.now();
		return setDateAndTime(now);
	}

	@Override
	public GHXPO850ISA setDateAndTime(LocalDateTime now) {
		String isa09 = now.format(DateTimeFormatter.ofPattern("yyMMdd"));
		setIsa09(isa09);
		String isa10 = now.format(DateTimeFormatter.ofPattern("HHmm"));
		setIsa10(isa10);
		return this;
	}

	@Override
	public String getIsa09() {
		return isa09;
	}

	@Override
	public GHXPO850ISA setIsa09(String isa09) {
		if(isa09 == null || isa09.length() != 6) {
			throw new IllegalArgumentException("ISA09 Interchnage Date must be 6 digit date YYMMDD");
		}
		this.isa09 = isa09;
		return this;
	}

	@Override
	public GHXPO850ISA setInterchangeDate(String isa09) {
		return setIsa09(isa09);
	}

	@Override
	public String getIsa10() {
		return isa10;
	}

	@Override
	public GHXPO850ISA setIsa10(String isa10) {
		if(isa10 == null || isa10.length() != 4) {
			throw new IllegalArgumentException("ISA10 Interchange Time must be 4 digit time HHMM");
		}
		this.isa10 = isa10;
		return this;
	}

	@Override
	public GHXPO850ISA setInterchangeTime0(String isa10) {
		return setIsa10(isa10);
	}

	@Override
	public String getIsa11() {
		return isa11;
	}

//	public PO850ISA setIsa11(String isa11) {
//		this.isa11 = isa11;
//		return this;
//	}

	@Override
	public String getIsa12() {
		return isa12;
	}

//	public PO850ISA setIsa12(String isa12) {
//		this.isa12 = isa12;
//		return this;
//	}

	@Override
	public String getIsa13() {
		return isa13;
	}

	@Override
	public GHXPO850ISA setIsa13(String isa13) {
		if(isa13 == null || isa13.length() != 9) {
			throw new IllegalArgumentException("ISA13 Interchange Control Number must be 9 digits");
		}
		this.isa13 = isa13;
		return this;
	}

	@Override
	public GHXPO850ISA setInterchangeControlNumber(String isa13) {
		return setIsa13(isa13);
	}

	@Override
	public String getIsa14() {
		return isa14;
	}

	@Override
	public GHXPO850ISA setIsa14(String isa14) {
		if(isa14 == null ||  (!isa14.equals("0") && !isa14.equals("1")) ) {
			throw new IllegalArgumentException("ISA14 Acknoledgement Requested must be 0=None or 1=Yes");
		}
		this.isa14 = isa14;
		return this;
	}

	@Override
	public GHXPO850ISA setAcknowledgementRequested(String isa14) {
		return setIsa14(isa14);
	}

	@Override
	public String getIsa15() {
		return isa15;
	}

	@Override
	public GHXPO850ISA setIsa15(String isa15) {
		if(isa15 == null ||  (!isa15.equals("T") && !isa15.equals("P")) ) {
			throw new IllegalArgumentException("ISA15 Usage Indicator must be T or P");
		}
		this.isa15 = isa15;
		return this;
	}

	@Override
	public GHXPO850ISA setUsageIndicator(String isa15) {
		return setIsa15(isa15);
	}

	@Override
	public String getIsa16() {
		return isa16;
	}

	@Override
	public GHXPO850ISA setIsa16(String isa16) {
		this.isa16 = isa16;
		return this;
	}
	
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		GHXPO850ISA isa = new GHXPO850ISA()
			.setAuthorizationInfoQualifier("00") // 1
			.setSecurityInformationQualifier("00") // 4
			.setSenderIdQualifier("ZZ") // 5
			.setSenderId("SENDER1234") // 6
			.setReceiverIdQualifier("ZZ") // 7
			.setReceiverId("RECEIVER12") // 8
			.setDateAndTime() // 9, 10
			.setInterchangeControlNumber("123456789") // 13
			.setAcknowledgementRequested("1")
			.setUsageIndicator("T")
			.setIsa16("^:~\n")
			;
		
		System.out.println(isa.generateX12("^", "~\n"));
	}
	
}
