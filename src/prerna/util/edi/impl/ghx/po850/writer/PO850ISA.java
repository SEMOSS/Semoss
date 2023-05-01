package prerna.util.edi.impl.ghx.po850.writer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import prerna.util.edi.IX12Format;

public class PO850ISA implements IX12Format {

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
				+ elementDelimiter + isa01
				+ elementDelimiter + isa02
				+ elementDelimiter + isa03
				+ elementDelimiter + isa04
				+ elementDelimiter + isa05
				+ elementDelimiter + isa06
				+ elementDelimiter + isa07
				+ elementDelimiter + isa08
				+ elementDelimiter + isa09
				+ elementDelimiter + isa10
				+ elementDelimiter + isa11
				+ elementDelimiter + isa12
				+ elementDelimiter + isa13
				+ elementDelimiter + isa14
				+ elementDelimiter + isa15
				+ isa16;
		
		return builder;
	}

	// setters/getter

	public String getIsa01() {
		return isa01;
	}

	public PO850ISA setIsa01(String isa01) {
		// possible values include 00 - 06
		this.isa01 = isa01;
		return this;
	}
	
	public PO850ISA setAuthorizationInfoQualifier(String isa01) {
		// possible values include 00 - 06
		this.isa01 = isa01;
		return this;
	} 

	public String getIsa02() {
		return isa02;
	}

	public PO850ISA setIsa02(String isa02) {
		this.isa02 = isa02;
		return this;
	}

	public String getIsa03() {
		return isa03;
	}

	public PO850ISA setIsa03(String isa03) {
		this.isa03 = isa03;
		return this;
	}
	
	public PO850ISA setSecurityInformationQualifier(String isa03) {
		this.isa03 = isa03;
		return this;
	}

	public String getIsa04() {
		return isa04;
	}

	public PO850ISA setIsa04(String isa04) {
		this.isa04 = isa04;
		return this;
	}

	public String getIsa05() {
		return isa05;
	}

	public PO850ISA setIsa05(String isa05) {
		this.isa05 = isa05;
		return this;
	}
	
	public PO850ISA setSenderIdQualifier(String isa05) {
		this.isa05 = isa05;
		return this;
	}

	public String getIsa06() {
		return isa06;
	}

	public PO850ISA setIsa06(String isa06) {
		this.isa06 = isa06;
		return this;
	}
	
	public PO850ISA setSenderId(String isa06) {
		this.isa06 = isa06;
		return this;
	}

	public String getIsa07() {
		return isa07;
	}

	public PO850ISA setIsa07(String isa07) {
		this.isa07 = isa07;
		return this;
	}
	
	public PO850ISA setReceiverIdQualifier(String isa07) {
		this.isa07 = isa07;
		return this;
	}

	public String getIsa08() {
		return isa08;
	}

	public PO850ISA setIsa08(String isa08) {
		this.isa08 = isa08;
		return this;
	}
	
	public PO850ISA setReceiverId(String isa08) {
		this.isa08 = isa08;
		return this;
	}
	
	public PO850ISA setDateAndTime() {
		LocalDateTime now = LocalDateTime.now();
		return setDateAndTime(now);
	}
	
	public PO850ISA setDateAndTime(LocalDateTime now) {
		String isa09 = now.format(DateTimeFormatter.ofPattern("yyMMdd"));
		setIsa09(isa09);
		String isa10 = now.format(DateTimeFormatter.ofPattern("HHmm"));
		setIsa10(isa10);
		return this;
	}

	public String getIsa09() {
		return isa09;
	}

	public PO850ISA setIsa09(String isa09) {
		if(isa09 == null || isa09.length() != 6) {
			throw new IllegalArgumentException("ISA09 Interchnage Date must be 6 digit date YYMMDD");
		}
		this.isa09 = isa09;
		return this;
	}
	
	public PO850ISA setInterchangeDate(String isa09) {
		return setIsa09(isa09);
	}
	
	public String getIsa10() {
		return isa10;
	}

	public PO850ISA setIsa10(String isa10) {
		if(isa10 == null || isa10.length() != 4) {
			throw new IllegalArgumentException("ISA10 Interchange Time must be 4 digit time HHMM");
		}
		this.isa10 = isa10;
		return this;
	}
	
	public PO850ISA setInterchangeTime0(String isa10) {
		return setIsa10(isa10);
	}

	public String getIsa11() {
		return isa11;
	}

//	public PO850ISA setIsa11(String isa11) {
//		this.isa11 = isa11;
//		return this;
//	}

	public String getIsa12() {
		return isa12;
	}

//	public PO850ISA setIsa12(String isa12) {
//		this.isa12 = isa12;
//		return this;
//	}

	public String getIsa13() {
		return isa13;
	}

	public PO850ISA setIsa13(String isa13) {
		if(isa13 == null || isa13.length() != 9) {
			throw new IllegalArgumentException("ISA13 Interchange Control Number must be 9 digits");
		}
		this.isa13 = isa13;
		return this;
	}
	
	public PO850ISA setInterchangeControlNumber(String isa13) {
		return setIsa13(isa13);
	}

	public String getIsa14() {
		return isa14;
	}

	public PO850ISA setIsa14(String isa14) {
		if(isa14 == null ||  (!isa14.equals("0") && !isa14.equals("1")) ) {
			throw new IllegalArgumentException("ISA14 Acknoledgement Requested must be 0=None or 1=Yes");
		}
		this.isa14 = isa14;
		return this;
	}
	
	public PO850ISA setAcknowledgementRequested(String isa14) {
		return setIsa14(isa14);
	}

	public String getIsa15() {
		return isa15;
	}

	public PO850ISA setIsa15(String isa15) {
		if(isa15 == null ||  (!isa15.equals("T") && !isa15.equals("P")) ) {
			throw new IllegalArgumentException("ISA15 Usage Indicator must be T or P");
		}
		this.isa15 = isa15;
		return this;
	}
	
	public PO850ISA setUsageIndicator(String isa15) {
		return setIsa15(isa15);
	}

	public String getIsa16() {
		return isa16;
	}

	public PO850ISA setIsa16(String isa16) {
		this.isa16 = isa16;
		return this;
	}
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850ISA isa = new PO850ISA()
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
