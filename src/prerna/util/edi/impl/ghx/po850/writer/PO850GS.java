package prerna.util.edi.impl.ghx.po850.writer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import prerna.util.edi.IX12Format;

public class PO850GS implements IX12Format {

	private String gs01 = "PO"; // functional identifier code
	private String gs02 = ""; // sender interchange id qualifier
	private String gs03 = ""; // receiver interchange id qualifier
	private String gs04 = ""; // date CCYYMMDD
	private String gs05 = ""; // time HHmm
	private String gs06 = ""; // group control number - must match GE02
	private String gs07 = "T"; // issuer of the standard used with GS08 - T = Transportation Data Coordinating Committe (TDCC), X = Accredited Standards Committee X12
	private String gs08 = "004010"; // version industry identifier
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "GS" 
				+ elementDelimiter + gs01
				+ elementDelimiter + gs02
				+ elementDelimiter + gs03
				+ elementDelimiter + gs04
				+ elementDelimiter + gs05
				+ elementDelimiter + gs06
				+ elementDelimiter + gs07
				+ elementDelimiter + gs08
				+ segmentDelimiter;
		
		return builder;
	}

	// setters/getter

	
//	public String getGs01() {
//		return gs01;
//	}
//
//	public PO850GS setGs01(String gs01) {
//		this.gs01 = gs01;
//	}

	public String getGs02() {
		return gs02;
	}

	public PO850GS setGs02(String gs02) {
		this.gs02 = gs02;
		return this;
	}
	
	public PO850GS setSenderId(String gs02) {
		return setGs02(gs02);
	}

	public String getGs03() {
		return gs03;
	}

	public PO850GS setGs03(String gs03) {
		this.gs03 = gs03;
		return this;
	}
	
	public PO850GS setReceiverId(String gs03) {
		return setGs03(gs03);
	}

	public PO850GS setDateAndTime() {
		LocalDateTime now = LocalDateTime.now();
		return setDateAndTime(now);
	}
	
	public PO850GS setDateAndTime(LocalDateTime now) {
		String gs04 = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		setGs04(gs04);
		String gs05 = now.format(DateTimeFormatter.ofPattern("HHmm"));
		setGs05(gs05);
		return this;
	}
	
	public String getGs04() {
		return gs04;
	}

	public PO850GS setGs04(String gs04) {
		if(gs04 == null || gs04.length() != 8) {
			throw new IllegalArgumentException("GS04 Date must be 8 digit date CCYYMMDD");
		}
		this.gs04 = gs04;
		return this;
	}
	
	public PO850GS setDate(String gs04) {
		return  setGs04(gs04);
	}

	public String getGs05() {
		return gs05;
	}

	public PO850GS setGs05(String gs05) {
		if(gs05 == null || gs05.length() != 4) {
			throw new IllegalArgumentException("GS05 Time must be 4 digit time HHMM");
		}
		this.gs05 = gs05;
		return this;
	}

	public String getGs06() {
		return gs06;
	}

	public PO850GS setGs06(String gs06) {
		if(gs06 == null || gs06.length() < 3) {
			throw new IllegalArgumentException("GS06 Group Control Number must be at least 3 digits");
		}
		this.gs06 = gs06;
		return this;
	}
	
	public PO850GS setGroupControlNumber(String gs06) {
		return setGs06(gs06);
	}

	public String getGs07() {
		return gs07;
	}

//	public PO850GS setGs07(String gs07) {
//		this.gs07 = gs07;
//		return this;
//	}

	public String getGs08() {
		return gs08;
	}

//	public PO850GS setGs08(String gs08) {
//		this.gs08 = gs08;
//		return this;
//	}

	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850GS gs = new PO850GS()
			.setSenderId("SENDER1234") // 2
			.setReceiverId("RECEIVER12") // 3
			.setDateAndTime() // 4, 5
			.setGroupControlNumber("001") // 6
			;
		
		System.out.println(gs.generateX12("^", "~\n"));
	}
	
}
