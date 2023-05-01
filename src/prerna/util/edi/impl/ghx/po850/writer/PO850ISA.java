package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;

public class PO850ISA implements IX12Format {

	private String isa01 = "";
	private String isa02 = "";
	private String isa03 = "";
	private String isa04 = "";
	private String isa05 = "";
	private String isa06 = "";
	private String isa07 = "";
	private String isa08 = "";
	private String isa09 = "";
	private String isa10 = "";
	private String isa11 = "";
	private String isa12 = "";
	private String isa13 = "";
	private String isa14 = "";
	private String isa15 = "";
	private String isa16 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		this.isa16 = segmentDelimiter;
		
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
				+ elementDelimiter + isa16;
		
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

	public String getIsa06() {
		return isa06;
	}

	public PO850ISA setIsa06(String isa06) {
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

	public String getIsa08() {
		return isa08;
	}

	public PO850ISA setIsa08(String isa08) {
		this.isa08 = isa08;
		return this;
	}

	public String getIsa09() {
		return isa09;
	}

	public PO850ISA setIsa09(String isa09) {
		this.isa09 = isa09;
		return this;
	}

	public String getIsa10() {
		return isa10;
	}

	public PO850ISA setIsa10(String isa10) {
		this.isa10 = isa10;
		return this;
	}

	public String getIsa11() {
		return isa11;
	}

	public PO850ISA setIsa11(String isa11) {
		this.isa11 = isa11;
		return this;
	}

	public String getIsa12() {
		return isa12;
	}

	public PO850ISA setIsa12(String isa12) {
		this.isa12 = isa12;
		return this;
	}

	public String getIsa13() {
		return isa13;
	}

	public PO850ISA setIsa13(String isa13) {
		this.isa13 = isa13;
		return this;
	}

	public String getIsa14() {
		return isa14;
	}

	public PO850ISA setIsa14(String isa14) {
		this.isa14 = isa14;
		return this;
	}

	public String getIsa15() {
		return isa15;
	}

	public PO850ISA setIsa15(String isa15) {
		this.isa15 = isa15;
		return this;
	}

	public String getIsa16() {
		return isa16;
	}

	public PO850ISA setIsa16(String isa16) {
		this.isa16 = isa16;
		return this;
	}
	
}
