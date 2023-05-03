package prerna.util.edi;

import java.time.LocalDateTime;

public interface IPO850ISA extends IX12Format {

	int SIZE_ISA01 = 2;
	int SIZE_ISA02 = 10;
	int SIZE_ISA03 = 2;
	int SIZE_ISA04 = 10;
	int SIZE_ISA05 = 2;
	int SIZE_ISA06 = 15;
	int SIZE_ISA07 = 2;
	int SIZE_ISA08 = 15;
	int SIZE_ISA09 = 6;
	int SIZE_ISA10 = 4;
	int SIZE_ISA11 = 1;
	int SIZE_ISA12 = 5;
	int SIZE_ISA13 = 9;
	int SIZE_ISA14 = 1;
	int SIZE_ISA15 = 1;
	int SIZE_ISA16 = 3;

	/**
	 * 
	 * @param val
	 * @param size
	 * @return
	 */
	String formatToFixedLength(String val, int size);
	
	/**
	 * 
	 * @return
	 */
	String getIsa01();

	/**
	 * 
	 * @param isa01
	 * @return
	 */
	IPO850ISA setIsa01(String isa01);
	
	/**
	 * 
	 * @param isa01
	 * @return
	 */
	IPO850ISA setAuthorizationInfoQualifier(String isa01);

	/**
	 * 
	 * @return
	 */
	String getIsa02();

	/**
	 * 
	 * @param isa02
	 * @return
	 */
	IPO850ISA setIsa02(String isa02);

	/**
	 * 
	 * @return
	 */
	String getIsa03();

	/**
	 * 
	 * @param isa03
	 * @return
	 */
	IPO850ISA setIsa03(String isa03);
	
	/**
	 * 
	 * @param isa03
	 * @return
	 */
	IPO850ISA setSecurityInformationQualifier(String isa03);

	/**
	 * 
	 * @return
	 */
	String getIsa04();

	/**
	 * 
	 * @param isa04
	 * @return
	 */
	IPO850ISA setIsa04(String isa04);

	/**
	 * 
	 * @return
	 */
	String getIsa05();

	/**
	 * 
	 * @param isa05
	 * @return
	 */
	IPO850ISA setIsa05(String isa05);
	
	/**
	 * 
	 * @param isa05
	 * @return
	 */
	IPO850ISA setSenderIdQualifier(String isa05);

	/**
	 * 
	 * @return
	 */
	String getIsa06();

	/**
	 * 
	 * @param isa06
	 * @return
	 */
	IPO850ISA setIsa06(String isa06);
	
	/**
	 * 
	 * @param isa06
	 * @return
	 */
	IPO850ISA setSenderId(String isa06);

	/**
	 * 
	 * @return
	 */
	String getIsa07();

	/**
	 * 
	 * @param isa07
	 * @return
	 */
	IPO850ISA setIsa07(String isa07);
	
	/**
	 * 
	 * @param isa07
	 * @return
	 */
	IPO850ISA setReceiverIdQualifier(String isa07);

	/**
	 * 
	 * @return
	 */
	String getIsa08();

	/**
	 * 
	 * @param isa08
	 * @return
	 */
	IPO850ISA setIsa08(String isa08);
	
	/**
	 * 
	 * @param isa08
	 * @return
	 */
	IPO850ISA setReceiverId(String isa08);
	
	/**
	 * 
	 * @return
	 */
	IPO850ISA setDateAndTime();
	
	/**
	 * 
	 * @param now
	 * @return
	 */
	IPO850ISA setDateAndTime(LocalDateTime now);

	/**
	 * 
	 * @return
	 */
	String getIsa09();

	/**
	 * 
	 * @param isa09
	 * @return
	 */
	IPO850ISA setIsa09(String isa09);
	
	/**
	 * 
	 * @param isa09
	 * @return
	 */
	IPO850ISA setInterchangeDate(String isa09);
	
	/**
	 * 
	 * @return
	 */
	String getIsa10();

	/**
	 * 
	 * @param isa10
	 * @return
	 */
	IPO850ISA setIsa10(String isa10);

	/**
	 * 
	 * @param isa10
	 * @return
	 */
	IPO850ISA setInterchangeTime0(String isa10);

	/**
	 * 
	 * @return
	 */
	String getIsa11();

//	public PO850ISA setIsa11(String isa11) {
//		this.isa11 = isa11;
//		return this;
//	}

	/**
	 * 
	 * @return
	 */
	String getIsa12();

//	public PO850ISA setIsa12(String isa12) {
//		this.isa12 = isa12;
//		return this;
//	}

	/**
	 * 
	 * @return
	 */
	String getIsa13();

	/**
	 * 
	 * @param isa13
	 * @return
	 */
	IPO850ISA setIsa13(String isa13);
	
	/**
	 * 
	 * @param isa13
	 * @return
	 */
	IPO850ISA setInterchangeControlNumber(String isa13);

	/**
	 * 
	 * @return
	 */
	String getIsa14();

	/**
	 * 
	 * @param isa14
	 * @return
	 */
	IPO850ISA setIsa14(String isa14);
	
	/**
	 * 
	 * @param isa14
	 * @return
	 */
	IPO850ISA setAcknowledgementRequested(String isa14);

	/**
	 * 
	 * @return
	 */
	String getIsa15();

	/**
	 * 
	 * @param isa15
	 * @return
	 */
	IPO850ISA setIsa15(String isa15);
	
	/**
	 * 
	 * @param isa15
	 * @return
	 */
	IPO850ISA setUsageIndicator(String isa15);

	/**
	 * 
	 * @return
	 */
	String getIsa16();

	/**
	 * 
	 * @param isa16
	 * @return
	 */
	IPO850ISA setIsa16(String isa16);

	
}
