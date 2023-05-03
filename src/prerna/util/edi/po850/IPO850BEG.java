package prerna.util.edi.po850;

import java.time.LocalDateTime;

import prerna.util.edi.IX12Format;
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;

public interface IPO850BEG extends IX12Format {

	/**
	 * 
	 * @return
	 */
	String getBeg01();

	/**
	 * 
	 * @param beg01
	 * @return
	 */
	IPO850BEG setBeg01(String beg01);

	/**
	 * 
	 * @param beg01
	 * @return
	 */
	IPO850BEG setTransactionPurposeCode(String beg01);
	
	/**
	 * 
	 * @return
	 */
	public String getBeg02();

	/**
	 * 
	 * @param beg02
	 * @return
	 */
	IPO850BEG setBeg02(PO850BEGQualifierIdEnum beg02);
	
	/**
	 * 
	 * @param beg02
	 * @return
	 */
	IPO850BEG setPurchaseOrderTypeCode(PO850BEGQualifierIdEnum beg02);

	/**
	 * 
	 * @return
	 */
	public String getBeg03();

	/**
	 * 
	 * @param beg03
	 * @return
	 */
	IPO850BEG setBeg03(String beg03);
	
	/**
	 * 
	 * @param beg03
	 * @return
	 */
	IPO850BEG setPurchaseOrderNumber(String beg03);

//	/**
//	 * 
//	 * @return
//	 */
//	String getBeg04();
//
//	/**
//	 * 
//	 * @param beg04
//	 * @return
//	 */
//	IPO850BEG setBeg04(String beg04);

	/**
	 * 
	 * @return
	 */
	IPO850BEG setDateAndTime();
	
	/**
	 * 
	 * @param now
	 * @return
	 */
	IPO850BEG setDateAndTime(LocalDateTime now);
	
	/**
	 * 
	 * @return
	 */
	public String getBeg05();

	/**
	 * 
	 * @param beg05
	 * @return
	 */
	IPO850BEG setBeg05(String beg05);

	/**
	 * 
	 * @param beg05
	 * @return
	 */
	IPO850BEG setDate(String beg05);
	
}
