package prerna.util.edi;

import java.time.LocalDateTime;

public interface IPO850GS extends IX12Format {

	/**
	 * @return
	 */
	String getGs02();

	/**
	 * @param gs02
	 * @return
	 */
	IPO850GS setGs02(String gs02);

	/**
	 * @param gs02
	 * @return
	 */
	IPO850GS setSenderId(String gs02);

	/**
	 * @return
	 */
	String getGs03();

	/**
	 * @param gs03
	 * @return
	 */
	IPO850GS setGs03(String gs03);

	/**
	 * @param gs03
	 * @return
	 */
	IPO850GS setReceiverId(String gs03);

	/**
	 * @return
	 */
	IPO850GS setDateAndTime();

	/**
	 * @param now
	 * @return
	 */
	IPO850GS setDateAndTime(LocalDateTime now);

	/**
	 * @return
	 */
	String getGs04();

	/**
	 * @param gs04
	 * @return
	 */
	IPO850GS setGs04(String gs04);

	/**
	 * @param gs04
	 * @return
	 */
	IPO850GS setDate(String gs04);

	/**
	 * @return
	 */
	String getGs05();

	/**
	 * @param gs05
	 * @return
	 */
	IPO850GS setGs05(String gs05);

	/**
	 * @return
	 */
	String getGs06();

	/**
	 * @param gs06
	 * @return
	 */
	IPO850GS setGs06(String gs06);

	/**
	 * @param gs06
	 * @return
	 */
	IPO850GS setGroupControlNumber(String gs06);

	/**
	 * @return
	 */
	String getGs07();

	/**
	 * @return
	 */
	String getGs08();

}