package prerna.util.edi.po850;

import prerna.util.edi.IX12Format;

public interface IPO850PER extends IX12Format {

	/**
	 * @return
	 */
	String getPer01();

	/**
	 * @param per01
	 * @return
	 */
	IPO850PER setPer01(String per01);

	/**
	 * @param per01
	 * @return
	 */
	IPO850PER setContactFunctionCode(String per01);

	/**
	 * @return
	 */
	String getPer02();

	/**
	 * @param per02
	 * @return
	 */
	IPO850PER setPer02(String per02);

	/**
	 * @param per02
	 * @return
	 */
	IPO850PER setContactName(String per02);

	/**
	 * @return
	 */
	String getPer04();

	/**
	 * @param per04
	 * @return
	 */
	IPO850PER setPer04(String per04);

	/**
	 * @param per04
	 * @return
	 */
	IPO850PER setTelephone(String per04);

	/**
	 * @return
	 */
	String getPer06();

	/**
	 * @param per06
	 * @return
	 */
	IPO850PER setPer06(String per06);

	/**
	 * @param per06
	 * @return
	 */
	IPO850PER setEmail(String per06);

}