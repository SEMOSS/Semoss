package prerna.util.edi;

public interface IPO850GE extends IX12Format {

	/**
	 * 
	 * @return
	 */
	String getGe01();

	/**
	 * 
	 * @param ge01
	 * @return
	 */
	IPO850GE setGe01(String ge01);

	/**
	 * 
	 * @param ge01
	 * @return
	 */
	IPO850GE setNumberOfTransactions(String ge01);

	/**
	 * 
	 * @return
	 */
	String getGe02();

	/**
	 * 
	 * @param ge02
	 * @return
	 */
	IPO850GE setGe02(String ge02);

	/**
	 * 
	 * @param ge02
	 * @return
	 */
	IPO850GE setGroupControlNumber(String ge02);

}