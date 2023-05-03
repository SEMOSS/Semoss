package prerna.util.edi;

public interface IPO850SE extends IX12Format {

	/**
	 * @return
	 */
	String getSe01();

	/**
	 * @param se01
	 * @return
	 */
	IPO850SE setSe01(String se01);

	/**
	 * @param se01
	 * @return
	 */
	IPO850SE setTotalSegments(String se01);

	/**
	 * @return
	 */
	String getSe02();

	/**
	 * @param se02
	 * @return
	 */
	IPO850SE setSe02(String se02);

	/**
	 * @param st02
	 * @return
	 */
	IPO850SE setTransactionSetControlNumber(String st02);

}