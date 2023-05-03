package prerna.util.edi;

public interface IPO850ST extends IX12Format {

	/**
	 * @return
	 */
	String getSt02();

	/**
	 * @param st02
	 * @return
	 */
	IPO850ST setSt02(String st02);

	/**
	 * 
	 * @param st02
	 * @return
	 */
	IPO850ST setTransactionSetControlNumber(String st02);

}