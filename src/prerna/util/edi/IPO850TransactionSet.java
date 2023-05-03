package prerna.util.edi;

public interface IPO850TransactionSet extends IX12Format {

	/**
	 * Need this to be dynamic to determine the # of segments in ST/SE
	 * @return
	 */
	IPO850TransactionSet calculateSe();
	
}
