package prerna.util.edi;

public interface IPO850FunctionalGroup extends IX12Format {

	/**
	 * Need this to be dynamic based on number of transaction sets added
	 * @return
	 */
	IPO850FunctionalGroup calculateGe();
	
	/**
	 * Adding a transaction set to the functional group
	 * @param st
	 * @return
	 */
	IPO850FunctionalGroup addTransactionSet(IPO850TransactionSet st);

	
}
