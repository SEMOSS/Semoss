package prerna.util.edi;

public interface IX12Format {

	
	/**
	 * 
	 * @param elementDelimiter
	 * @param segmentDelimiter
	 * @return
	 */
	String generateX12(String elementDelimiter, String segmentDelimiter);
	
}
