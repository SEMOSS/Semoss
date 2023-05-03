package prerna.util.edi.po850;

import java.util.List;

import prerna.util.edi.IPO850FunctionalGroup;
import prerna.util.edi.IPO850IEA;
import prerna.util.edi.IPO850ISA;
import prerna.util.edi.IX12Format;

public interface IPO850 extends IX12Format {

	/**
	 * Get the element delimiter
	 * @return
	 */
	String getElementDelimiter();

	/**
	 * Set the element delimiter
	 * @param elementDelimiter
	 * @return
	 */
	IPO850 setElementDelimiter(String elementDelimiter);

	/**
	 * Get the segment delimiter
	 * @return
	 */
	String getSegmentDelimiter();

	/**
	 * Set the segment delimiter
	 * @param segmentDelimiter
	 * @return
	 */
	IPO850 setSegmentDelimiter(String segmentDelimiter);

	/**
	 * Add a functional group to the 850
	 * @param fg
	 * @return
	 */
	IPO850 addFunctionalGroup(IPO850FunctionalGroup fg);

	/**
	 * Get the list of functional groups
	 * @return
	 */
	List<IPO850FunctionalGroup> getFunctionalGroups();
	
	/**
	 * 
	 * @return
	 */
	IPO850ISA getIsa();
	
	/**
	 * 
	 * @param isa
	 * @return
	 */
	IPO850 setIsa(IPO850ISA isa);
	
	/**
	 * 
	 * @return
	 */
	IPO850IEA getIea();
	
	/**
	 * 
	 * @param iea
	 * @return
	 */
	IPO850 setIea(IPO850IEA iea);
	
	
}
