package prerna.util.ghx.edi.po850.writer;

import prerna.util.ghx.edi.po850.writer.n1loop.PO850N1Loop;
import prerna.util.ghx.edi.po850.writer.po1loop.PO850PO1Loop;

public class PO850 {

	// headers isa and ga
	private PO850ISA isa;
	private PO850GS gs;
	// start transaction
	private PO850ST st;
	
	
	
	// beginning segment
	private PO850BEG beg;
	// reference identification
	private PO850REF ref;
	
	// n1 loop
	private PO850N1Loop n1loop;
	// po1 loop
	private PO850PO1Loop po1loop;
	
	
	
	// end transaction 
	private PO850SE se;
	// end of gs and isa
	private PO850GE ge;
	private PO850IEA iea;

}
