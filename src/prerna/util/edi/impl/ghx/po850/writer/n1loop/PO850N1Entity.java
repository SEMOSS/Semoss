package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N1;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N2;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N3;

public class PO850N1Entity implements IX12Format {

	private PO850N1 n1;
	private PO850N2 n2;
	private PO850N3 n3;
	private PO850N3 n4;
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		// TODO Auto-generated method stub
		return null;
	}

}
