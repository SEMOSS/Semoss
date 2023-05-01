package prerna.util.edi.impl.ghx.po850.writer.po1loop;

import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.po1loop.PO850PO1Entity;

public class PO850PO1Loop implements IX12Format {

	private List<PO850PO1Entity> po1list = new ArrayList<>();

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
