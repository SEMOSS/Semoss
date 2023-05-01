package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;

public class PO850N1Loop implements IX12Format {

	private List<PO850N1Entity> n1list = new ArrayList<>();

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "";
		for(PO850N1Entity loop : n1list) {
			builder += loop.generateX12(elementDelimiter, segmentDelimiter);
		}
		return builder;
	}
	
	public PO850N1Loop addPO1(PO850N1Entity n1) {
		n1list.add(n1);
		return this;
	}
	
	public List<PO850N1Entity> getPO1List() {
		return this.n1list;
	}
	
}