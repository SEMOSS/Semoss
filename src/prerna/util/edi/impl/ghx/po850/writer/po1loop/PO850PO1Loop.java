package prerna.util.edi.impl.ghx.po850.writer.po1loop;

import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;

public class PO850PO1Loop implements IX12Format {

	private List<PO850PO1Entity> po1list = new ArrayList<>();

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "";
		for(PO850PO1Entity loop : po1list) {
			builder += loop.generateX12(elementDelimiter, segmentDelimiter);
		}
		return builder;
	}
	
	public PO850PO1Loop addPO1(PO850PO1Entity po1) {
		po1list.add(po1);
		return this;
	}
	
	public List<PO850PO1Entity> getPO1List() {
		return this.po1list;
	}
	
}
