package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N1Entity implements IX12Format {

	private PO850N1 n1;
	private PO850N2 n2;
	private PO850N3 n3;
	private PO850N3 n4;
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = n1.generateX12(elementDelimiter, segmentDelimiter);
		if(n2 != null) {
			builder += n2.generateX12(elementDelimiter, segmentDelimiter);
		}
		if(n3 != null) {
			builder += n2.generateX12(elementDelimiter, segmentDelimiter);
		}
		if(n4 != null) {
			builder += n2.generateX12(elementDelimiter, segmentDelimiter);
		}
		return builder;
	}

	public PO850N1 getN1() {
		return n1;
	}

	public PO850N1Entity setN1(PO850N1 n1) {
		this.n1 = n1;
		return this;
	}

	public PO850N2 getN2() {
		return n2;
	}

	public PO850N1Entity setN2(PO850N2 n2) {
		this.n2 = n2;
		return this;
	}

	public PO850N3 getN3() {
		return n3;
	}

	public PO850N1Entity setN3(PO850N3 n3) {
		this.n3 = n3;
		return this;
	}

	public PO850N3 getN4() {
		return n4;
	}

	public PO850N1Entity setN4(PO850N3 n4) {
		this.n4 = n4;
		return this;
	}

}
