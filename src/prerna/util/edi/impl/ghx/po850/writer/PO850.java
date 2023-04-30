package prerna.util.edi.impl.ghx.po850.writer;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N1Loop;
import prerna.util.edi.impl.ghx.po850.writer.po1loop.PO850PO1Loop;

public class PO850 implements IX12Format {

	private String elementDelimiter = "^";
	private String segmentDelimiter = "~\n";
	
	
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
	
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(elementDelimiter != null && !(elementDelimiter=elementDelimiter.trim()).isEmpty()) {
			this.elementDelimiter = elementDelimiter;
		}
		if(segmentDelimiter != null && !(segmentDelimiter=segmentDelimiter.trim()).isEmpty()) {
			this.segmentDelimiter = segmentDelimiter;
		}
		
		String builder = isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter)
				+ isa.generateX12(elementDelimiter, segmentDelimiter);
		
		return builder;
	}
	
	// get/set methods
	
	public String getElementDelimiter() {
		return elementDelimiter;
	}

	public void setElementDelimiter(String elementDelimiter) {
		this.elementDelimiter = elementDelimiter;
	}

	public String getSegmentDelimiter() {
		return segmentDelimiter;
	}

	public void setSegmentDelimiter(String segmentDelimiter) {
		this.segmentDelimiter = segmentDelimiter;
	}
	
	public PO850ISA getIsa() {
		return isa;
	}
	
	public void setIsa(PO850ISA isa) {
		this.isa = isa;
	}
	
	public PO850GS getGs() {
		return gs;
	}
	
	public void setGs(PO850GS gs) {
		this.gs = gs;
	}
	
	public PO850ST getSt() {
		return st;
	}
	
	public void setSt(PO850ST st) {
		this.st = st;
	}
	
	public PO850BEG getBeg() {
		return beg;
	}
	
	public void setBeg(PO850BEG beg) {
		this.beg = beg;
	}
	
	public PO850REF getRef() {
		return ref;
	}
	
	public void setRef(PO850REF ref) {
		this.ref = ref;
	}
	
	public PO850N1Loop getN1loop() {
		return n1loop;
	}
	
	public void setN1loop(PO850N1Loop n1loop) {
		this.n1loop = n1loop;
	}
	
	public PO850PO1Loop getPo1loop() {
		return po1loop;
	}
	
	public void setPo1loop(PO850PO1Loop po1loop) {
		this.po1loop = po1loop;
	}
	
	public PO850SE getSe() {
		return se;
	}
	
	public void setSe(PO850SE se) {
		this.se = se;
	}
	
	public PO850GE getGe() {
		return ge;
	}
	
	public void setGe(PO850GE ge) {
		this.ge = ge;
	}
	
	public PO850IEA getIea() {
		return iea;
	}
	
	public void setIea(PO850IEA iea) {
		this.iea = iea;
	}
	
	public static void main(String[] args) {
		PO850 po850 = new PO850();
	
		
		System.out.println(po850.generateX12("^", "~\n"));
	}

	
}
