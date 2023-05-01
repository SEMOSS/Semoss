package prerna.util.edi.impl.ghx.po850.writer;

import java.time.LocalDateTime;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N1;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N1Entity;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N1Loop;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N3;
import prerna.util.edi.impl.ghx.po850.writer.n1loop.PO850N4;
import prerna.util.edi.impl.ghx.po850.writer.po1loop.PO850PID;
import prerna.util.edi.impl.ghx.po850.writer.po1loop.PO850PO1;
import prerna.util.edi.impl.ghx.po850.writer.po1loop.PO850PO1Entity;
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
	// reference identification
	private PO850PER per;
	
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
		if(elementDelimiter != null && !elementDelimiter.isEmpty()) {
			this.elementDelimiter = elementDelimiter;
		}
		if(segmentDelimiter != null && !segmentDelimiter.isEmpty()) {
			this.segmentDelimiter = segmentDelimiter;
		}
		
		String builder = isa.generateX12(elementDelimiter, segmentDelimiter)
				+ gs.generateX12(elementDelimiter, segmentDelimiter)
				+ st.generateX12(elementDelimiter, segmentDelimiter)
				+ beg.generateX12(elementDelimiter, segmentDelimiter)
				+ ref.generateX12(elementDelimiter, segmentDelimiter)
				+ per.generateX12(elementDelimiter, segmentDelimiter)
				+ n1loop.generateX12(elementDelimiter, segmentDelimiter)
				+ po1loop.generateX12(elementDelimiter, segmentDelimiter)
//				+ se.generateX12(elementDelimiter, segmentDelimiter)
//				+ ge.generateX12(elementDelimiter, segmentDelimiter)
//				+ iea.generateX12(elementDelimiter, segmentDelimiter)
				;
		
		return builder;
	}
	
	// get/set methods
	
	public String getElementDelimiter() {
		return elementDelimiter;
	}

	public PO850 setElementDelimiter(String elementDelimiter) {
		this.elementDelimiter = elementDelimiter;
		return this;
	}

	public String getSegmentDelimiter() {
		return segmentDelimiter;
	}

	public PO850 setSegmentDelimiter(String segmentDelimiter) {
		this.segmentDelimiter = segmentDelimiter;
		return this;
	}
	
	public PO850ISA getIsa() {
		return isa;
	}
	
	public PO850 setIsa(PO850ISA isa) {
		this.isa = isa;
		return this;
	}
	
	public PO850GS getGs() {
		return gs;
	}
	
	public PO850 setGs(PO850GS gs) {
		this.gs = gs;
		return this;
	}
	
	public PO850ST getSt() {
		return st;
	}
	
	public PO850 setSt(PO850ST st) {
		this.st = st;
		return this;
	}
	
	public PO850BEG getBeg() {
		return beg;
	}
	
	public PO850 setBeg(PO850BEG beg) {
		this.beg = beg;
		return this;
	}
	
	public PO850REF getRef() {
		return ref;
	}
	
	public PO850 setRef(PO850REF ref) {
		this.ref = ref;
		return this;
	}
	
	public PO850PER getPer() {
		return per;
	}
	
	public PO850 setPer(PO850PER per) {
		this.per = per;
		return this;
	}
	
	public PO850N1Loop getN1loop() {
		return n1loop;
	}
	
	public PO850 setN1loop(PO850N1Loop n1loop) {
		this.n1loop = n1loop;
		return this;
	}
	
	public PO850PO1Loop getPo1loop() {
		return po1loop;
	}
	
	public PO850 setPo1loop(PO850PO1Loop po1loop) {
		this.po1loop = po1loop;
		return this;
	}
	
	public PO850SE getSe() {
		return se;
	}
	
	public PO850 setSe(PO850SE se) {
		this.se = se;
		return this;
	}
	
	public PO850GE getGe() {
		return ge;
	}
	
	public PO850 setGe(PO850GE ge) {
		this.ge = ge;
		return this;
	}
	
	public PO850IEA getIea() {
		return iea;
	}
	
	public PO850 setIea(PO850IEA iea) {
		this.iea = iea;
		return this;
	}
	
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();
		
		PO850 po850 = new PO850()
				.setIsa(new PO850ISA()
					.setAuthorizationInfoQualifier("00") // 1
					.setSecurityInformationQualifier("00") // 4
					.setSenderIdQualifier("ZZ") // 5
					.setSenderId("SENDER1234") // 6
					.setReceiverIdQualifier("ZZ") // 7
					.setReceiverId("RECEIVER12") // 8
					.setDateAndTime(now) // 9, 10
					.setInterchangeControlNumber("123456789") // 13
					.setAcknowledgementRequested("1")
					.setUsageIndicator("T")
					.setIsa16("^:~\n")
					)
				.setGs(new PO850GS()
					.setSenderId("SENDER1234") // 2
					.setReceiverId("RECEIVER12") // 3
					.setDateAndTime(now) // 4, 5
					.setGroupControlNumber("1") // 6
					)
				.setSt(new PO850ST()
					.setTransactionSetControlNumber("0001")
				)
				.setBeg(new PO850BEG()
					.setTransactionPurposeCode("00") // 1
					.setPurchaseOrderNumber("RequestID") // 3
					.setDateAndTime(now)
				)
				.setRef(new PO850REF()
					.setReferenceIdentification("NCRT-Demo")
				)
				.setPer(new PO850PER()
						.setContactFunctionCode("BD") // 1 - BD=Buyer Name
						.setContactName("Maher Khalil") // 2
						.setTelephone("(202)222-2222") // 4
						.setEmail("mahkhalil@deloitte.com") // 6
				)
				.setN1loop(new PO850N1Loop()
					.addN1Group(new PO850N1Entity()
						.setN1( new PO850N1()
							.setEntityCode("ST") // 1 - ship to
							.setName("Anchorage VA Medical Center") // 2 - name
							.setIdentificationCode("91") // 3 - 91=assigned by seller
							.setIdentificationCode("DEMO-ID")
						)
						.setN3(new PO850N3()
							.setAddressInfo1("1201 N Muldoon Rd")
						)
						.setN4(new PO850N4()
							.setCity("Anchorage")
							.setState("AK")
							.setZip("99504")
							.setCountryCode("US")
						)
					)
				)
				.setPo1loop(new PO850PO1Loop()
					.addPO1(new PO850PO1Entity()
						.setPo1(new PO850PO1()
							.setUniqueId("1") // 1 - unique id 
							.setQuantityOrdered("10") // 2 - quantity ordered
							.setUnitOfMeasure("BX") // 3 - unit measurement
							.setUnitPrice("27.50") // 4 unit price (not total)
							.setProductId("BXTS1040") // product id
						)
						.setPid(new PO850PID()
							.setItemDescription("CARDINAL HEALTH™ WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50")
						)
					)
				)
			;
		
		System.out.println(po850.generateX12("^", "~\n"));
	}

	
}
