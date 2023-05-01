package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850PER;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850REF;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850SE;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850ST;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1Entity;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1Loop;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N3;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N4;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PID;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1Entity;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1Loop;

public class PO850TransactionSet implements IX12Format {

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

	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = st.generateX12(elementDelimiter, segmentDelimiter)
			+ beg.generateX12(elementDelimiter, segmentDelimiter)
			+ ref.generateX12(elementDelimiter, segmentDelimiter)
			+ per.generateX12(elementDelimiter, segmentDelimiter)
			+ n1loop.generateX12(elementDelimiter, segmentDelimiter)
			+ po1loop.generateX12(elementDelimiter, segmentDelimiter)
			+ se.generateX12(elementDelimiter, segmentDelimiter)
			;
		
		return builder;
	}
	
	public PO850TransactionSet calculateSe() {
		this.se = new PO850SE();
		this.se.setTotalSegments(
				(
					1 // sf
					+ 1 // beg
					+ 1 // ref
					+ 1 // per
					+ n1loop.getNumSegments()
					+ po1loop.getNumSegments()
					+ 1 // se
				)
				+ ""
		);
		this.se.setTransactionSetControlNumber(st.getSt02());
		return this;
	}
	
	// get/set methods

	public PO850ST getSt() {
		return st;
	}
	
	public PO850TransactionSet setSt(PO850ST st) {
		this.st = st;
		return this;
	}
	
	public PO850BEG getBeg() {
		return beg;
	}
	
	public PO850TransactionSet setBeg(PO850BEG beg) {
		this.beg = beg;
		return this;
	}
	
	public PO850REF getRef() {
		return ref;
	}
	
	public PO850TransactionSet setRef(PO850REF ref) {
		this.ref = ref;
		return this;
	}
	
	public PO850PER getPer() {
		return per;
	}
	
	public PO850TransactionSet setPer(PO850PER per) {
		this.per = per;
		return this;
	}
	
	public PO850N1Loop getN1loop() {
		return n1loop;
	}
	
	public PO850TransactionSet setN1loop(PO850N1Loop n1loop) {
		this.n1loop = n1loop;
		return this;
	}
	
	public PO850PO1Loop getPo1loop() {
		return po1loop;
	}
	
	public PO850TransactionSet setPo1loop(PO850PO1Loop po1loop) {
		this.po1loop = po1loop;
		return this;
	}
	
	public PO850SE getSe() {
		return se;
	}
	
	public PO850TransactionSet setSe(PO850SE se) {
		this.se = se;
		return this;
	}
	
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();

		PO850TransactionSet st = new PO850TransactionSet()
			.setSt(new PO850ST()
				.setTransactionSetControlNumber("0001")
			)
			.setBeg(new PO850BEG()
				.setTransactionPurposeCode("00") // 1
				.setPurchaseOrderNumber("RequestID") // 3
				.setDateAndTime(now)
			)
			.setRef(new PO850REF()
				.setReferenceIdQualifier("ZZ") // 1
				.setReferenceId("NCRT-Demo") // 2
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
						.setIdentificationCodeQualifier("91") // 3 - 91=assigned by seller
						.setIdentificationCode("FACILITY-DEMO-ID")
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
				.addN1Group(new PO850N1Entity()
					.setN1( new PO850N1()
						.setEntityCode("SE") // 1 - selling party
						.setName("MEDLINE") // 2 - name
						.setIdentificationCodeQualifier("91") // 3 - 91=assigned by seller
						.setIdentificationCode("MEDLINE-DEMO-ID")
					)
					.setN3(new PO850N3()
						.setAddressInfo1("1900 Meadowville Technology Pkwy")
					)
					.setN4(new PO850N4()
						.setCity("Chester")
						.setState("VA")
						.setZip("23836")
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
			.calculateSe();

		System.out.println(st.generateX12("^", "~\n"));
	}
	
	
}
