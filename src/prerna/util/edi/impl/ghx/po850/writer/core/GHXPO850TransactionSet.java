package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;
import java.util.List;

import prerna.util.edi.IPO850CTT;
import prerna.util.edi.IPO850SE;
import prerna.util.edi.IPO850ST;
import prerna.util.edi.IPO850TransactionSet;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850CTT;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850PER;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850REF;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850SE;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850ST;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1Entity;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N1Loop;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N3;
import prerna.util.edi.impl.ghx.po850.writer.loop.n1loop.PO850N4;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PID;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1Entity;
import prerna.util.edi.impl.ghx.po850.writer.loop.po1loop.PO850PO1Loop;
import prerna.util.edi.po850.IPO850BEG;
import prerna.util.edi.po850.IPO850PER;
import prerna.util.edi.po850.IPO850REF;
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;
import prerna.util.edi.po850.enums.PO850PO1QualifierIdEnum;

public class GHXPO850TransactionSet implements IPO850TransactionSet {

	// start transaction
	private IPO850ST st;
	
	// beginning segment
	private IPO850BEG beg;
	// reference identification
	private IPO850REF ref;
	// reference identification
	private IPO850PER per;
	
	// n1 loop
	private PO850N1Loop n1loop;
	// po1 loop
	private PO850PO1Loop po1loop;
	
	// transaction totals
	private IPO850CTT ctt;
	// end transaction 
	private IPO850SE se;

	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = st.generateX12(elementDelimiter, segmentDelimiter)
			+ beg.generateX12(elementDelimiter, segmentDelimiter)
			+ ref.generateX12(elementDelimiter, segmentDelimiter)
			+ per.generateX12(elementDelimiter, segmentDelimiter)
			+ n1loop.generateX12(elementDelimiter, segmentDelimiter)
			+ po1loop.generateX12(elementDelimiter, segmentDelimiter)
			+ ctt.generateX12(elementDelimiter, segmentDelimiter)
			+ se.generateX12(elementDelimiter, segmentDelimiter)
			;
		
		return builder;
	}
	
	public GHXPO850TransactionSet calculateSe() {
		this.se = new GHXPO850SE();
		this.se.setTotalSegments(
				(
					1 // sf
					+ 1 // beg
					+ 1 // ref
					+ 1 // per
					+ n1loop.getNumSegments()
					+ po1loop.getNumSegments()
					+ 1 // ctt
					+ 1 // se
				)
				+ ""
		);
		this.se.setTransactionSetControlNumber(st.getSt02());
		return this;
	}
	
	public GHXPO850TransactionSet calculateCtt() {
		List<PO850PO1Entity> po1List = po1loop.getPO1List();
		int numPo1 = po1List.size();
		int sumPo101 = 0;
		
		for(PO850PO1Entity po1 : po1List) {
			PO850PO1 poi = po1.getPo1();
			sumPo101 += poi.getPo102();
		}
		
		this.ctt = new GHXPO850CTT()
				.setNumPO1Segments(numPo1)
				.setSumPO102Qualities(sumPo101);
		return this;
	}
	
	// get/set methods

	public IPO850ST getSt() {
		return st;
	}
	
	public GHXPO850TransactionSet setSt(IPO850ST st) {
		this.st = st;
		return this;
	}
	
	public IPO850BEG getBeg() {
		return beg;
	}
	
	public GHXPO850TransactionSet setBeg(IPO850BEG beg) {
		this.beg = beg;
		return this;
	}
	
	public IPO850REF getRef() {
		return ref;
	}
	
	public GHXPO850TransactionSet setRef(IPO850REF ref) {
		this.ref = ref;
		return this;
	}
	
	public IPO850PER getPer() {
		return per;
	}
	
	public GHXPO850TransactionSet setPer(IPO850PER per) {
		this.per = per;
		return this;
	}
	
	public PO850N1Loop getN1loop() {
		return n1loop;
	}
	
	public GHXPO850TransactionSet setN1loop(PO850N1Loop n1loop) {
		this.n1loop = n1loop;
		return this;
	}
	
	public PO850PO1Loop getPo1loop() {
		return po1loop;
	}
	
	public GHXPO850TransactionSet setPo1loop(PO850PO1Loop po1loop) {
		this.po1loop = po1loop;
		return this;
	}
	
	public IPO850SE getSe() {
		return se;
	}
	
	public GHXPO850TransactionSet setSe(IPO850SE se) {
		this.se = se;
		return this;
	}
	
	
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();

		GHXPO850TransactionSet st = new GHXPO850TransactionSet()
			.setSt(new GHXPO850ST()
				.setTransactionSetControlNumber("0001")
			)
			.setBeg(new GHXPO850BEG()
				.setPurchaseOrderTypeCode(PO850BEGQualifierIdEnum.NE)
				.setTransactionPurposeCode("00") // 1
				.setPurchaseOrderNumber("RequestID") // 3
				.setDateAndTime(now)
			)
			.setRef(new GHXPO850REF()
				.setReferenceIdQualifier("ZZ") // 1
				.setReferenceId("NCRT-Demo") // 2
			)
			.setPer(new GHXPO850PER()
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
						.setQuantityOrdered(10) // 2 - quantity ordered
						.setUnitOfMeasure("BX") // 3 - unit measurement
						.setUnitPrice(27.50) // 4 unit price (not total)
						.addQualifierAndValue(PO850PO1QualifierIdEnum.VC, "BXTS1040")
						.addQualifierAndValue(PO850PO1QualifierIdEnum.IN, "299176")
					)
					.setPid(new PO850PID()
						.setItemDescription("CARDINAL HEALTH™ WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50")
					)
				)
				.addPO1(new PO850PO1Entity()
						.setPo1(new PO850PO1()
							.setUniqueId("2") // 1 - unique id 
							.setQuantityOrdered(25) // 2 - quantity ordered
							.setUnitOfMeasure("BX") // 3 - unit measurement
							.setUnitPrice(260.27) // 4 unit price (not total)
							.addQualifierAndValue(PO850PO1QualifierIdEnum.VC, "VMRM1535")
							.addQualifierAndValue(PO850PO1QualifierIdEnum.IN, "299188")
						)
					.setPid(new PO850PID()
						.setItemDescription("PIN SKULL ADULT DISPO SS 3/PK 12/BX, FOB (Destination), Manufacturer (BECTON, DICKINSON AND COMPANY); BOX of 12")
					)
				)
			)
			.calculateCtt()
			.calculateSe();

		System.out.println(st.generateX12("^", "~\n"));
	}
	
	
}
