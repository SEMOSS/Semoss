package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IPO850FunctionalGroup;
import prerna.util.edi.IPO850IEA;
import prerna.util.edi.IPO850ISA;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850GS;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850IEA;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850ISA;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850PER;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850REF;
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
import prerna.util.edi.po850.IPO850;
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;

public class GHXPO850 implements IPO850 {

	private String elementDelimiter = "^";
	private String segmentDelimiter = "~\n";
	
	// headers isa and ga
	private IPO850ISA isa;
	// list of functional groups
	private List<IPO850FunctionalGroup> functionalGroups = new ArrayList<>();
	// end isa
	private IPO850IEA iea;
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(elementDelimiter != null && !elementDelimiter.isEmpty()) {
			this.elementDelimiter = elementDelimiter;
		}
		if(segmentDelimiter != null && !segmentDelimiter.isEmpty()) {
			this.segmentDelimiter = segmentDelimiter;
		}
		
		String builder = isa.generateX12(elementDelimiter, segmentDelimiter);
		for(IPO850FunctionalGroup fg : functionalGroups) {
			builder += fg.generateX12(elementDelimiter, segmentDelimiter);
		}
		builder += iea.generateX12(elementDelimiter, segmentDelimiter);
		
		return builder;
	}
	
	// get/set methods
	
	public String getElementDelimiter() {
		return elementDelimiter;
	}

	public GHXPO850 setElementDelimiter(String elementDelimiter) {
		this.elementDelimiter = elementDelimiter;
		return this;
	}

	public String getSegmentDelimiter() {
		return segmentDelimiter;
	}

	public GHXPO850 setSegmentDelimiter(String segmentDelimiter) {
		this.segmentDelimiter = segmentDelimiter;
		return this;
	}
	
	public IPO850ISA getIsa() {
		return isa;
	}
	
	public GHXPO850 setIsa(IPO850ISA isa) {
		this.isa = isa;
		return this;
	}
	
	public IPO850IEA getIea() {
		return iea;
	}
	
	public GHXPO850 setIea(IPO850IEA iea) {
		this.iea = iea;
		return this;
	}
	
	public GHXPO850 addFunctionalGroup(IPO850FunctionalGroup fg) {
		this.functionalGroups.add(fg);
		return this;
	}
	
	public List<IPO850FunctionalGroup> getFunctionalGroups() {
		return this.functionalGroups;
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();
		GHXPO850 po850 = new GHXPO850();
		po850.setIsa(new GHXPO850ISA()
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
				.addFunctionalGroup(new GHXPO850FunctionalGroup()
					.setGs(new GHXPO850GS()
						.setSenderId("SENDER1234") // 2
						.setReceiverId("RECEIVER12") // 3
						.setDateAndTime(now) // 4, 5
						.setGroupControlNumber("001") // 6
					)
					.addTransactionSet(new GHXPO850TransactionSet()
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
							.setReferenceId("NCRT-Demo")
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
									.setQuantityOrdered("10") // 2 - quantity ordered
									.setUnitOfMeasure("BX") // 3 - unit measurement
									.setUnitPrice("27.50") // 4 unit price (not total)
									.setProductId("BXTS1040") // product id
								)
								.setPid(new PO850PID()
									.setItemDescription("CARDINAL HEALTH WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50")
								)
							)
						)
						.calculateSe()
					)
					.calculateGe()
				)
				.setIea(new GHXPO850IEA()
					.setTotalGroups(po850.getFunctionalGroups().size()+"")
					.setInterchangeControlNumber(po850.getIsa().getIsa13()) // 2 - IEA02 = ISA13
				)
			;
		
		System.out.println(po850.generateX12("^", "~\n"));
	}

}
