package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850GS;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850IEA;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850ISA;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850PER;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850REF;
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

public class PO850 implements IX12Format {

	private String elementDelimiter = "^";
	private String segmentDelimiter = "~\n";
	
	// headers isa and ga
	private PO850ISA isa;
	
	// list of functional groups
	private List<PO850FunctionalGroup> functionalGroups = new ArrayList<>();
	
	// end isa
	private PO850IEA iea;
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(elementDelimiter != null && !elementDelimiter.isEmpty()) {
			this.elementDelimiter = elementDelimiter;
		}
		if(segmentDelimiter != null && !segmentDelimiter.isEmpty()) {
			this.segmentDelimiter = segmentDelimiter;
		}
		
		String builder = isa.generateX12(elementDelimiter, segmentDelimiter);
		for(PO850FunctionalGroup fg : functionalGroups) {
			builder += fg.generateX12(elementDelimiter, segmentDelimiter);
		}
		builder += iea.generateX12(elementDelimiter, segmentDelimiter);
		
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
	
	public PO850 addFunctionalGroup(PO850FunctionalGroup fg) {
		this.functionalGroups.add(fg);
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
		PO850 po850 = new PO850();
		po850.setIsa(new PO850ISA()
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
				.addFunctionalGroup(new PO850FunctionalGroup()
					.setGs(new PO850GS()
						.setSenderId("SENDER1234") // 2
						.setReceiverId("RECEIVER12") // 3
						.setDateAndTime(now) // 4, 5
						.setGroupControlNumber("001") // 6
					)
					.addTransactionSet(new PO850TransactionSet()
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
							.setReferenceId("NCRT-Demo")
						)
						.setPer(new PO850PER()
								.setContactFunctionCode("NE") // 1 - NE=NewOrder, BD=Bidding
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
				.setIea(new PO850IEA()
					.setInterchangeControlNumber(po850.getIsa().getIsa13()) // 2 - IEA02 = ISA13
				)
			;
		
		System.out.println(po850.generateX12("^", "~\n"));
	}

}
