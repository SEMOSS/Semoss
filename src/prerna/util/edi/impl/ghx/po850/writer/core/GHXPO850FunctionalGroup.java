package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IPO850FunctionalGroup;
import prerna.util.edi.IPO850GE;
import prerna.util.edi.IPO850GS;
import prerna.util.edi.IPO850TransactionSet;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850GE;
import prerna.util.edi.impl.ghx.po850.writer.heading.GHXPO850GS;
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
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;
import prerna.util.edi.po850.enums.PO850PO1QualifierIdEnum;

public class GHXPO850FunctionalGroup implements IPO850FunctionalGroup {

	private IPO850GS gs;
	private List<IPO850TransactionSet> stList = new ArrayList<>();
	private IPO850GE ge;

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = gs.generateX12(elementDelimiter, segmentDelimiter);
		for(IPO850TransactionSet st : stList) {
			builder += st.generateX12(elementDelimiter, segmentDelimiter);
		}
		builder += ge.generateX12(elementDelimiter, segmentDelimiter);
		
		return builder;
	}
	
	public GHXPO850FunctionalGroup calculateGe() {
		this.ge = new GHXPO850GE();
		this.ge.setNumberOfTransactions(this.stList.size()+"");
		this.ge.setGroupControlNumber(gs.getGs06());
		return this;
	}
	
	// get/set methods
	
	public IPO850GS getGs() {
		return gs;
	}
	
	public GHXPO850FunctionalGroup setGs(IPO850GS gs) {
		this.gs = gs;
		return this;
	}
	
	public IPO850GE getGe() {
		return ge;
	}
	
	public GHXPO850FunctionalGroup setGe(IPO850GE ge) {
		this.ge = ge;
		return this;
	}
	
	public GHXPO850FunctionalGroup addTransactionSet(IPO850TransactionSet st) {
		this.stList.add(st);
		return this;
	}
	

	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();

		GHXPO850FunctionalGroup fg = new GHXPO850FunctionalGroup()
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
				.calculateSe()
			)
			.calculateGe();

		
		System.out.println(fg.generateX12("^", "~\n"));
	}
	
}
