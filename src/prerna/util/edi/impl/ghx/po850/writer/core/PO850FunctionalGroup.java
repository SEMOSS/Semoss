package prerna.util.edi.impl.ghx.po850.writer.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850BEG;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850GE;
import prerna.util.edi.impl.ghx.po850.writer.heading.PO850GS;
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

public class PO850FunctionalGroup implements IX12Format {

	private PO850GS gs;
	private PO850GE ge;
	
	private List<PO850TransactionSet> stList = new ArrayList<>();
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = gs.generateX12(elementDelimiter, segmentDelimiter);
		for(PO850TransactionSet st : stList) {
			builder += st.generateX12(elementDelimiter, segmentDelimiter);
		}
		builder += ge.generateX12(elementDelimiter, segmentDelimiter);
		
		return builder;
	}
	
	public PO850FunctionalGroup calculateGe() {
		this.ge = new PO850GE();
		this.ge.setNumberOfTransactions(this.stList.size()+"");
		this.ge.setGroupControlNumber(gs.getGs06());
		return this;
	}
	
	// get/set methods
	
	public PO850GS getGs() {
		return gs;
	}
	
	public PO850FunctionalGroup setGs(PO850GS gs) {
		this.gs = gs;
		return this;
	}
	
	public PO850GE getGe() {
		return ge;
	}
	
	public PO850FunctionalGroup setGe(PO850GE ge) {
		this.ge = ge;
		return this;
	}
	
	public PO850FunctionalGroup addTransactionSet(PO850TransactionSet st) {
		this.stList.add(st);
		return this;
	}
	

	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LocalDateTime now = LocalDateTime.now();

		PO850FunctionalGroup fg = new PO850FunctionalGroup()
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
				.calculateSe()
			)
			.calculateGe();

		
		System.out.println(fg.generateX12("^", "~\n"));
	}
	
}
