package prerna.util.edi.impl.ghx.po850.writer.n1loop;

import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;

public class PO850N1Loop implements IX12Format {

	private List<PO850N1Entity> n1list = new ArrayList<>();

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "";
		for(PO850N1Entity loop : n1list) {
			builder += loop.generateX12(elementDelimiter, segmentDelimiter);
		}
		return builder;
	}
	
	public PO850N1Loop addN1Group(PO850N1Entity n1) {
		n1list.add(n1);
		return this;
	}
	
	public List<PO850N1Entity> getN1List() {
		return this.n1list;
	}
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850N1Loop n1loop = new PO850N1Loop()
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
			;
	
		System.out.println(n1loop.generateX12("^", "~\n"));
	}
	
}