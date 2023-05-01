package prerna.util.edi.impl.ghx.po850.writer.po1loop;

import prerna.util.edi.IX12Format;

public class PO850PO1Entity implements IX12Format {

	private PO850PO1 po1;
	private PO850PID pid;
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = po1.generateX12(elementDelimiter, segmentDelimiter);
		if(pid != null) {
			builder += pid.generateX12(elementDelimiter, segmentDelimiter);
		}

		return builder;
	}

	public PO850PO1 getPo1() {
		return po1;
	}

	public PO850PO1Entity setPo1(PO850PO1 po1) {
		this.po1 = po1;
		return this;
	}

	public PO850PID getPid() {
		return pid;
	}

	public PO850PO1Entity setPid(PO850PID pid) {
		this.pid = pid;
		return this;
	}
	
	
	public static void main(String[] args) {
		PO850PO1Entity po1group = new PO850PO1Entity()
			.setPo1(new PO850PO1()
				.setUniqueId("1") // 1 - unique id 
				.setQuantityOrdered("10") // 2 - quantity ordered
				.setUnitOfMeasure("BX") // 3 - unit measurement
				.setUnitPrice("27.50") // 4 unit price (not total)
				.setProductId("BXTS1040") // product id
			)
			.setPid(new PO850PID()
				.setItemDescription("CARDINAL HEALTH� WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50")
			)
			;
			
		System.out.println(po1group.generateX12("^", "~\n"));
	}

}
