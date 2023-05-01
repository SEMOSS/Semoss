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

}
