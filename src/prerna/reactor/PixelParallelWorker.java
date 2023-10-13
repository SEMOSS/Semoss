package prerna.reactor;

import prerna.om.Insight;

public class PixelParallelWorker implements IParallelWorker {

	private Insight insight;
	private String pixel;
	
	@Override
	public void setInisight(Insight insight) {
		this.insight = insight;
	}

	public void setPixel(String pixel) {
		this.pixel = pixel;
	}
	
	@Override
	public void run() {
		this.insight.runPixel(this.pixel);
	}

}
