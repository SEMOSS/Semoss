package prerna.reactor.browser;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAllInputsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}
		String url = pbu.getUrl();
		Map inputs = pbu.getInputs();

		return new NounMetadata(inputs, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
