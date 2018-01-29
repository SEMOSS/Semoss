package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.NounMetadata;

public class UpdateInsightImageReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		String engineName = getEngine();
		String rdbmsId = getRdbmsId();
		String imageURL = this.getImageURL();
		if (engineName != null && rdbmsId != null && imageURL != null) {
			imageURL = imageURL.replace("<engine>", engineName);
			imageURL = imageURL.replace("<id>", rdbmsId+"");
			updateSolrImageByRecreatingInsight(rdbmsId + "", rdbmsId + "", imageURL, engineName);
		}
		return null;
	}

}
