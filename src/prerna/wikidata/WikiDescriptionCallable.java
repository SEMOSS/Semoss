package prerna.wikidata;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.wikibaseapi.WbSearchEntitiesResult;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataFetcher;

public class WikiDescriptionCallable implements Callable<String>  {

	public static final Logger LOGGER = LogManager.getLogger(WikiDescriptionCallable.class.getName());
	private Logger logger;
	
	WbSearchEntitiesResult res;
	WikibaseDataFetcher wbdf;
	
	public WikiDescriptionCallable(WikibaseDataFetcher wbdf, WbSearchEntitiesResult res) {
		this.wbdf = wbdf;
		this.res = res;
	}

	@Override
	public String call() throws Exception {
		return getDescription();
	}
	
	private String getDescription() throws Exception {
		Logger logger = getLogger();
		String description = null;
		
		String entityId = res.getEntityId();
		EntityDocument entity = wbdf.getEntityDocument(entityId);
		if(entity instanceof ItemDocument) {
			ItemDocument document = (ItemDocument) entity;
			
			String label = null;
			// for logging
			{
				Map<String, MonolingualTextValue> labels = document.getLabels();
				if(labels.get("en") != null) {
					label = labels.get("en").getText();
					logger.info("Processing document = " + label);
				}
			}
			
			Map<String, MonolingualTextValue> descMap = document.getDescriptions();
			if(descMap.get("en") != null) {
//				if(label != null) {
//					description = label + " = " + descMap.get("en").getText();
//				} else {
					description = descMap.get("en").getText();
//				}
			}
		}
		
		return description;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	/**
	 * Get the correct logger
	 * @return
	 */
	private Logger getLogger() {
		if(this.logger == null) {
			return LOGGER;
		}
		return this.logger;
	}
}
