package prerna.wikidata;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wikidata.wdtk.wikibaseapi.WbSearchEntitiesResult;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataFetcher;

public class WikiDescriptionExtractor {

	public static final Logger LOGGER = LogManager.getLogger(WikiDescriptionExtractor.class.getName());
	private Logger logger;

	public WikiDescriptionExtractor() {
		
	}
	
	public List<String> getDescriptions(String searchTerm) throws Exception {
		Logger logger = getLogger();
		List<String> descriptionList = new Vector<String>();

		WikibaseDataFetcher wbdf = WikibaseDataFetcher.getWikidataDataFetcher();
		List<WbSearchEntitiesResult> searchResults = wbdf.searchEntities(searchTerm, new Long(10));
		int numReturns = searchResults.size();
		logger.info("Querying wikidata returned " + numReturns + " results");
		List<Callable<String>> descriptionExtractors = new Vector<Callable<String>>();
		for(int i = 0; i < searchResults.size(); i++) {
			WbSearchEntitiesResult res = searchResults.get(i);
			WikiDescriptionCallable callable = new WikiDescriptionCallable(wbdf, res);
			callable.setLogger(this.logger);
			descriptionExtractors.add(callable);
		}
		
		ExecutorService executorService = Executors.newFixedThreadPool(searchResults.size());
		CompletionService<String> completionService = new ExecutorCompletionService<>(executorService);
		for (Callable<String> descriptionExtractor : descriptionExtractors) {
			completionService.submit(descriptionExtractor);
		}
		
		// Continue until all have completed
		while (numReturns > 0) {
			try {
				String foundDescription = completionService.take().get();
				if(foundDescription != null && !foundDescription.trim().isEmpty()) {
					descriptionList.add(foundDescription.trim());
				}
				numReturns--;
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			} finally {
				executorService.shutdownNow();
			}
		}
		
		return descriptionList;
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
