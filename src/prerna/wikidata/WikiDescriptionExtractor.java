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

import prerna.util.Utility;

public class WikiDescriptionExtractor {

	private static List<String> ignoreResultsList = new Vector<>();
	static {
		ignoreResultsList.add("Wikimedia disambiguation page");
		ignoreResultsList.add("scientific article published on");
	}

	public static final Logger LOGGER = LogManager.getLogger(WikiDescriptionExtractor.class);
	private Logger logger;

	public WikiDescriptionExtractor() {
		
	}
	
	public List<String> getDescriptions(String searchTerm) throws Exception {
		Logger logger = getLogger();
		List<String> descriptionList = new Vector<>();

		WikibaseDataFetcher wbdf = WikibaseDataFetcher.getWikidataDataFetcher();
		searchTerm = searchTerm.trim().replace("_", " ");
		List<WbSearchEntitiesResult> searchResults = wbdf.searchEntities(searchTerm, new Long(10));
		int numReturns = searchResults.size();
		if(numReturns == 0) {
			logger.info("Found no results searching for " + Utility.cleanLogString(searchTerm));
			return descriptionList;
		}
		logger.info("Querying wikidata returned " + numReturns + " results for " + Utility.cleanLogString(searchTerm));
		List<Callable<String>> descriptionExtractors = new Vector<>();
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
				if(foundDescription != null) {
					foundDescription = foundDescription.trim();
					if(!foundDescription.isEmpty()) {
						boolean ignoreDescription = false;;
						IGNORE_LOOP : for(String ignore : ignoreResultsList) {
							if(foundDescription.startsWith(ignore)) {
								ignoreDescription = true;
								break IGNORE_LOOP;
							}
						}
						if(!ignoreDescription) {
							descriptionList.add(foundDescription.trim());
						}
					}
				}
				numReturns--;
			} catch (Exception e) {
				logger.error("StackTrace: ", e);
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
