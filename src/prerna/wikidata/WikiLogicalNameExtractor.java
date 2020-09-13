package prerna.wikidata;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wikidata.wdtk.wikibaseapi.WbSearchEntitiesResult;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataFetcher;

import prerna.util.Utility;

public class WikiLogicalNameExtractor {

	public static final Logger LOGGER = LogManager.getLogger(WikiLogicalNameExtractor.class);
	private Logger logger;

	public WikiLogicalNameExtractor() {
		
	}
	
	public List<String> getLogicalNames(String searchTerm) throws Exception {
		Logger logger = getLogger();
		List<String> logicalNames = new Vector<>();

		WikibaseDataFetcher wbdf = WikibaseDataFetcher.getWikidataDataFetcher();
		searchTerm = splitCamelCase(searchTerm.trim().replace("_", " ")).replaceAll("\\s+", " ");
		List<WbSearchEntitiesResult> searchResults = wbdf.searchEntities(searchTerm, new Long(10));
		int numReturns = searchResults.size();
		if(numReturns == 0) {
			logger.info("Found no results searching for " + Utility.cleanLogString(searchTerm));
			return logicalNames;
		}
		logger.info("Querying wikidata returned " + numReturns + " results for " + Utility.cleanLogString(searchTerm));
		List<Callable<List<String>>> logicalNamesExtractors = new Vector<>();
		for(int i = 0; i < searchResults.size(); i++) {
			WbSearchEntitiesResult res = searchResults.get(i);
			WikiLogicalNameCallable callable = new WikiLogicalNameCallable(wbdf, res);
			callable.setLogger(this.logger);
			logicalNamesExtractors.add(callable);
		}
		
		ExecutorService executorService = Executors.newFixedThreadPool(searchResults.size());
		CompletionService<List<String>> completionService = new ExecutorCompletionService<>(executorService);
		for (Callable<List<String>> logicalNameExtractor : logicalNamesExtractors) {
			completionService.submit(logicalNameExtractor);
		}
		
		// Continue until all have completed
		while (numReturns > 0) {
			try {
				List<String> foundLogicalNames = completionService.take().get();
				logicalNames.addAll(foundLogicalNames);
				numReturns--;
			} catch (Exception e) {
				logger.error("StackTrace: ", e);
				throw e;
			} finally {
				executorService.shutdownNow();
			}
		}
		
		return logicalNames;
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
	
	static String splitCamelCase(String s) {
		   return s.replaceAll(
		      String.format("%s|%s|%s",
		         "(?<=[A-Z])(?=[A-Z][a-z])",
		         "(?<=[^A-Z])(?=[A-Z])",
		         "(?<=[A-Za-z])(?=[^A-Za-z])"
		      ),
		      " "
		   );
		}
}
