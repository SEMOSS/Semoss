//package prerna.solr;
//
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.common.params.CommonParams;
//import org.apache.solr.common.params.DisMaxParams;
//import org.apache.solr.common.params.FacetParams;
//import org.apache.solr.common.params.GroupParams;
//import org.apache.solr.common.params.MoreLikeThisParams;
//import org.apache.solr.common.params.SpellingParams;
//import org.apache.solr.search.DisMaxQParserPlugin;
//import org.apache.solr.search.QueryParsing;
//
//public class SolrIndexEngineQueryBuilder {
//
//	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngineQueryBuilder.class.getName());
//	private SolrQuery Q;
//	private String searchString;
//	
//	public static final String DESC = "desc";
//	public static final String ASC = "asc";
//	public static final String SCORE = "score";
//	
//	/**
//	 * Constructor create a SolrQuery object to set all the query parameters
//	 */
//	public SolrIndexEngineQueryBuilder() {
//		Q = new SolrQuery();
//	}
//	
//	/**
//	 * Returns the solr query object
//	 * @return
//	 */
//	public SolrQuery getSolrQuery() {
//		// default query is the query all which will return all results
//		if(this.searchString == null || this.searchString.isEmpty() || this.searchString.equals(SolrIndexEngine.QUERY_ALL)) {
//			this.Q.set(CommonParams.Q, SolrIndexEngine.QUERY_ALL);
//		} else {
//			// since regex needs to be added as special characters on the search string
//			// we append the search string at the end when we get the solr query so the user has an option
//			// to also set regex parameters
//			this.Q.set(CommonParams.Q, this.searchString);
////			this.Q.set(CommonParams.Q, "*" + this.searchString + "*");
//		}
//		
//		return Q;
//	}
//	
//	@Override
//	public String toString() {
//		return this.Q.toString();
//	}
//	
//	/**
//	 * Set the search string to be used in the query
//	 * @param searchString
//	 */
//	public void setSearchString(String searchString) {
//		this.searchString = escapeSpecialCharacters(searchString.trim());
//	}
//	
//	/**
//	 * Set the default search field to be used in the query
//	 * @param defaultField
//	 */
//	public void setDefaultSearchField(String defaultField) {
//		this.Q.set(CommonParams.DF, defaultField);
//	}
//	
//	/**
//	 * Sets the solr request handler for the search 
//	 * @param queryType
//	 */
//	public void setQueryType(String queryType) {
//		this.Q.set(CommonParams.QT, queryType);
//	}
//	
//	/**
//	 * Sets the sort as ascending or descending based on a specific field
//	 * @param sortField				The name of the field
//	 * @param sort					String containing either "asc" or "desc"
//	 */
//	public void setSort(String sortField, String sort) {
//		switch (sort) {
//			case ASC : this.Q.addSort(sortField, SolrQuery.ORDER.asc);
//			break;
//			case DESC : this.Q.addSort(sortField, SolrQuery.ORDER.desc);
//			break;
//		}
//	}
//	
//	/**
//	 * Appends the filter information onto the query
//	 * @param filterData				The filter values for the query.  The key in the map is the specific 
//	 * 									schema key and the list corresponds to the values to filter on.
//	 * 									Each entry in the list is a logical or with regards to the filter logic.
//	 * 									Example: {layout : [bar, pie] } means filter to show insights where the 
//	 * 									layout is either a bar OR pie chart 
//	 */
//	public void setFilterOptions(Map<String, List<String>> filterData) {
//		// loop through each field entry
//		// this corresponds to a schema field
//		// in the method example, the only fieldName is layout
//		for(String fieldName : filterData.keySet()) {
//			// grab the list of values for the field name
//			// in the above example, it is a list containing bar and pie
//			List<String> filterValuesList = filterData.get(fieldName);
//			StringBuilder filterStr = new StringBuilder();
//			// generate the filter string for the values
//			// this assumes an OR logic
//			// i.e. the filter will return things that are bar OR pie
//			int size = filterValuesList.size();
//			if(size == 1) {
//				String filterValue = filterValuesList.get(0);
//				if(filterValue.equals("*")) {
//					this.Q.addFilterQuery(fieldName + ":*");
//				} else {
//					this.Q.addFilterQuery(fieldName + ":" + "\"" + filterValuesList.get(0) + "\"");
//				}
//			} else {
//				for (int i = 0; i < filterValuesList.size(); i++) {
//					if (i == filterValuesList.size() - 1) {
//						filterStr.append( "\"" + filterValuesList.get(i) + "\"" );
//					} else {
//						filterStr.append( "\"" + filterValuesList.get(i) + "\""  + " OR ");
//					}
//				}
//				// set the filter
//				this.Q.addFilterQuery(fieldName + ":" + "(" + filterStr.toString() + ")");
//			}
//		}
//	}
//
//	/**
//	 * Set the limit for the query return
//	 * @param limit
//	 */
//	public void setLimit(int limit) {
//		this.Q.setRows(limit);
//	}
//	
//	/**
//	 * Set the offset for the query return
//	 * @param offset
//	 */
//	public void setOffset(int offset) {
//		this.Q.setStart(offset);
//
//	}
//
//	/**
//	 * Add a return field for the query to return
//	 * @param field
//	 */
//	public void addReturnFields(String field) {
//		this.Q.addField(field);
//	}
//	
//	/**
//	 * Set the list of return fields for the query to return
//	 * @param fields
//	 */
//	public void setReturnFields(List<String> fields) {
//		this.Q.setFields(fields.toArray(new String[]{}));
//	}
//	
//	/**
//	 * Set if the query should perform a facet
//	 * @param facet
//	 */
//	public void setFacet(boolean facet) {
//		this.Q.setFacet(facet);
//	}
//	
//	/**
//	 * Set the list of fields to facet
//	 * @param facetFields
//	 */
//	public void setFacetField(List<String> facetFields) {
//		this.Q.addFacetField(facetFields.toArray(new String[] {}));	
//	}
//	
//	/**
//	 * Set the minimum amount of records found in order for the facet to show
//	 * @param facetMinCount
//	 */
//	public void setFacetMinCount(int facetMinCount) {
//		this.Q.setFacetMinCount(facetMinCount);
//	}
//	
//	/**
//	 * Set a filter based on the number of found documents for the facet values
//	 * @param facetSortCount
//	 */
//	public void setFacetSortCount(boolean facetSortCount) {
//		this.Q.set(FacetParams.FACET_SORT_COUNT, facetSortCount);
//	}
//	
//	/**
//	 * Set if the query should perform a group by
//	 * @param groupBy
//	 */
//	public void setGroupBy(boolean groupBy) {
//		this.Q.set(GroupParams.GROUP, groupBy);
//	}
//	
//	/**
//	 * Set the group by fields for the query
//	 * @param groupFields
//	 */
//	public void setGroupFields(List<String> groupFields) {
//		this.Q.set(GroupParams.GROUP_FIELD, groupFields.toArray(new String[] {}));
//	}
//	
//	/**
//	 * Set the limit for the number of documents within each group
//	 * @param groupLimit
//	 */
//	public void setGroupLimit(int groupLimit) {
//		this.Q.set(GroupParams.GROUP_LIMIT, groupLimit);
//	}
//	
//	/**
//	 * Set the offset for the number of documents within each group
//	 * @param groupOffset
//	 */
//	public void setGroupOffset(int groupOffset) {
//		Q.set(GroupParams.GROUP_OFFSET, groupOffset);
//	}
//	
//	/**
//	 * Sets the sorting within each group as ascending or descending based on a specific field
//	 * @param sortField				The name of the field
//	 * @param sort					String containing either "asc" or "desc"
//	 */
//	public void setGroupSort(String sortField, String sort) {
//		switch (sort) {
//			case ASC : this.Q.add(GroupParams.GROUP_SORT, sortField + " " + SolrQuery.ORDER.asc);
//			break;
//			case DESC : this.Q.add(GroupParams.GROUP_SORT, sortField + " " + SolrQuery.ORDER.desc);
//			break;
//		}
//	}
//	
//	/**
//	 * Set if the query should return spell check response
//	 * @param spellCheck
//	 */
//	public void setSpellCheck(boolean spellCheck) {
//		this.Q.set(SpellingParams.SPELLCHECK_PREFIX, spellCheck);
//	}
//
//	/**
//	 * Set the boolean is the query should see if the spell check dictionary is built, if not, build it
//	 * @param spellCheckBuild
//	 */
//	public void setSpellCheckBuild(boolean spellCheckBuild) {
//		this.Q.set(SpellingParams.SPELLCHECK_BUILD, spellCheckBuild);
//	}
//	
//	/**
//	 * Set if the query should perform collation. A collation is the original query string used with the best
//	 * suggestions for each unidentified token (word) replaced in it.  
//	 * When set to true, solr will automatically use the best suggestion for each misspelled token and use that to execute the query
//	 * @param spellCheckCollate
//	 */
//	public void setSpellCheckCollate(boolean spellCheckCollate) {
//		if(spellCheckCollate) {
//			this.Q.set(SpellingParams.SPELLCHECK_COLLATE, "true");
//		} else {
//			this.Q.set(SpellingParams.SPELLCHECK_COLLATE, "false");
//		}
//	}
//	
//	/**
//	 * Sets if query response should detail extended information regarding each collation found.
//	 * If collate is false, this does nothing.
//	 * @param spellCheckCollateExtendedResults
//	 */
//	public void setSpellCheckCollateExtendedResults(boolean spellCheckCollateExtendedResults) {
//		if(spellCheckCollateExtendedResults) {
//			this.Q.set(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true");
//		} else {
//			this.Q.set(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "false");
//		}
//	}
//	
//	/**
//	 * Sets the maximum number of spelling corrections to show for each term
//	 * @param spellCheckCount
//	 */
//	public void setSpellCheckCount(int spellCheckCount) {
//		this.Q.set(SpellingParams.SPELLCHECK_COUNT, spellCheckCount);
//	}
//
//	/**
//	 * Sets the query to be used by the spellcheck
//	 * If not set, it automatically uses the search query
//	 * @param spellCheckQuery
//	 */
//	public void setSpellCheckQuery(String spellCheckQuery) {
//		this.Q.set(SpellingParams.SPELLCHECK_Q, spellCheckQuery);
//	}
//	
//	/**
//	 * Removes all the spell check parameters
//	 */
//	public void removeSpellCheckParams() {
//		this.Q.remove(SpellingParams.SPELLCHECK_PREFIX);
//		this.Q.remove(SpellingParams.SPELLCHECK_BUILD);
//		this.Q.remove(SpellingParams.SPELLCHECK_COLLATE);
//		this.Q.remove(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS);
//	}
//	
//	/**
//	 * Adds a field to the dismax calculation
//	 * @param field				The field to add to the calculation
//	 * @param power				The power to boost/reduce the field
//	 */
//	public void addDisMax(String field, double power) {
//		// dismax is a single string, so every time there is a dismax value added
//		// we need to concat it to the existing dismax values in the query if present
//		String currDisMax = this.Q.get(DisMaxParams.QF);
//		if(currDisMax == null) {
//			this.Q.set(DisMaxParams.QF, field + "^" + power);
//		} else {
//			currDisMax += " " + field + "^" + power;
//			this.Q.set(DisMaxParams.QF, currDisMax);
//		}
//	}
//	
//	/**
//	 * Sets the query to use a specific distance calculation
//	 * @param type
//	 */
//	public void setDefType(String type) {
//		this.Q.set(QueryParsing.DEFTYPE, type);
//	}
//	
//	/**
//	 * Sets a field to boost on when finding phrases that match the query search
//	 * @param field				The field to add the boost for
//	 */
//	public void setPhraseBooster(String field) {
//		this.Q.set(DisMaxParams.PF, field);
//	}
//	
//	/**
//	 * Sets a heuristic that seems to work well with determining what insights better match the search terms 
//	 * It uses the solr dismax similarity value which helps to score matches based on the position of terms found
//	 * with respect to the search string
//	 * The importance is based on raising specific fields to a power (if power > 1, it is a boost, if power < 1, it is a reduce
//	 * Currently, it is setting the following weighting: index_name ^3.5 + tags^1.5 + engine + params
//	 */
//	public void setDefaultDisMaxWeighting() {
//		setDefType(DisMaxQParserPlugin.NAME);
//		addDisMax(SolrIndexEngine.INDEX_NAME, 3.5);
//		addDisMax(SolrIndexEngine.INDEXED_DESCRIPTION, 2.0);
//		addDisMax(SolrIndexEngine.TAGS, 1.5);
//		addDisMax(SolrIndexEngine.INDEXED_TAGS, 1.5);
//		// we want to boost the question name field to increase the score when question name contains phrases that match
//		// with the query search 
//		setPhraseBooster(SolrIndexEngine.INDEX_NAME);
//	}
//	
//	/**
//	 * Escapes characters from the search string
//	 * As a result, the user cannot use regex in their query search
//	 * @param s				The input search string
//	 * @return				The search string escaping special characters
//	 */
//	public static String escapeSpecialCharacters(String s) {
//		if(s.equals(SolrIndexEngine.QUERY_ALL)) {
//			return s;
//		}
//		
//		s = s.replace("\\", "\\\\");
//		s = s.replace("+", "\\+");
//		s = s.replace("-", "\\-");
//		s = s.replace("&&", "\\&&");
//		s = s.replace("||", "\\||");
//		s = s.replace("!", "\\!");
//		s = s.replace("(", "\\(");
//		s = s.replace(")", "\\)");
//		s = s.replace("{", "\\{");
//		s = s.replace("}", "\\}");
//		s = s.replace("[", "\\[");
//		s = s.replace("]", "\\]");
//		s = s.replace("^", "\\^");
//		s = s.replace("~", "\\~");
//		s = s.replace("*", "\\*");
//		s = s.replace("?", "\\?");
//		s = s.replace(":", "\\:");
//		s = s.replace("\"", "\\\"");
//
//		return s;
//	}
//	
//	
//	///////////////////////////////// MLT LOGIC IS NOT CURRENTLY USED //////////////////////////////////
//	
//	/**
//	 * Set if the query should perform MLT
//	 * @param moreLikeThis
//	 */
//	public void setMoreLikeThis(boolean moreLikeThis) {
//		this.Q.set(MoreLikeThisParams.MLT, "true");
//	}
//	
//	/**
//	 * Sets the fields to be used in MLT
//	 * @param moreLikeThisFields
//	 */
//	public void setMoreLikeThisFields(List<String> moreLikeThisFields) {
//		Q.set(MoreLikeThisParams.SIMILARITY_FIELDS, moreLikeThisFields.toArray(new String[] {}));
//	}
//	
//	/**
//	 * Set the frequency of words that will be ignored if not included in set number of documents
//	 * @param minDocFreq
//	 */
//	public void setMoreLikeThisDocFreq(int minDocFreq) {
//		Q.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
//	}
//	
//	/**
//	 * Sets the frequency below which terms will be ignored in the source doc
//	 * @param minTermFreq
//	 */
//	public void setMoreLikeThisMinTermFreq(int minTermFreq) {
//		Q.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
//	}
//
//	/**
//	 * Sets the minimum word length for MLT
//	 * @param minWordLength
//	 */
//	public void setMoreLikeThisMinWordLength(int minWordLength) {
//		this.Q.set(MoreLikeThisParams.MIN_WORD_LEN, minWordLength);
//	}
//	
//	/**
//	 * Set the max query term for MLT
//	 * @param maxQueryTerms
//	 */
//	public void setMoreLikeThisMaxQueryTerms(int maxQueryTerms) {
//		this.Q.set(MoreLikeThisParams.MAX_QUERY_TERMS, maxQueryTerms);
//	}
//	
//	/**
//	 * Sets the number of documents to return for MLT
//	 * @param docCount
//	 */
//	public void setMoreLikeThisDocCount(int docCount) {
//		this.Q.set(MoreLikeThisParams.DOC_COUNT, docCount);
//	}
//	
//}