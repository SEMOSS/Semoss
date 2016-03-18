package prerna.solr;

import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.search.DisMaxQParserPlugin;
import org.apache.solr.search.QueryParsing;

public class SolrIndexEngineQueryBuilder {

	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngineQueryBuilder.class.getName());
	private SolrQuery Q;
	private String searchString;
	private boolean preFixSearch;
	
	public static final String DESC = "desc";
	public static final String ASC = "asc";
	public static final String SCORE = "score";
	
	public SolrIndexEngineQueryBuilder() {
		Q = new SolrQuery();
	}
	
	public SolrQuery getSolrQuery() {
		if(this.searchString == null || this.searchString.trim().isEmpty()) {
			this.Q.set(CommonParams.Q, SolrIndexEngine.QUERY_ALL);
		} else {
			if(preFixSearch) {
				this.Q.set(CommonParams.Q, this.searchString + "*");
			} else {
				this.Q.set(CommonParams.Q, this.searchString);
			}
		}
		
		return Q;
	}
	
	@Override
	public String toString() {
		return this.Q.toString();
	}
	
	public void setSearchString(String searchString) {
		this.searchString = escapeSpecialCharacters(searchString.trim());
	}
	
	public void setDefaultSearchField(String defaultField) {
		this.Q.set(CommonParams.DF, defaultField);
	}
	
	public void setQueryType(String queryType) {
		this.Q.set(CommonParams.QT, queryType);
	}
	
	public void setSort(String sortField, String sort) {
		switch (sort) {
			case ASC : this.Q.setSort(sortField, SolrQuery.ORDER.asc);
			break;
			case DESC : this.Q.setSort(sortField, SolrQuery.ORDER.desc);
			break;
		}
	}
	
	public void setFilterOptions(Map<String, List<String>> filterData) {
		for(String fieldName : filterData.keySet()) {
			List<String> filterValuesList = filterData.get(fieldName);
			StringBuilder filterStr = new StringBuilder();
			for (int i = 0; i < filterValuesList.size(); i++) {
				if (i == filterValuesList.size() - 1) {
					filterStr.append(filterValuesList.get(i));
				} else {
					filterStr.append(filterValuesList.get(i) + " OR ");
				}
			}
			this.Q.addFilterQuery(fieldName + ":" + "(" + filterStr.toString() + ")");
		}
	}

	public void setLimit(int limit) {
		this.Q.setRows(limit);
	}
	
	public void setOffset(int offset) {
		this.Q.setStart(offset);

	}

	public void addReturnFields(String field) {
		this.Q.addField(field);
	}
	
	public void setReturnFields(List<String> fields) {
		this.Q.setFields(fields.toArray(new String[]{}));
	}
	

	public void setFacet(boolean facet) {
		this.Q.setFacet(facet);
	}
	
	public void setFacetField(List<String> facetFields) {
		this.Q.addFacetField(facetFields.toArray(new String[] {}));	
	}
	
	public void setFacetMinCount(int facetMinCount) {
		this.Q.setFacetMinCount(facetMinCount);
	}
	
	public void setFacetSortCount(boolean facetSortCount) {
		this.Q.set(FacetParams.FACET_SORT_COUNT, facetSortCount);
	}
	
	public void setGroupBy(boolean groupBy) {
		this.Q.set(GroupParams.GROUP, groupBy);
	}
	
	public void setGroupFields(List<String> groupFields) {
		this.Q.set(GroupParams.GROUP_FIELD, groupFields.toArray(new String[] {}));
	}
	
	public void setGroupLimit(int groupLimit) {
		this.Q.set(GroupParams.GROUP_LIMIT, groupLimit);
	}
	
	public void setGroupOffset(int groupOffset) {
		Q.set(GroupParams.GROUP_OFFSET, groupOffset);
	}
	
	public void setGroupSort(String sortField, String sort) {
		switch (sort) {
			case ASC : this.Q.add(GroupParams.GROUP_SORT, sortField + " " + SolrQuery.ORDER.asc);
			break;
			case DESC : this.Q.add(GroupParams.GROUP_SORT, sortField + " " + SolrQuery.ORDER.desc);
			break;
		}
	}
	
	public void setMoreLikeThis(boolean moreLikeThis) {
		this.Q.set(MoreLikeThisParams.MLT, "true");
	}
	
	public void setMoreLikeThisFields(List<String> moreLikeThisFields) {
		Q.set(MoreLikeThisParams.SIMILARITY_FIELDS, moreLikeThisFields.toArray(new String[] {}));
	}
	
	// frequency words will be ignored if not included in set number of documents
	public void setMoreLikeThisDocFreq(int minDocFreq) {
		Q.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
	}
	
	// the frequency below which terms will be ignored in the source doc
	public void setMoreLikeThisMinTermFreq(int minTermFreq) {
		Q.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
	}

	public void setMoreLikeThisMinWordLength(int minWordLength) {
		this.Q.set(MoreLikeThisParams.MIN_WORD_LEN, minWordLength);
	}
	
	public void setMoreLikeThisMaxQueryTerms(int maxQueryTerms) {
		this.Q.set(MoreLikeThisParams.MAX_QUERY_TERMS, maxQueryTerms);
	}
	
	public void setMoreLikeThisDocCount(int docCount) {
		this.Q.set(MoreLikeThisParams.DOC_COUNT, docCount);
	}
	
	public void setSpellCheck(boolean spellCheck) {
		this.Q.set(SpellingParams.SPELLCHECK_PREFIX, spellCheck);
	}

	public void setSpellCheckBuild(boolean spellCheckBuild) {
		this.Q.set(SpellingParams.SPELLCHECK_BUILD, spellCheckBuild);
	}
	
	public void setSpellCheckCollate(boolean spellCheckCollate) {
		if(spellCheckCollate) {
			this.Q.set(SpellingParams.SPELLCHECK_COLLATE, "true");
		} else {
			this.Q.set(SpellingParams.SPELLCHECK_COLLATE, "false");
		}
	}
	
	public void setSpellCheckCollateExtendedResults(boolean spellCheckCollateExtendedResults) {
		if(spellCheckCollateExtendedResults) {
			this.Q.set(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true");
		} else {
			this.Q.set(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "false");
		}
	}
	
	public void setSpellCheckCount(int spellCheckCount) {
		this.Q.set(SpellingParams.SPELLCHECK_COUNT, "4");
	}

	public void setSpellCheckQuery(String spellCheckQuery) {
		this.Q.set(SpellingParams.SPELLCHECK_Q, spellCheckQuery);
	}
	
	public void removeSpellCheckParams() {
		this.Q.remove(SpellingParams.SPELLCHECK_PREFIX);
		this.Q.remove(SpellingParams.SPELLCHECK_BUILD);
		this.Q.remove(SpellingParams.SPELLCHECK_COLLATE);
		this.Q.remove(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS);
	}
	
	public void setPreFixSearch(boolean preFixSearch) {
		this.preFixSearch = preFixSearch;
	}
	
	public void addDisMax(String field, double power) {
		String currDisMax = this.Q.get(DisMaxParams.QF);
		if(currDisMax == null) {
			this.Q.set(DisMaxParams.QF, field + "^" + power);
		} else {
			currDisMax += " " + field + "^" + power;
			this.Q.set(DisMaxParams.QF, currDisMax);
		}
	}
	
	public void setDefType(String type) {
		this.Q.set(QueryParsing.DEFTYPE, type);
	}
	
	public void setPhraseBooster(String field) {
		this.Q.set(DisMaxParams.PF, field);
	}
	
	public void setDefaultDisMaxWeighting() {
		setDefType(DisMaxQParserPlugin.NAME);
		addDisMax(SolrIndexEngine.INDEX_NAME, 3.5);
		addDisMax(SolrIndexEngine.TAGS, 1.5);
		addDisMax(SolrIndexEngine.ENGINES, 1.0);
		addDisMax(SolrIndexEngine.PARAMS, 1.0);
		setPhraseBooster(SolrIndexEngine.INDEX_NAME);
	}
	
	public static String escapeSpecialCharacters(String s) {
		if(s.equals(SolrIndexEngine.QUERY_ALL)) {
			return s;
		}
		
		s = s.replace("\\", "\\\\");
		s = s.replace("+", "\\+");
		s = s.replace("-", "\\-");
		s = s.replace("&&", "\\&&");
		s = s.replace("||", "\\||");
		s = s.replace("!", "\\!");
		s = s.replace("(", "\\(");
		s = s.replace(")", "\\)");
		s = s.replace("{", "\\{");
		s = s.replace("}", "\\}");
		s = s.replace("[", "\\[");
		s = s.replace("]", "\\]");
		s = s.replace("^", "\\^");
		s = s.replace("~", "\\~");
		s = s.replace("*", "\\*");
		s = s.replace("?", "\\?");
		s = s.replace(":", "\\:");
		s = s.replace("\"", "\\\"");

		return s;
	}
	
}