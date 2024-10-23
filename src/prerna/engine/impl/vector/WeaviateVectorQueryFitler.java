package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.weaviate.client.v1.filters.Operator;
import io.weaviate.client.v1.filters.WhereFilter;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class WeaviateVectorQueryFitler {

	private static final Logger classLogger = LogManager.getLogger(WeaviateVectorQueryFitler.class);

	public static String[] checkModalityFilters(List<IQueryFilter> filters, String column) {
		String[] filterSyntax = null;
		for (IQueryFilter filter : filters) {
			filterSyntax = processModalityFilter(filter, column);

		}
		return filterSyntax;
	}

	private static String[] processModalityFilter(IQueryFilter filter, String column) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilterForModality((SimpleQueryFilter) filter);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilterforModality((AndQueryFilter) filter, column);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilterforModality((OrQueryFilter) filter, column);
		}
		return null;
	}

	private static String[] processOrQueryFilterforModality(OrQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}
		
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToModalityValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processAndQueryFilterforModality(AndQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}
		
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToModalityValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processSimpleQueryFilterForModality(SimpleQueryFilter filter) {
		NounMetadata rightComp = filter.getRComparison();

		FILTER_TYPE fType = filter.getSimpleFilterType();

		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToModalityValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] addSelectorToModalityValuesFilter(NounMetadata rightComp) {

		String rightSelector = null;
		Vector<String> rs = new Vector<>();
		String[] result = new String[1];

		if (!(rightComp.getValue().toString().contains("["))) {
			rightSelector = (String) rightComp.getValue();
			result[0] = rightSelector;

		} else {
			rs = (Vector<String>) rightComp.getValue();
			result = new String[rs.size()];
			for (int i = 0; i < rs.size(); i++) {
				result[i] = rs.get(i);
			}
		}

		return result;
	}

	public static List<String> addFilters(List<IQueryFilter> filters) {
		String ERROR = "Unable to generate filter";
		List<String> filterStatements = new ArrayList<>();

		for (IQueryFilter filter : filters) {
			List<String> filterSyntax = processFilter(filter);
			if (filterSyntax != null) {
				for (int i = 0; i < filterSyntax.size(); i++) {
					filterStatements.add(filterSyntax.get(i));
				}
			}
		}
		if (filterStatements.size() == 0) {
			throw new IllegalArgumentException(ERROR);
		}
		return filterStatements;
	}

	public static String checkFilters(List<IQueryFilter> filters) {
		String filterType = "";
		for (IQueryFilter filter : filters) {
			filterType = checkFilterType(filter);
		}
		return filterType;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	private static List<String> processFilter(IQueryFilter filter) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter);
		}
		return null;
	}

	private static String checkFilterType(IQueryFilter filter) {

		String AND = "And";
		String OR = "Or";
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return AND;
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return AND;
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return OR;
		}
		return null;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected static List<String> processOrQueryFilter(OrQueryFilter filter) {
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		List<String> filterBuilder = new ArrayList<String>();
		List<String> AndList = new ArrayList<String>();
		for (int i = 0; i < numAnds; i++) {
			filterBuilder = processFilter(filterList.get(i));
			AndList.addAll(processFilter(filterList.get(i)));
		}
		return AndList;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected static List<String> processAndQueryFilter(AndQueryFilter filter) {

		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		List<String> filterBuilder = new ArrayList<String>();
		List<String> AndList = new ArrayList<String>();
		for (int i = 0; i < numAnds; i++) {
			filterBuilder = processFilter(filterList.get(i));
			AndList.addAll(processFilter(filterList.get(i)));
		}

		return AndList;
	}

	protected static String[] processAndQueryFilterforSource(AndQueryFilter filter, String column) {

		List<IQueryFilter> filterList = filter.getFilterList();
		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))

				rightComp = filterSimple.getRComparison();
		}

		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToSourceValuesFilter(rightComp);
		}
		return null;

	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected static List<String> processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		List<String> results = new ArrayList<String>();
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			String op = addSelectorToValuesFilter(leftComp, thisComparator);
			results.add(op);
			return (List<String>) results;
		}
		return null;
	}

	protected static String[] processSimpleQueryFilterForSource(SimpleQueryFilter filter) {
		NounMetadata rightComp = filter.getRComparison();

		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToSourceValuesFilter(rightComp);
		}
		return null;
	}

	/**
	 * Add filter for a column to values
	 * 
	 * @param filters
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected static String addSelectorToValuesFilter(NounMetadata leftComp, String thisComparator) {
		String leftresult = "";
		String SOURCE = "Source";
		String MODALITY = "Modality";
		String DIVIDER = "divider";
		String PART = "part";

		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftDataType = leftSelector.getDataType();

		if (leftDataType == null) {
			String leftConceptProperty = leftSelector.getQueryStructName();

			if (leftConceptProperty.equalsIgnoreCase(SOURCE)) {
				leftresult += leftConceptProperty;
			}
			if (leftConceptProperty.equalsIgnoreCase(MODALITY)) {
				leftresult += leftConceptProperty;
			}
			if (leftConceptProperty.equalsIgnoreCase(DIVIDER)) {
				leftresult += leftConceptProperty;
			}
			if (leftConceptProperty.equalsIgnoreCase(PART)) {
				leftresult += leftConceptProperty;
			}
		}

		return leftresult;
	}

	protected static String[] addSelectorToSourceValuesFilter(NounMetadata rightComp) {

		String rightSelector = null;
		Vector<String> rs = new Vector<>();
		String[] result = new String[1];

		if (!(rightComp.getValue().toString().contains("["))) {
			rightSelector = (String) rightComp.getValue();

			result[0] = rightSelector;

		} else {
			rs = (Vector<String>) rightComp.getValue();
			result = new String[rs.size()];
			for (int i = 0; i < rs.size(); i++) {
				result[i] = rs.get(i);
			}
		}

		return result;

	}

	public static String[] checkSourceFilters(List<IQueryFilter> filters, String column) {
		String[] filterSyntax = null;
		for (IQueryFilter filter : filters) {
			filterSyntax = processSourceFilter(filter, column);
		}
		return filterSyntax;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	private static String[] processSourceFilter(IQueryFilter filter, String column) {

		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilterForSource((SimpleQueryFilter) filter);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilterforSource((AndQueryFilter) filter, column);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilterforSource((OrQueryFilter) filter, column);
		}
		return null;
	}

	private static String[] processOrQueryFilterforSource(OrQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();
		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}

		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToSourceValuesFilter(rightComp);
		}
		return null;
	}

	public static String[] checkDividerFilters(List<IQueryFilter> filters, String column) {
		String[] filterSyntax = null;
		for (IQueryFilter filter : filters) {
			filterSyntax = processDividerFilter(filter, column);
		}
		return filterSyntax;
	}

	private static String[] processDividerFilter(IQueryFilter filter, String column) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilterForDivider((SimpleQueryFilter) filter);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilterForDivider((AndQueryFilter) filter, column);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilterForDivider((OrQueryFilter) filter, column);
		}
		return null;

	}

	private static String[] processOrQueryFilterForDivider(OrQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToDividerValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processAndQueryFilterForDivider(AndQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToDividerValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processSimpleQueryFilterForDivider(SimpleQueryFilter filter) {
		NounMetadata rightComp = filter.getRComparison();

		FILTER_TYPE fType = filter.getSimpleFilterType();

		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToDividerValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] addSelectorToDividerValuesFilter(NounMetadata rightComp) {
		String rightSelector = null;
		Vector<String> rs = new Vector<>();
		String[] result = new String[1];

		if (!(rightComp.getValue().toString().contains("["))) {
			rightSelector = (String) rightComp.getValue();
			result[0] = rightSelector;

		} else {
			rs = (Vector<String>) rightComp.getValue();
			result = new String[rs.size()];
			for (int i = 0; i < rs.size(); i++) {
				result[i] = rs.get(i);
			}
		}

		return result;
	}

	public static String[] checkPartFilters(List<IQueryFilter> filters, String column) {
		String[] filterSyntax = null;
		for (IQueryFilter filter : filters) {
			filterSyntax = processPartFilter(filter, column);
		}
		return filterSyntax;
	}

	private static String[] processPartFilter(IQueryFilter filter, String column) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilterForPart((SimpleQueryFilter) filter);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilterForPart((AndQueryFilter) filter, column);
		}
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilterForPart((OrQueryFilter) filter, column);
		}
		return null;
	}

	private static String[] processOrQueryFilterForPart(OrQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))

				rightComp = filterSimple.getRComparison();
		}
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToPartValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processAndQueryFilterForPart(AndQueryFilter filter, String column) {
		List<IQueryFilter> filterList = filter.getFilterList();

		SimpleQueryFilter filterSimple = null;
		NounMetadata rightComp = null;
		for (int i = 0; i < filterList.size(); i++) {
			filterSimple = (SimpleQueryFilter) filterList.get(i);
			if (filterSimple.getLComparison().toString().contains(column))
				rightComp = filterSimple.getRComparison();
		}
		FILTER_TYPE fType = filterSimple.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToPartValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] processSimpleQueryFilterForPart(SimpleQueryFilter filter) {
		NounMetadata rightComp = filter.getRComparison();

		FILTER_TYPE fType = filter.getSimpleFilterType();

		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToPartValuesFilter(rightComp);
		}
		return null;
	}

	private static String[] addSelectorToPartValuesFilter(NounMetadata rightComp) {
		String rightSelector = null;
		Vector<String> rs = new Vector<>();
		String[] result = new String[1];

		if (!(rightComp.getValue().toString().contains("["))) {
			rightSelector = (String) rightComp.getValue();
			result[0] = rightSelector;

		} else {
			rs = (Vector<String>) rightComp.getValue();
			result = new String[rs.size()];
			for (int i = 0; i < rs.size(); i++) {
				result[i] = rs.get(i);
			}
		}

		return result;
	}

	public static WhereFilter[] addSearchFilters(List<IQueryFilter> filters, List<String> searchFilters,
			String operationType) {

		WhereFilter whereSource = null;
		WhereFilter whereModality = null;
		WhereFilter whereDivider = null;
		WhereFilter wherePart = null;

		String[] filesList = null;
		String[] modalityList = null;
		String[] dividerList = null;
		String[] partList = null;
		
		int size = 0;
		String SOURCE = "source";
		String MODALITY = "modality";
		String DIVIDER = "divider";
		String PART = "part";

		String errorMsg = "Column value is not valid.Please pass valid column values,i.e; Source,Modality,Divider and Part";

		for (int i = 0; i < searchFilters.size(); i++) {
			if (searchFilters.get(i).equalsIgnoreCase(SOURCE)) {

				filesList = WeaviateVectorQueryFitler.checkSourceFilters(filters, searchFilters.get(i));
				size++;

			}
			if (searchFilters.get(i).equalsIgnoreCase(MODALITY)) {

				modalityList = WeaviateVectorQueryFitler.checkModalityFilters(filters, searchFilters.get(i));
				size++;

			}
			if (searchFilters.get(i).equalsIgnoreCase(DIVIDER)) {

				dividerList = WeaviateVectorQueryFitler.checkDividerFilters(filters, searchFilters.get(i));
				size++;

			}
			if (searchFilters.get(i).equalsIgnoreCase(PART)) {

				partList = WeaviateVectorQueryFitler.checkPartFilters(filters, searchFilters.get(i));
				size++;

			}
		}
		if (size == 0) {
			classLogger.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);

		}

		WhereFilter[] whereCheck = new WhereFilter[size];
		int i = 0;

		if (filesList != null) {
			whereSource = WhereFilter.builder().path(SOURCE).operator(Operator.ContainsAny).valueText(filesList)
					.build();
			whereCheck[i] = whereSource;
			i++;
		}

		if (modalityList != null) {
			whereModality = WhereFilter.builder().path(MODALITY).operator(Operator.ContainsAny).valueText(modalityList)
					.build();
			whereCheck[i] = whereModality;
			i++;
		}

		if (dividerList != null) {
			whereDivider = WhereFilter.builder().path(DIVIDER).operator(Operator.ContainsAny).valueText(dividerList)
					.build();
			whereCheck[i] = whereDivider;
			i++;
		}

		if (partList != null) {
			wherePart = WhereFilter.builder().path(PART).operator(Operator.ContainsAny).valueText(partList).build();
			whereCheck[i] = wherePart;
			i++;
		}

		return whereCheck;

	}

	public static String checkOperationType(boolean flag1, boolean flag2, String operationType1,
			String operationType2) {
		String operationType = null;
		String AND = "And";
		String errorMessage = "Operation type is not valid";

		if (flag1 && flag2 && operationType1 == null && operationType2 == null) {
			operationType = AND;
		}

		else if (flag1 && flag2 && operationType1 != null && operationType2 == null) {
			operationType = operationType1;
		}

		else if (flag1 && flag2 && operationType2 != null && operationType1 == null) {
			operationType = operationType2;
		}

		else if (flag1 && flag2 && operationType2 != null && operationType1 != null
				&& operationType1 == operationType2) {
			operationType = operationType2;
		} else if (flag1 && !flag2) {
			operationType = operationType1;
		} else if (!flag1 && flag2) {
			operationType = operationType2;
		} else
			throw new SemossPixelException(errorMessage);

		return operationType;
	}
}
