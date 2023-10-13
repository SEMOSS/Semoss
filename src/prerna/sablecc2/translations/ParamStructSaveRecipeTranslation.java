package prerna.sablecc2.translations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.PARAMETER_FILL_TYPE;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QsFilterParameterizeConverter2;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.reactor.IReactor;
import prerna.reactor.imports.ImportReactor;
import prerna.reactor.imports.MergeReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.reactor.qs.source.FileReadReactor;
import prerna.reactor.qs.source.FrameReactor;
import prerna.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.reactor.qs.source.JdbcSourceReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ParamStructSaveRecipeTranslation extends LazyTranslation {
	
	private static final Logger logger = LogManager.getLogger(ParamStructSaveRecipeTranslation.class);

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<>();
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public List<String> encodingList = new Vector<>();
	public HashMap<String, String> encodedToOriginal = new HashMap<>();
	
	// set the parameters we care about
	public List<ParamStruct> paramStructs;
	
	private SelectQueryStruct importQs;
	private String sourceStr;
	private String importStr;
	
	public String currentPixelId = "";
	
	public ParamStructSaveRecipeTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		ROUTINE_LOOP : for(PRoutine e : copy) {
			String expression = e.toString();
			if(expression.contains("Import")) {
        		this.resultKey = "$RESULT_" + e.hashCode();

				logger.info("Processing " + Utility.cleanLogString(expression));
				e.apply(this);

				// check if we have a QS to modify
				if(this.importQs == null) {
					// just store
					// add to list of expressions
					expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
					this.pixels.add(expression);
				} else {
					// we have a QS
					
					// first, need to see if this import requires
					// loop through all the params
					// and see if this pixelId is included
					// and which params are required to be applied
					List<ParamStructDetails> thisImportParams = new ArrayList<>();
					Map<ParamStructDetails, ParamStruct> detailsLookup = new HashMap<>();
					for(ParamStruct struct  : this.paramStructs) {
						for(ParamStructDetails details : struct.getDetailsList()) {
							if(details.getPixelId().equals(this.currentPixelId)) {
								// store this
								thisImportParams.add(details);
								detailsLookup.put(details, struct);
							}
						}
					}
					
					// if no matches
					// nothing to do
					if(thisImportParams.isEmpty()) {
						this.pixels.add(expression);
						continue;
					}
					
					// we have something
					// now we need to see
					// if this is pre-existing
					// or something new
					// this will follow 2 different flows 
					// based on if it is hqs or sqs
					if(this.importQs instanceof HardSelectQueryStruct) {
						logger.info("Parameterizing hard query struct");
						
						HardSelectQueryStruct hqs = (HardSelectQueryStruct) this.importQs;
						String query = hqs.getQuery();
						String finalQuery = null;
						try {
							finalQuery = GenExpressionWrapper.transformQueryWithParams(query, thisImportParams, detailsLookup);
						} catch (Exception e1) {
							logger.error(Constants.STACKTRACE, e1);
							// add to list of expressions
							expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
							this.pixels.add(expression);
							continue ROUTINE_LOOP;
						}
						
						ParamStructDetails datasoucreParameter = getDatasourceParameter(thisImportParams);
						String newExpr = null;
						if(datasoucreParameter != null) {
							ParamStruct paramStruct = detailsLookup.get(datasoucreParameter);
							// assume Database()
							newExpr = "Database(<\"" + paramStruct.getParamName() + ">\")";
						} else {
							newExpr = this.sourceStr;
						}
						newExpr += " | Query(\"<encode>" + finalQuery + "</encode>\") | " + this.importStr + ";";
						this.pixels.add(newExpr);
						
					} else {
						logger.info("Parameterizing pixel select query struct");
						
						// now we will look for if this is a filter already applied in the import
						// or a new filter we are adding
						
						List<IQueryFilter> newFilters = new Vector<>();

						// you can have a filter
						// that is not a selector
						// so lets look at filters as well
						List<Integer> paramIndexFound = new Vector<>();
						List<IQueryFilter> currentImportFilters = importQs.getExplicitFilters().getFilters();
						for(IQueryFilter f : currentImportFilters) {
							IQueryFilter modification = f;
							// i will want to run through EVERY column
							// for the same filter
							// in case it contains it
							for(int i = 0; i < thisImportParams.size(); i++) {
								// we need to do this based on the level
								ParamStructDetails importParam = thisImportParams.get(i);
								if(importParam.getParameterFillType() == PARAMETER_FILL_TYPE.FILTER) {
									ParamStruct paramStruct = detailsLookup.get(importParam);
									modification = QsFilterParameterizeConverter2.modifyFilter(modification, importParam, paramStruct);
									// if we have returned a new filter object
									// that means it has been modified
									if(modification != f) {
										paramIndexFound.add(i);
										// switch the reference of f in this example
										// so that not all the param index get added
										f = modification;
									}
								}
							}
							
							// add the modified filter if it was changed
							newFilters.add(modification);
						}
						
						// FE prefers a single AND block
						// so adding all the current filters
						// note - this maybe 0 or more
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(newFilters);
						
						for(int i = 0; i < thisImportParams.size(); i++) {
							if(paramIndexFound.contains(new Integer(i))) {
								continue;
							}
							
							// this is a param we have not found yet
							// add new filters
							ParamStructDetails importParam = thisImportParams.get(i);
							if(importParam.getParameterFillType() == PARAMETER_FILL_TYPE.FILTER) {

								ParamStruct pStruct = detailsLookup.get(importParam);
								String comparator = importParam.getOperator();
								if(comparator == null || comparator.isEmpty()) {
									comparator = "==";
								}
								// this is the replacement
								String replacement = null;
								if(ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(pStruct.getModelDisplay()) 
										|| importParam.getQuote() == QUOTE.NO) {
									replacement = "[<" + pStruct.getParamName() + ">]";
								} else {
									PixelDataType importType = importParam.getType();
									if(importType == PixelDataType.CONST_INT || importType == PixelDataType.CONST_DECIMAL) {
										replacement = "<" + pStruct.getParamName() + ">";
									} else {
										replacement = "\"<" + pStruct.getParamName() + ">\"";
									}
								}
								SimpleQueryFilter paramF = new SimpleQueryFilter(
										new NounMetadata(new QueryColumnSelector(importParam.getTableName() + "__" + importParam.getColumnName()), PixelDataType.COLUMN), 
										comparator, 
										new NounMetadata(replacement, PixelDataType.CONST_STRING)
										);
								
								// add these filters into the AND
								andFilter.addFilter(paramF);
								
								logger.info("Adding new filter for column = " + pStruct.getParamName() );
							}
						}
						
						// swap the filter lists
						currentImportFilters.clear();
						// if only 1 value in the AND block
						// just grab it and send that filter
						if(andFilter.getFilterList().size() == 1) {
							currentImportFilters.add(andFilter.getFilterList().get(0));
						} else {
							currentImportFilters.add(andFilter);
						}
						
						ParamStructDetails datasoucreParameter = getDatasourceParameter(thisImportParams);
						String newExpr = null;
						if(datasoucreParameter != null) {
							ParamStruct paramStruct = detailsLookup.get(datasoucreParameter);
							// assume Database()
							newExpr = "Database(<\"" + paramStruct.getParamName() + ">\")";
						} else {
							newExpr = this.sourceStr;
						}
						newExpr += " | " + QsToPixelConverter.getPixel(this.importQs, false) + " | " + this.importStr + ";";
						this.pixels.add(newExpr);
					}
					
					// set the import string into the details
					// this is so we know if it is Database or FileRead or something else
					for(ParamStructDetails details : thisImportParams) {
						details.setImportSource(this.sourceStr);
					}
					
					// reset
					this.importQs = null;
					this.sourceStr = null;
					this.importStr = null;
				}
			} else {
				// add to list of expressions
				expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
				this.pixels.add(expression);
			}
		}
	}
	
	@Override
	public void inAOperation(AOperation node) {
		super.inAOperation(node);
		if(this.curReactor instanceof DatabaseReactor || this.curReactor instanceof FileReadReactor
				|| this.curReactor instanceof GoogleFileRetrieverReactor 
				|| this.curReactor instanceof FrameReactor 
				|| this.curReactor instanceof JdbcSourceReactor) {
			this.sourceStr = node.toString().trim();
		}
		else if(this.curReactor instanceof ImportReactor || this.curReactor instanceof MergeReactor) {
			this.importStr = node.toString().trim();
		}
	}
	
	/**
	 * Same method as in lazy with addition of addRoutine method
	 */
	@Override
    protected void deInitReactor() {
    	if(curReactor != null) {
    		// merge up and update the plan
    		try {
    			curReactor.mergeUp();
    			curReactor.updatePlan();
    		} catch(Exception e) {
    			logger.error(Constants.STACKTRACE, e);
    			throw new IllegalArgumentException(e.getMessage());
    		}
    		
    		// get the parent
    		Object parent = curReactor.Out();
    		// set the parent as the curReactor if it is present
    		prevReactor = curReactor;
    		if(parent instanceof IReactor) {
    			curReactor = (IReactor) parent;
    		} else {
    			curReactor = null;
    		}

    		// account for moving qs
    		if(curReactor == null && prevReactor instanceof AbstractQueryStructReactor) {
    			AbstractQueryStruct qs = ((AbstractQueryStructReactor) prevReactor).getQs();
	    		this.planner.addVariable(this.resultKey, new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
    		}
    		
        	// need to find imports
        	if(prevReactor != null && (prevReactor instanceof ImportReactor || prevReactor instanceof MergeReactor)) {
    			importQs = (SelectQueryStruct) prevReactor.getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey()).get(0);
    		}
    	}
    }
	
	private ParamStructDetails getDatasourceParameter(List<ParamStructDetails> thisImportParams) {
		for(ParamStructDetails detail : thisImportParams) {
			if(detail.getParameterFillType() == PARAMETER_FILL_TYPE.DATASOURCE) {
				return detail;
			}
		}
		return null;
	}
	
    /////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    
	public void setCurrentPixelId(String currentPixelId) {
		this.currentPixelId = currentPixelId;
	}
	
	/**
	 * Get the new pixels
	 * @return
	 */
	public List<String> getPixels() {
		return this.pixels;
	}
	
	public void setInputsToParameterize(List<ParamStruct> paramStructs) {
		this.paramStructs = paramStructs;
	}
	
	/**
	 * Testing method
	 * @param args
	 */
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:\\workspace3\\Semoss_Dev\\RDF_Map.prop");
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//		
//		String[] recipe = new String[]{
//				"AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ;",
//				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;",
//				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
//				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
//				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
//				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;", 
//				
//				"Database ( database = [ \"98ff280b-e219-4c4f-b6a5-2425e6caa93d\" ] ) | Query ( \"<encode> " + 
//				" SELECT Member_Engagement_Tier AS \"Member Engagement Tier\",Percent_of_Members_by_Engagement AS \"Percent of Members by Engagement\",Account_ID AS \"Account ID\",Account_Name AS \"Account Name\"  FROM  (  SELECT MBRSHP.MBR_ENGGMNT_TIER AS Member_Engagement_Tier,case when MAX( TOTL_MBRSHP.TOTL_MDCL_MBR_CVRG_CNT1) = 0 then 0 else (cast(max(MBRENGGMNT.ENGGD_MDCL_MBR_CVRG_CNT) as decimal(18,6))/cast(MAX( TOTL_MBRSHP.TOTL_MDCL_MBR_CVRG_CNT1) as decimal(18,6)))*100 end AS Percent_of_Members_by_Engagement,MBRSHP.ACCT_ID AS Account_ID,MBRSHP.ACCT_NM AS Account_Name,MBRSHP.TM_PRD_NM AS Time_Period  FROM  (  SELECT coalesce(TMBRENGGMNT.ENGGMNT_TIER,'Not Engaged' ) AS MBR_ENGGMNT_TIER,CII_ACCT_PRFL.ACCT_ID AS ACCT_ID,CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"	STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"	END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"		else SRVC_STRT_MNTH_NBR\r\n" + 
//				"	end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"		else SRVC_END_MNTH_NBR\r\n" + 
//				"	end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"		when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"	end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"	DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 1\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) ) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH LEFT JOIN  (  select a.acct_id, a.MCID,a.TM_PRD_NM, a.ENGGMNT, b.ENGGMNT_TIER   from  (  select CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID, TM_PRD_FNCTN.TM_PRD_NM , ( CASE   WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'        WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0   AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'        WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0    AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0   AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'        ELSE 'Not Engaged'   END) AS ENGGMNT     from CII_FACT_CP_ENGGMNT   INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0004156') and SRC_FLTR_ID in ('4e982e3f-31c4-4518-9805-f43531478a66'))SGMNTN on CII_FACT_CP_ENGGMNT.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_CP_ENGGMNT.ACCT_ID=SGMNTN.ACCT_ID inner join DIM_ENGGMNT       on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (  SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				" STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				" END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				" end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				" end as END_SRVC_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				" end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				" DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 1\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN     ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH    and TM_PRD_FNCTN. END_YEAR_MNTH   WHERE CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0004156')   group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID , TM_PRD_FNCTN.TM_PRD_NM ) a  inner join (  select   CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   WHEN DIM_ENGGMNT.TRDTNL_IND = 0   AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))   WHEN DIM_ENGGMNT.TRDTNL_IND= 0    AND DIM_ENGGMNT.ENHNCD_IND = 0   AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN  cast('Comprehensive' as char(20))   ELSE cast('Not Engaged' as char(20))   END AS ENGGMNT ,  CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   WHEN DIM_ENGGMNT.TRDTNL_IND = 0   AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))   WHEN DIM_ENGGMNT.TRDTNL_IND= 0    AND DIM_ENGGMNT.ENHNCD_IND = 0   AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN cast('Comprehensive' as char(20))   ELSE cast('Not Engaged' as char(20))   END AS ENGGMNT_TIER                         from DIM_ENGGMNT      union all      \r\n" + 
//				"select   CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   else cast('Care Coordination' as char(20))   end as ENGGMNT , cast( 'Comprehensive' as char(20))  AS ENGGMNT_TIER   from DIM_ENGGMNT   \r\n" + 
//				"where DIM_ENGGMNT.TRDTNL_IND= 1   or DIM_ENGGMNT.ENHNCD_IND= 1      union all      select cast('Traditional' as char(20)) as ENGGMNT, cast('Care Coordination' as char(20))  AS ENGGMNT_TIER   from DIM_ENGGMNT   where DIM_ENGGMNT.TRDTNL_IND= 1 ) b   on a.ENGGMNT = b.ENGGMNT    )  TMBRENGGMNT    ON CII_FACT_MBRSHP.ACCT_ID = TMBRENGGMNT.ACCT_ID    and CII_FACT_MBRSHP.MCID=TMBRENGGMNT.MCID    and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT.TM_PRD_NM INNER JOIN CII_ACCT_PRFL ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0004156') and SRC_FLTR_ID in ('4e982e3f-31c4-4518-9805-f43531478a66'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0004156')   GROUP BY coalesce(TMBRENGGMNT.ENGGMNT_TIER,'Not Engaged' ),CII_ACCT_PRFL.ACCT_ID,CII_ACCT_PRFL.ACCT_NM,TM_PRD_FNCTN.TM_PRD_NM ) AS MBRSHP LEFT JOIN  (  SELECT TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,coalesce(TMBRENGGMNT.ENGGMNT_TIER,'Not Engaged' ) AS MBR_ENGGMNT_TIER,CII_FACT_MBRSHP.ACCT_ID AS ACCT_ID,SUM(case when CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD='001' then CII_FACT_MBRSHP.MBR_CVRG_CNT else 0 end) AS ENGGD_MDCL_MBR_CVRG_CNT  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"	STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"	END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"		else SRVC_STRT_MNTH_NBR\r\n" + 
//				"	end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"		else SRVC_END_MNTH_NBR\r\n" + 
//				"	end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"		when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"	end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"	DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 1\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) ) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH \r\n" + 
//				"LEFT JOIN  (  select a.acct_id, a.MCID,a.TM_PRD_NM, a.ENGGMNT, b.ENGGMNT_TIER   from  (  select CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID, TM_PRD_FNCTN.TM_PRD_NM , \r\n" + 
//				"( CASE   WHEN MAX(DIM_ENGGMNT.TRDTNL_IND)= 1 THEN 'Traditional'        \r\n" + 
//				"WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0   AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 1 THEN 'Care Coordination'        \r\n" + 
//				"WHEN MAX(DIM_ENGGMNT.TRDTNL_IND) = 0    AND MAX(DIM_ENGGMNT.ENHNCD_IND) = 0   AND MAX(DIM_ENGGMNT.EXPNDD_IND)= 1 THEN 'Comprehensive'        \r\n" + 
//				"ELSE 'Not Engaged'   END) AS ENGGMNT     from CII_FACT_CP_ENGGMNT   \r\n" + 
//				"INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0004156') and SRC_FLTR_ID in ('4e982e3f-31c4-4518-9805-f43531478a66'))SGMNTN on CII_FACT_CP_ENGGMNT.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_CP_ENGGMNT.ACCT_ID=SGMNTN.ACCT_ID inner join DIM_ENGGMNT       on CII_FACT_CP_ENGGMNT.MBR_ENGGMNT_ID = DIM_ENGGMNT.ENGGMNT_ID JOIN (  SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				" STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				" END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"  else SRVC_STRT_MNTH_NBR\r\n" + 
//				" end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"  and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"  when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"  else SRVC_END_MNTH_NBR\r\n" + 
//				" end as END_SRVC_YEAR_MNTH,\r\n" + 
//				" case\r\n" + 
//				"  when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"  when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"  when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				" end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				" DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 1\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) )   )  TM_PRD_FNCTN     \r\n" + 
//				"ON CII_FACT_CP_ENGGMNT.ENGGMNT_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH    and TM_PRD_FNCTN. END_YEAR_MNTH   \r\n" + 
//				"WHERE CII_FACT_CP_ENGGMNT.ACCT_ID IN ('W0004156')   group by  CII_FACT_CP_ENGGMNT.acct_id, CII_FACT_CP_ENGGMNT.MCID , TM_PRD_FNCTN.TM_PRD_NM ) a  \r\n" + 
//				"inner join \r\n" + 
//				"(  select   CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   \r\n" + 
//				"WHEN DIM_ENGGMNT.TRDTNL_IND = 0   AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))   \r\n" + 
//				"WHEN DIM_ENGGMNT.TRDTNL_IND= 0    AND DIM_ENGGMNT.ENHNCD_IND = 0   AND DIM_ENGGMNT.EXPNDD_IND= 1 \r\n" + 
//				"THEN  cast('Comprehensive' as char(20))   ELSE cast('Not Engaged' as char(20))   END AS ENGGMNT , \r\n" + 
//				"CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   \r\n" + 
//				"WHEN DIM_ENGGMNT.TRDTNL_IND = 0   AND DIM_ENGGMNT.ENHNCD_IND= 1 THEN cast('Care Coordination' as char(20))   \r\n" + 
//				"WHEN DIM_ENGGMNT.TRDTNL_IND= 0    AND DIM_ENGGMNT.ENHNCD_IND = 0   AND DIM_ENGGMNT.EXPNDD_IND= 1 THEN cast('Comprehensive' as char(20))   ELSE cast('Not Engaged' as char(20))   END AS ENGGMNT_TIER                         \r\n" + 
//				"from DIM_ENGGMNT      union all      \r\n" + 
//				"select   CASE   WHEN DIM_ENGGMNT.TRDTNL_IND= 1 THEN cast('Traditional' as char(20))   else cast('Care Coordination' as char(20))   end as ENGGMNT , \r\n" + 
//				"cast( 'Comprehensive' as char(20))  AS ENGGMNT_TIER   from DIM_ENGGMNT   where DIM_ENGGMNT.TRDTNL_IND= 1   or DIM_ENGGMNT.ENHNCD_IND= 1      \r\n" + 
//				"union all      \r\n" + 
//				"select cast('Traditional' as char(20)) as ENGGMNT, cast('Care Coordination' as char(20))  AS ENGGMNT_TIER   \r\n" + 
//				"from DIM_ENGGMNT   where DIM_ENGGMNT.TRDTNL_IND= 1 ) b   on a.ENGGMNT = b.ENGGMNT    )  TMBRENGGMNT    \r\n" + 
//				"ON CII_FACT_MBRSHP.ACCT_ID = TMBRENGGMNT.ACCT_ID    and CII_FACT_MBRSHP.MCID=TMBRENGGMNT.MCID    and TM_PRD_FNCTN.TM_PRD_NM =  TMBRENGGMNT.TM_PRD_NM \r\n" + 
//				"INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0004156') and SRC_FLTR_ID in ('4e982e3f-31c4-4518-9805-f43531478a66'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0004156')   GROUP BY TM_PRD_FNCTN.TM_PRD_NM,coalesce(TMBRENGGMNT.ENGGMNT_TIER,'Not Engaged' ),CII_FACT_MBRSHP.ACCT_ID ) AS MBRENGGMNT ON MBRSHP.MBR_ENGGMNT_TIER=MBRENGGMNT.MBR_ENGGMNT_TIER AND MBRSHP.ACCT_ID=MBRENGGMNT.ACCT_ID AND MBRSHP.TM_PRD_NM=MBRENGGMNT.TM_PRD_NM INNER JOIN  (  SELECT TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM,CII_FACT_MBRSHP.ACCT_ID AS ACCT_ID,SUM( case when CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD='001' then CII_FACT_MBRSHP.MBR_CVRG_CNT else 0  end) AS TOTL_MDCL_MBR_CVRG_CNT1  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
//				"	STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
//				"	END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
//				"		else SRVC_STRT_MNTH_NBR\r\n" + 
//				"	end as STRT_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"		and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
//				"		when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
//				"		else SRVC_END_MNTH_NBR\r\n" + 
//				"	end as END_SRVC_YEAR_MNTH,\r\n" + 
//				"	case\r\n" + 
//				"		when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
//				"		when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
//				"		when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
//				"	end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
//				"from\r\n" + 
//				"	DIM_TM_PRD_ADHC dtp\r\n" + 
//				"join DIM_MNTH dm on 1=1\r\n" + 
//				"where	dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
//				"and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
//				"and dtp.LAG_MNTH_NBR = 0\r\n" + 
//				"and dtp.YEAR_ID <= 1\r\n" + 
//				"and dm.YEAR_MNTH_NBR = 202011\r\n" + 
//				"and (\r\n" + 
//				"(dtp.CSTM_STRT_CURNT_MNTH_NBR = 201912 and dtp.CSTM_END_CURNT_MNTH_NBR = 202011)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201812 and dtp.CSTM_END_PRIOR_MNTH_NBR = 201911)\r\n" + 
//				"or (dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801 and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201811) ) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0004156') and SRC_FLTR_ID in ('4e982e3f-31c4-4518-9805-f43531478a66'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0004156')   GROUP BY TM_PRD_FNCTN.TM_PRD_NM,CII_FACT_MBRSHP.ACCT_ID ) AS TOTL_MBRSHP ON MBRSHP.TM_PRD_NM=TOTL_MBRSHP.TM_PRD_NM AND MBRSHP.ACCT_ID=TOTL_MBRSHP.ACCT_ID  GROUP BY MBRSHP.MBR_ENGGMNT_TIER,MBRSHP.ACCT_ID,MBRSHP.ACCT_NM,MBRSHP.TM_PRD_NM ) AS FNL   ORDER BY Member_Engagement_Tier,Percent_of_Members_by_Engagement,Account_ID,Account_Name"
//				+ "</encode>\" ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"External_FRAME887854\" ] ) ] ) ;",
//						
//				"META | PositionInsightRecipeStep ( positionMap = [ { \"auto\" : false , \"top\" : 24 , \"left\" : 24 } ] ) ;",
//				"META | SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"order\":0,\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});",
//		};
//		int recipeLength = recipe.length;
//		String[] ids = new String[recipeLength];
//		for(int i = 0; i < recipeLength; i++) {
//			ids[i] = i+"";
//		}
//		
//		List<ParamStruct> params = new Vector<>();
//		// param
//		{
//			ParamStruct pStruct = new ParamStruct();
//			params.add(pStruct);
//			pStruct.setDefaultValue(null);
//			pStruct.setParamName("ACCT_ID");
//			pStruct.setFillType(FILL_TYPE.QUERY);
//			pStruct.setModelQuery("");
//			pStruct.setMultiple(true);
//			pStruct.setRequired(true);
//			pStruct.setSearchable(true);
//			pStruct.setModelLabel("Fill in ACCT_ID");
//			{
//				ParamStructDetails details = new ParamStructDetails();
//				pStruct.addParamStructDetails(details);
//				details.setPixelId("6");
//				details.setPixelString(recipe[7]);
//				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
//				details.setColumnName("ACCT_ID");
//				details.setOperator("in");
//				details.setLevel(LEVEL.COLUMN);
//				details.setQuote(QUOTE.NO);
//			}
//		}
//		
//		Insight in = new Insight();
//		ParamStructSaveRecipeTranslation translation = new ParamStructSaveRecipeTranslation(in);
//		translation.setInputsToParameterize(params);
//		
//		// loop through recipe
//		for(int i = 0; i < recipeLength; i++) {
//			String expression = recipe[i];
//			String pixelId = ids[i];
//			try {
//				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList, translation.encodedToOriginal);
//				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
//				// parsing the pixel - this process also determines if expression is syntactically correct
//				Start tree = p.parse();
//				// apply the translation.
//				translation.setCurrentPixelId(pixelId);
//				tree.apply(translation);
//			} catch (ParserException | LexerException | IOException e) {
//				e.printStackTrace();
//			}
//		}
//		
//		System.out.println(gson.toJson(translation.getPixels()));
//	}

}
