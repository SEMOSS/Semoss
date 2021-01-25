package prerna.sablecc2.translations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStruct.FILL_TYPE;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.LEVEL;
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
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.imports.ImportReactor;
import prerna.sablecc2.reactor.imports.MergeReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.qs.source.FileReadReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.test.TestUtilityMethods;
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
					List<ParamStructDetails> thisImportParams = new Vector<>();
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
						
						String newExpr = sourceStr + "| Query(\"<encode>" + finalQuery + "</encode>\") | " + this.importStr + " ;";
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
								modification = QsFilterParameterizeConverter2.modifyFilter(modification, importParam, detailsLookup.get(importParam));
								// if we have returned a new filter object
								// that means it has been modified
								if(modification != f) {
									paramIndexFound.add(i);
									// switch the reference of f in this example
									// so that not all the param index get added
									f = modification;
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
						
						// swap the filter lists
						currentImportFilters.clear();
						// if only 1 value in the AND block
						// just grab it and send that filter
						if(andFilter.getFilterList().size() == 1) {
							currentImportFilters.add(andFilter.getFilterList().get(0));
						} else {
							currentImportFilters.add(andFilter);
						}
						
						String newExpr = sourceStr + "|" + QsToPixelConverter.getPixel(this.importQs, false) + " | " + this.importStr + " ;";
						this.pixels.add(newExpr);
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
				|| this.curReactor instanceof GoogleFileRetrieverReactor || this.curReactor instanceof FrameReactor) {
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
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace3\\Semoss_Dev\\RDF_Map.prop");
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		String[] recipe = new String[]{
				"AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ;",
				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;",
				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;", 
				
				"Database ( database = [ \"98ff280b-e219-4c4f-b6a5-2425e6caa93d\" ] ) | Query ( \"<encode> SELECT Account_ID AS \"Account ID\",Master_Consumer_ID AS \"Master Consumer ID\",Member_Coverage_Type_Description AS \"Member Coverage Type Description\",Member_Gender_Code AS \"Member Gender Code\",Reporting_Member_Relationship_Description AS \"Reporting Member Relationship Description\",Age_Group_Description AS \"Age Group Description\",Age_In_Years AS \"Age In Years\",Contract_Type_Code AS \"Contract Type Code\",Eligibility_Year_Month_Ending_Number AS \"Eligibility Year Month Ending Number\",State_Code AS \"State Code\",CBSA_Name AS \"CBSA Name\",Member_PCP_Indicator AS \"Member PCP Indicator\",Subscriber_ID AS \"Subscriber ID\",Continuous_Enrollment_for_1_Period AS \"Continuous Enrollment for 1 Period\",Member_Birth_Date AS \"Member Birth Date\",Account_Name AS \"Account Name\",Time_Period_Start AS \"Time Period Start\",Time_Period_End AS \"Time Period End\",Non_Utilizer_Indicator AS \"Non Utilizer Indicator\",Member_Coverage_Count AS \"Member Coverage Count\"  FROM  (  SELECT MBRSHP.ACCT_ID AS Account_ID,MBRSHP.MCID AS Master_Consumer_ID,MBRSHP.MBR_CVRG_TYPE_DESC AS Member_Coverage_Type_Description,MBRSHP.MBR_GNDR_CD AS Member_Gender_Code,MBRSHP.RPTG_MBR_RLTNSHP_DESC AS Reporting_Member_Relationship_Description,MBRSHP.AGE_GRP_DESC AS Age_Group_Description,MBRSHP.AGE_IN_YRS_NBR AS Age_In_Years,MBRSHP.CNTRCT_TYPE_CD AS Contract_Type_Code,MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS Eligibility_Year_Month_Ending_Number,MBRSHP.ST_CD AS State_Code,MBRSHP.CBSA_NM AS CBSA_Name,MBRSHP.PCP_IND AS Member_PCP_Indicator,MBRSHP.FMBRSHP_SBSCRBR_ID AS Subscriber_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD AS Continuous_Enrollment_for_1_Period,MBRSHP.MBR_BRTH_DT AS Member_Birth_Date,MBRSHP.ACCT_NM AS Account_Name,MBRSHP.TIME_PRD_STRT_NBR AS Time_Period_Start,MBRSHP.TIME_PRD_END_NBR AS Time_Period_End,MBRSHP.Non_Utilizer_Ind AS Non_Utilizer_Indicator,SUM(MBRSHP.SUM_MBR_CVRG_CNT) AS Member_Coverage_Count,MBRSHP.TM_PRD_NM AS Time_Period  FROM  (  SELECT CII_ACCT_PRFL.ACCT_ID AS ACCT_ID,CII_FACT_MBRSHP.MCID AS MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC AS MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD AS MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC AS RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end AS AGE_GRP_DESC,CII_FACT_MBRSHP.AGE_IN_YRS_NBR AS AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD AS CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR AS ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD AS ST_CD,DIM_CBSA.CBSA_NM AS CBSA_NM,CII_FACT_MBRSHP.PCP_IND AS PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID AS FMBRSHP_SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD AS CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT AS MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM AS ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end AS TIME_PRD_STRT_NBR,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end AS TIME_PRD_END_NBR,UT.Non_Utilizer_Ind AS Non_Utilizer_Ind,SUM(MBR_CVRG_CNT) AS SUM_MBR_CVRG_CNT,TM_PRD_FNCTN.TM_PRD_NM AS TM_PRD_NM  FROM CII_FACT_MBRSHP INNER JOIN (SELECT YEAR_CD_NM as TM_PRD_NM,\r\n" + 
				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
				"                                else SRVC_END_MNTH_NBR\r\n" + 
				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
				"from\r\n" + 
				"                DIM_TM_PRD_ADHC dtp\r\n" + 
				"cross join DIM_MNTH dm\r\n" + 
				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
				")\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2') \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN ON CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH and TM_PRD_FNCTN.END_YEAR_MNTH INNER JOIN ( Select\r\n" + 
				"mbrshp.ACCT_ID,\r\n" + 
				"mbrshp.MCID,\r\n" + 
				"mbrshp.mbr_cvrg_type_cd,\r\n" + 
				"mbrshp.tm_prd_nm,\r\n" + 
				"Case \r\n" + 
				"                when (mbrshp.mcid = clms.mcid \r\n" + 
				"                and mbrshp.mbr_cvrg_type_cd = clms.mbr_cvrg_type_cd) then 'N' \r\n" + 
				"                Else 'Y' \r\n" + 
				"End        as Non_Utilizer_Ind\r\n" + 
				"from\r\n" + 
				"(\r\n" + 
				"Select\r\n" + 
				"fact.ACCT_ID,\r\n" + 
				"MCID,\r\n" + 
				"MBR_CVRG_TYPE_CD,\r\n" + 
				"  TM_PRD_NM\r\n" + 
				"  from\r\n" + 
				"cii_fact_mbrshp fact\r\n" + 
				"\r\n" + 
				"JOIN (             \r\n" + 
				"SELECT    YEAR_CD_NM as TM_PRD_NM,\r\n" + 
				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
				"                                else SRVC_END_MNTH_NBR\r\n" + 
				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
				"from\r\n" + 
				"                DIM_TM_PRD_ADHC dtp\r\n" + 
				"cross join DIM_MNTH dm\r\n" + 
				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
				")\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2') \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
				"    ON fact.ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH                \r\n" + 
				"    and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
				"                INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID  \r\n" + 
				"WHERE      fact.ACCT_ID = 'W0016437'                  \r\n" + 
				"GROUP BY  fact.acct_id,  fact.MCID, TM_PRD_FNCTN.TM_PRD_NM, fact.MBR_CVRG_TYPE_CD\r\n" + 
				") mbrshp\r\n" + 
				"\r\n" + 
				"left outer join\r\n" + 
				"(\r\n" + 
				"Select\r\n" + 
				"clm.ACCT_ID,\r\n" + 
				"MCID,\r\n" + 
				"MBR_CVRG_TYPE_CD,\r\n" + 
				"TM_PRD_NM\r\n" + 
				"\r\n" + 
				"from\r\n" + 
				"cii_fact_clm_line clm\r\n" + 
				"JOIN (             \r\n" + 
				"SELECT     YEAR_CD_NM as TM_PRD_NM,\r\n" + 
				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
				"                                else SRVC_END_MNTH_NBR\r\n" + 
				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
				"from\r\n" + 
				"                DIM_TM_PRD_ADHC dtp\r\n" + 
				"cross join DIM_MNTH dm\r\n" + 
				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
				")\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2') \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)) TM_PRD_FNCTN                 \r\n" + 
				"    ON clm.CLM_SRVC_YEAR_MNTH_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH   \r\n" + 
				"                and TM_PRD_FNCTN.END_YEAR_MNTH  \r\n" + 
				"                INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on clm.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and clm.ACCT_ID=SGMNTN.ACCT_ID\r\n" + 
				"WHERE      clm.ACCT_ID =  'W0016437'                        \r\n" + 
				"GROUP BY  clm.acct_id,  clm.MCID, TM_PRD_FNCTN.TM_PRD_NM, clm.MBR_CVRG_TYPE_CD\r\n" + 
				") clms\r\n" + 
				"\r\n" + 
				"                on\r\n" + 
				"mbrshp.acct_id = clms.acct_id \r\n" + 
				"                and mbrshp.mcid=clms.mcid  \r\n" + 
				"                and mbrshp.TM_PRD_NM = clms.tm_prd_nm \r\n" + 
				"                and mbrshp.mbr_cvrg_type_cd  = clms.mbr_cvrg_type_cd  ) UT\r\n" + 
				"\r\n" + 
				"                ON TM_PRD_FNCTN.TM_PRD_NM = UT.TM_PRD_NM  \r\n" + 
				"                AND  CII_FACT_MBRSHP.ACCT_ID =UT.ACCT_ID  \r\n" + 
				"                AND  CII_FACT_MBRSHP.MCID =UT.MCID  \r\n" + 
				"                and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = UT.MBR_CVRG_TYPE_CD INNER JOIN CII_ACCT_PRFL ON CII_FACT_MBRSHP.ACCT_ID = CII_ACCT_PRFL.ACCT_ID INNER JOIN DIM_MBR_CVRG_TYPE ON CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_CD INNER JOIN DIM_RPTG_MBR_RLTNSHP ON CII_FACT_MBRSHP.RPTG_MBR_RLTNSHP_CD = DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_CD INNER JOIN DIM_AGE_GRP ON CII_FACT_MBRSHP.ACCT_AGE_GRP_KEY = DIM_AGE_GRP.AGE_GRP_KEY INNER JOIN DIM_CBSA ON CII_FACT_MBRSHP.CBSA_ID = DIM_CBSA.CBSA_ID INNER  JOIN ( \r\n" + 
				"                select      m.TM_PRD_NM,  m.acct_id, m.mcid, m. MBR_CVRG_TYPE_CD ,\r\n" + 
				"   case\r\n" + 
				"    when m.mnths = b.mnths\r\n" + 
				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Continuous'\r\n" + 
				"          when strt_mnth = start_prd\r\n" + 
				"    and end_mnth<>end_prd\r\n" + 
				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill' then 'Termed'\r\n" + 
				"          when strt_mnth <> start_prd\r\n" + 
				"    and end_mnth=end_prd\r\n" + 
				"    and m.MBR_CVRG_TYPE_CD <> 'back_fill'then 'Added'\r\n" + 
				"          when m.MBR_CVRG_TYPE_CD = 'back_fill' Then 'NA'\r\n" + 
				"          else 'Other'\r\n" + 
				"    end as CNTNUS_ENRLMNT_1_PRD_CD\r\n" + 
				"                from      (\r\n" + 
				"                                select    fact .acct_id,\r\n" + 
				"                                                                fact .MCID,\r\n" + 
				"                                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
				"                                                                fact.MBR_CVRG_TYPE_CD ,\r\n" + 
				"                                                                count (distinct fact .ELGBLTY_CY_MNTH_END_NBR) mnths ,\r\n" + 
				"                                                                min(fact .ELGBLTY_CY_MNTH_END_NBR) strt_mnth ,\r\n" + 
				"                                                                max(fact .ELGBLTY_CY_MNTH_END_NBR) \r\n" + 
				"                                                                end_mnth \r\n" + 
				"                                from      cii_fact_mbrshp fact  JOIN (              \r\n" + 
				"                                                SELECT      YEAR_CD_NM as TM_PRD_NM,\r\n" + 
				"                STRT_MNTH_NBR as START_YEAR_MNTH,\r\n" + 
				"                END_MNTH_NBR as END_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 111101\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then STRT_MNTH_NBR\r\n" + 
				"                                else SRVC_STRT_MNTH_NBR\r\n" + 
				"                end as STRT_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                                and INCRD_PAID_CD = 'PAID' then 888812\r\n" + 
				"                                when TM_PRD_TYPE_CD = 'Custom' then END_MNTH_NBR\r\n" + 
				"                                else SRVC_END_MNTH_NBR\r\n" + 
				"                end as END_SRVC_YEAR_MNTH,\r\n" + 
				"                case\r\n" + 
				"                                when TM_PRD_TYPE_CD <> 'Custom' then PAID_END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'PAID' then END_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC2' then 888811\r\n" + 
				"                                when INCRD_PAID_CD = 'INC3' then dm.YEAR_MNTH_NBR\r\n" + 
				"                                when INCRD_PAID_CD = 'INC1' then PAID_END_MNTH_NBR\r\n" + 
				"                end as END_RPTG_PAID_YEAR_MNTH\r\n" + 
				"from\r\n" + 
				"                DIM_TM_PRD_ADHC dtp\r\n" + 
				"cross join DIM_MNTH dm\r\n" + 
				"                where (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --    AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'  \r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_CURNT_MNTH_NBR = 201910\r\n" + 
				"                and dtp.CSTM_END_CURNT_MNTH_NBR = 202009\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009\r\n" + 
				")\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2')  \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_MNTH_NBR = 201810\r\n" + 
				"                and dtp.CSTM_END_PRIOR_MNTH_NBR = 201909\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009)\r\n" + 
				"                or (dtp.TM_PRD_TYPE_CD = 'Custom'\r\n" + 
				"                and dtp.INCRD_PAID_CD = 'PAID'\r\n" + 
				"                and dtp.LAG_MNTH_NBR = 0\r\n" + 
				"                --AND DIM_TM_PRD_ADHC.TM_3061_RULE_IND = 'N'\r\n" + 
				"                and dtp.YEAR_ID <= 1\r\n" + 
				"                --In ('Current','Prior','Prior 2') \r\n" + 
				"                and dtp.CSTM_STRT_PRIOR_2_MNTH_NBR = 201801\r\n" + 
				"                and dtp.CSTM_END_PRIOR_2_MNTH_NBR = 201809\r\n" + 
				"                and dm.YEAR_MNTH_NBR = 202009) ) TM_PRD_FNCTN                      \r\n" + 
				"                                                ON fact .ELGBLTY_CY_MNTH_END_NBR BETWEEN TM_PRD_FNCTN.START_YEAR_MNTH  \r\n" + 
				"                                                and TM_PRD_FNCTN. \r\n" + 
				"                                                                END_YEAR_MNTH INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on fact.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and fact.ACCT_ID=SGMNTN.ACCT_ID   \r\n" + 
				"                                WHERE fact .ACCT_ID = 'W0016437'                         \r\n" + 
				"                                GROUP BY  fact .acct_id,\r\n" + 
				"                                                                fact .MCID,\r\n" + 
				"                                                                TM_PRD_FNCTN.TM_PRD_NM,\r\n" + 
				"                                                                fact .MBR_CVRG_TYPE_CD ) m \r\n" + 
				"                inner join (\r\n" + 
				"                                select    \r\n" + 
				"                                                                case \r\n" + 
				"                                                                                when YEAR_ID= 1 then 'Current Period'  \r\n" + 
				"                                                                                when YEAR_ID= 2 then 'Prior Period' \r\n" + 
				"                                                                                when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
				"                                                                                ELSE    'FOO' \r\n" + 
				"                                                                END as TM_PRD_NM,\r\n" + 
				"                                                                max(e_abs) - max(s_abs)+ 1 mnths,\r\n" + 
				"                                                                max(s_prd) as start_prd,\r\n" + 
				"                                                                max(e_prd)  as \r\n" + 
				"                                                                end_prd \r\n" + 
				"                                from         ( \r\n" + 
				"                                                select         \r\n" + 
				"                                                                                CASE \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 202009 then 1 \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
				"                                                                or YEAR_MNTH_NBR =  201909   then 2   \r\n" + 
				"                                                                                                WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
				"                                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
				"                                                                                                ELSE  4 \r\n" + 
				"                                                                                END  as YEAR_ID,\r\n" + 
				"                                                                                CASE \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
				"                                                                or  YEAR_MNTH_NBR = 201810  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 201801  then ABS_YEAR_MNTH_NBR \r\n" + 
				"                                                                                                else null \r\n" + 
				"                                                                                end as s_abs ,\r\n" + 
				"                                                                                CASE \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 201810  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 201801  then YEAR_MNTH_NBR \r\n" + 
				"                                                                                                else null \r\n" + 
				"                                                                                end as s_prd ,\r\n" + 
				"                                                                                                 \r\n" + 
				"                                                                                CASE  \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
				"                                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 201809  then ABS_YEAR_MNTH_NBR \r\n" + 
				"                                                                                                else null \r\n" + 
				"                                                                                end as e_abs,\r\n" + 
				"                                                                                CASE \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR = 202009  \r\n" + 
				"                                                                or YEAR_MNTH_NBR =  201909  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 201809  then YEAR_MNTH_NBR \r\n" + 
				"                                                                                                else null \r\n" + 
				"                                                                                end as e_prd                 \r\n" + 
				"                                                from      dim_mnth   \r\n" + 
				"                                                where   \r\n" + 
				"                                                                                CASE \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR =  201910  \r\n" + 
				"                                                                or YEAR_MNTH_NBR = 202009 then 1               \r\n" + 
				"                                                                                                WHEN YEAR_MNTH_NBR = 201810  \r\n" + 
				"                                                                or YEAR_MNTH_NBR =  201909   then 2 \r\n" + 
				"                                                                                                WHEN  YEAR_MNTH_NBR = 201801  \r\n" + 
				"                                                                or  YEAR_MNTH_NBR = 201809 then  3 \r\n" + 
				"                                                                                                ELSE 0 \r\n" + 
				"                                                                                END  > 0 ) a  \r\n" + 
				"                                group by \r\n" + 
				"                                                                case \r\n" + 
				"                                                                                when YEAR_ID= 1 then 'Current Period' \r\n" + 
				"                                                                                when YEAR_ID= 2 then 'Prior Period'  \r\n" + 
				"                                                                                when YEAR_ID= 3 then 'Prior Period 2'   \r\n" + 
				"                                                                                ELSE    'FOO' \r\n" + 
				"                                                                END) b     \r\n" + 
				"                                on  m.TM_PRD_NM = b.TM_PRD_NM) CE              \r\n" + 
				"                ON TM_PRD_FNCTN.TM_PRD_NM = CE.TM_PRD_NM      \r\n" + 
				"                AND  CII_FACT_MBRSHP.ACCT_ID =CE.ACCT_ID     \r\n" + 
				"                AND  CII_FACT_MBRSHP.MCID =CE.MCID     \r\n" + 
				"                and CII_FACT_MBRSHP.MBR_CVRG_TYPE_CD = CE.MBR_CVRG_TYPE_CD INNER JOIN DIM_MCID ON CII_FACT_MBRSHP.MCID = DIM_MCID.MCID AND CII_FACT_MBRSHP.ACCT_ID = DIM_MCID.ACCT_ID INNER JOIN (Select ACCT_ID,SGMNTN_DIM_KEY, SGMNTN_NM, SRC_FLTR_ID from ACIISST_SGMNTN_BRDG where ACCT_ID in ('W0016437') and SRC_FLTR_ID in ('4733bea4-a055-4641-b834-2c6818e13f45'))SGMNTN on CII_FACT_MBRSHP.SGMNTN_DIM_KEY = SGMNTN.SGMNTN_DIM_KEY and CII_FACT_MBRSHP.ACCT_ID=SGMNTN.ACCT_ID WHERE CII_FACT_MBRSHP.ACCT_ID in ('W0016437')   GROUP BY CII_ACCT_PRFL.ACCT_ID,CII_FACT_MBRSHP.MCID,DIM_MBR_CVRG_TYPE.MBR_CVRG_TYPE_DESC,CII_FACT_MBRSHP.MBR_GNDR_CD,DIM_RPTG_MBR_RLTNSHP.RPTG_MBR_RLTNSHP_DESC,case when DIM_AGE_GRP.AGE_GRP_DESC='1-17' then '1 through 17' else  DIM_AGE_GRP.AGE_GRP_DESC end,CII_FACT_MBRSHP.AGE_IN_YRS_NBR,CII_FACT_MBRSHP.CNTRCT_TYPE_CD,CII_FACT_MBRSHP.ELGBLTY_CY_MNTH_END_NBR,CII_FACT_MBRSHP.ST_CD,DIM_CBSA.CBSA_NM,CII_FACT_MBRSHP.PCP_IND,CII_FACT_MBRSHP.SBSCRBR_ID,CE.CNTNUS_ENRLMNT_1_PRD_CD,DIM_MCID.MBR_BRTH_DT,CII_ACCT_PRFL.ACCT_NM,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 201910 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201810 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201801 end,CASE WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Current Period'  THEN 202009 WHEN TM_PRD_FNCTN.TM_PRD_NM= 'Prior Period' THEN 201909 WHEN TM_PRD_FNCTN.TM_PRD_NM=  'Prior Period 2'  THEN 201809 end,UT.Non_Utilizer_Ind,TM_PRD_FNCTN.TM_PRD_NM ) AS MBRSHP  GROUP BY MBRSHP.ACCT_ID,MBRSHP.MCID,MBRSHP.MBR_CVRG_TYPE_DESC,MBRSHP.MBR_GNDR_CD,MBRSHP.RPTG_MBR_RLTNSHP_DESC,MBRSHP.AGE_GRP_DESC,MBRSHP.AGE_IN_YRS_NBR,MBRSHP.CNTRCT_TYPE_CD,MBRSHP.ELGBLTY_CY_MNTH_END_NBR,MBRSHP.ST_CD,MBRSHP.CBSA_NM,MBRSHP.PCP_IND,MBRSHP.FMBRSHP_SBSCRBR_ID,MBRSHP.CNTNUS_ENRLMNT_1_PRD_CD,MBRSHP.MBR_BRTH_DT,MBRSHP.ACCT_NM,MBRSHP.TIME_PRD_STRT_NBR,MBRSHP.TIME_PRD_END_NBR,MBRSHP.Non_Utilizer_Ind,MBRSHP.TM_PRD_NM ) AS FNL   ORDER BY Account_ID,Master_Consumer_ID,Member_Coverage_Type_Description,Member_Gender_Code,Reporting_Member_Relationship_Description,Age_Group_Description,Age_In_Years,Contract_Type_Code,Eligibility_Year_Month_Ending_Number,State_Code,CBSA_Name,Member_PCP_Indicator,Subscriber_ID,Continuous_Enrollment_for_1_Period,Member_Birth_Date,Account_Name,Time_Period_Start,Time_Period_End,Non_Utilizer_Indicator,Member_Coverage_Count</encode>\" ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"External_FRAME887854\" ] ) ] ) ;",
						
						
				"META | PositionInsightRecipeStep ( positionMap = [ { \"auto\" : false , \"top\" : 24 , \"left\" : 24 } ] ) ;",
				"META | SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"order\":0,\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});",
		};
		int recipeLength = recipe.length;
		String[] ids = new String[recipeLength];
		for(int i = 0; i < recipeLength; i++) {
			ids[i] = i+"";
		}
		
		List<ParamStruct> params = new Vector<>();
		// param
		{
			ParamStruct pStruct = new ParamStruct();
			params.add(pStruct);
			pStruct.setDefaultValue(null);
			pStruct.setParamName("ACCT_ID");
			pStruct.setFillType(FILL_TYPE.QUERY);
			pStruct.setModelQuery("");
			pStruct.setMultiple(true);
			pStruct.setRequired(true);
			pStruct.setSearchable(true);
			pStruct.setModelLabel("Fill in ACCT_ID");
			{
				ParamStructDetails details = new ParamStructDetails();
				pStruct.addParamStructDetails(details);
				details.setPixelId("6");
				details.setPixelString(recipe[7]);
				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
				details.setColumnName("ACCT_ID");
				details.setOperator("in");
				details.setLevel(LEVEL.COLUMN);
				details.setQuote(QUOTE.NO);
			}
		}
		
		Insight in = new Insight();
		ParamStructSaveRecipeTranslation translation = new ParamStructSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);
		
		// loop through recipe
		for(int i = 0; i < recipeLength; i++) {
			String expression = recipe[i];
			String pixelId = ids[i];
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList, translation.encodedToOriginal);
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				translation.setCurrentPixelId(pixelId);
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println(gson.toJson(translation.getPixels()));
	}

}
