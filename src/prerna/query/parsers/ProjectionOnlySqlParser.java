package prerna.query.parsers;

import java.util.List;
import java.util.Vector;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOnlySqlParser {

	public List<String> projections = new Vector<String>();
	
	public void processQuery(String query) throws Exception {
		// parse the sql
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select)stmt);
		PlainSelect sb = (PlainSelect)select.getSelectBody();
		
		List<SelectItem> selects = sb.getSelectItems();
		for(int selectIndex = 0;selectIndex < selects.size();selectIndex++) {
			SelectItem si = selects.get(selectIndex);
			if(si instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Alias seiAlias = sei.getAlias();
				if(seiAlias == null) {
					projections.add(sei.toString());
				} else {
					String aliasName = seiAlias.getName().trim();
					if(aliasName.startsWith("\"") || aliasName.startsWith("'")) {
						aliasName = aliasName.substring(1); //remove the first quote
					}
					if(aliasName.endsWith("\"") || aliasName.endsWith("'")) {
						aliasName = aliasName.substring(0, aliasName.length()-1); //remove the end quote
					}
					projections.add(aliasName);
				}
			}
		}
	}
	
	public List<String> getProjections() {
		return projections;
	}
	
	public static void main(String[] args) throws Exception {
		String query = "\r\n" + 
				"SELECT CLM_MBRSHP.YEAR_ID AS \"Year_ID\",\r\n" + 
				"CLM_MBRSHP.MBR_CVRG_TYPE_CD AS  Coverage,\r\n" + 
				"CLM_MBRSHP.AGE_BAND_DESC AS \"Age_Desc\",\r\n" + 
				"CLM_MBRSHP.MBR_GNDR_CD AS Gender,\r\n" + 
				"CLM_MBRSHP.RPTG_MBR_RLTNSHP_CD AS Relationship,\r\n" + 
				"CLM_MBRSHP.CBSA_NM  AS \"CBSA_NM\",\r\n" + 
				"CLM_MBRSHP.ST_CD AS State,\r\n" + 
				"CLM_MBRSHP.ZIP_CD AS \"Zip Code\",\r\n" + 
				"BDTC_CTGRY_CD,\r\n" + 
				"HLTH1.RISK_STG_VAL_TXT AS Health_Status,\r\n" + 
				"(CASE WHEN HCC_COST >= 25000 THEN 'Yes'  ELSE 'No'   \r\n" + 
				"       END) AS  HCC,            \r\n" + 
				"\r\n" + 
				"COALESCE(EN_PRD.IsTraditional, 0) AS \"Is_Traditional\", \r\n" + 
				"COALESCE(EN_PRD.IsEnhanced, 0) AS \"Is_Enhanced\", \r\n" + 
				"COALESCE(EN_PRD.IsExpanded, 0) AS \"Is_Expanded\", \r\n" + 
				"COALESCE(EN_PRD.EngagementType, 'Not_Engaged') AS \"Engagement_Type\",\r\n" + 
				"\r\n" + 
				"COALESCE(Case EN_PRD.EngagementType when 'Primary' then 'Traditional'\r\n" + 
				"                                    WHEN 'Enhanced' then 'Care Coordination'\r\n" + 
				"         Else EN_PRD.EngagementType end, 'Not Engaged')as Report_Display,\r\n" + 
				"--CLM_MBRSHP.MCID as \"Members_mcid\", -- for Testing purpose Only\r\n" + 
				"--COUNT(  CLM_MBRSHP.MCID) over (PARTITION BY CLM_MBRSHP.YEAR_ID,CLM_MBRSHP.MBR_CVRG_TYPE_CD, BDTC_CTGRY_CD, HLTH1.RISK_STG_VAL_TXT, HCC ) as Num_Members,\r\n" + 
				"sum(MemberMonthsTotal) as \"Member_Months_Total\",\r\n" + 
				"max(MemberMonthsDenominator) as \"Member_Months_Denominator\",\r\n" + 
				"sum(MEMBER_MONTHS_COVERAGE) as \"Member_Months_Coverage\",\r\n" + 
				"sum(TOTL_COST_AMT) as \"Cost_Total\",\r\n" + 
				"sum(TOTL_INPAT_COST_AMT) as \"IP_Cost_Total\",\r\n" + 
				"sum(TOTL_OUTPAT_COST_AMT )as \"OP_Cost_Total\",\r\n" + 
				"sum(TOTL_ED_COST_AMT) as \"ED_Cost_Total\",\r\n" + 
				"sum(TOTL_PROF_COST_AMT) as \"Prof_Cost_Total\",\r\n" + 
				"sum(TOTL_AVDBL_ED_COST_AMT) as \"Avoidable_ED_Cost_Total\",\r\n" + 
				"sum(TOTL_RX_COST_AMT) as \"Rx_Cost_Total\",\r\n" + 
				"sum(ADMIT) as Admit,\r\n" + 
				"sum(INPAT_ADMT_CNT) as \"IP_Admit\",\r\n" + 
				"sum(OUTPAT_ADMT_CNT) as \"OP_Admit\",\r\n" + 
				"sum(ED_ADMT_CNT) as \"ED_Admit\",\r\n" + 
				"sum(PROF_ADMT_CNT) as \"Prof_Admit\",\r\n" + 
				"sum(AVDBL_ED_VST_CNT) as \"Avoidable_ED_Admit\",\r\n" + 
				"sum(ACUT_INPAT_ADMT_CNT) as \"Acute_IP_Admit\",\r\n" + 
				"sum(ACUT_INPAT_ADMT_DAYS_CNT) as \"Acute_IP_Admit_Days\"\r\n" + 
				"FROM\r\n" + 
				"(\r\n" + 
				"SELECT\r\n" + 
				"fact.MCID, \r\n" + 
				"fact.ACCT_ID,\r\n" + 
				"TP.YEAR_ID ,        \r\n" + 
				"MBR_CVRG_TYPE_CD, \r\n" + 
				"AGE_BAND_DESC, \r\n" + 
				"MBR_GNDR_CD,\r\n" + 
				"RPTG_MBR_RLTNSHP_CD,  \r\n" + 
				"CBSA_NM AS CBSA_NM,\r\n" + 
				"ST_CD AS ST_CD,\r\n" + 
				"ZIP_CD AS ZIP_CD,\r\n" + 
				"BDTC_CTGRY_CD,  \r\n" + 
				"-- cost / admit\r\n" + 
				"SUM(TOTL_COST_AMT) AS TOTL_COST_AMT,\r\n" + 
				"SUM(TOTL_INPAT_COST_AMT) AS TOTL_INPAT_COST_AMT,\r\n" + 
				"SUM(TOTL_OUTPAT_COST_AMT) AS TOTL_OUTPAT_COST_AMT,\r\n" + 
				"SUM(TOTL_ED_COST_AMT) AS TOTL_ED_COST_AMT,\r\n" + 
				"SUM(TOTL_PROF_COST_AMT) AS TOTL_PROF_COST_AMT,\r\n" + 
				"SUM(TOTL_AVDBL_ED_COST_AMT) AS TOTL_AVDBL_ED_COST_AMT,\r\n" + 
				"SUM(TOTL_RX_COST_AMT) AS TOTL_RX_COST_AMT,\r\n" + 
				"--admits\r\n" + 
				"SUM(INPAT_ADMT_CNT) AS INPAT_ADMT_CNT,\r\n" + 
				"SUM(OUTPAT_ADMT_CNT) AS OUTPAT_ADMT_CNT,\r\n" + 
				"SUM(ED_ADMT_CNT) AS ED_ADMT_CNT,\r\n" + 
				"SUM(PROF_ADMT_CNT) AS PROF_ADMT_CNT,\r\n" + 
				"SUM(AVDBL_ED_VST_CNT) AS AVDBL_ED_VST_CNT,\r\n" + 
				"SUM(ACUT_INPAT_ADMT_CNT) AS ACUT_INPAT_ADMT_CNT,\r\n" + 
				"SUM(ACUT_INPAT_ADMT_DAYS_CNT) AS ACUT_INPAT_ADMT_DAYS_CNT,\r\n" + 
				"sum(ADMT_CNT) as Admit,\r\n" + 
				"0 AS MemberMonthsDenominator,\r\n" + 
				"SUM(0) AS MemberMonthsTotal,\r\n" + 
				"sum(0) as MEMBER_MONTHS_COVERAGE\r\n" + 
				"FROM CII_CLMS_DSHBRD fact\r\n" + 
				"/*INNER JOIN ACIISST_USER_SGMNTN SGMNT ON fact.ACCT_ID=SGMNT.ACCT_ID AND fact.SGMNTN_DIM_KEY=SGMNT.ACIISST_SGMNTN_DIM_KEY \r\n" + 
				"AND ACIISST_USER_ID=<Parameters.USER_ID> AND SESN_ID=<Parameters.Session_Id> AND fact.ACCT_ID='W0003618'*/\r\n" + 
				"INNER JOIN \r\n" + 
				"(\r\n" + 
				"SELECT YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609  END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN   201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth,\r\n" + 
				"'2019-08-' AS StaticEndDate, 888812 as HighMonth,  111101 as lowMonth,  'Paid' as PaidIncurred,\r\n" + 
				"EXTRACT (YEAR FROM \r\n" + 
				"(ADD_MONTHS((CAST (CAST(StaticEndDate as CHAR(6))||'01' AS DATE)), 0)))*100\r\n" + 
				"+ EXTRACT (MONTH FROM \r\n" + 
				"(ADD_MONTHS((CAST (CAST(StaticEndDate as CHAR(6))||'01' AS DATE)), 0)))  as LagMonth,  -- replace 3 with parm 0\r\n" + 
				"case when  'Paid'='Paid'  then  LowMonth else StartYearMonth end as Srvc_Start_Year_month ,\r\n" + 
				"case WHEN 'Paid'='Paid' then HighMonth else EndYearMonth end as Srvc_End_Year_month,\r\n" + 
				"Case when 'Paid'='Paid'  then EndYearMonth  when 'Paid'='Incurred' and 0=0 then LagMonth else HighMonth end as Rptg_Paid_End_Year_Mnth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND Year_ID <= 1\r\n" + 
				") TP  ON fact.CLM_SRVC_YEAR_MNTH_NBR >= TP.Srvc_Start_Year_month AND fact.CLM_SRVC_YEAR_MNTH_NBR<= TP.Srvc_End_Year_month\r\n" + 
				"AND fact.RPTG_PAID_YEAR_MNTH_NBR >= TP.StartYearMonth AND fact.RPTG_PAID_YEAR_MNTH_NBR <=Rptg_Paid_End_Year_Mnth\r\n" + 
				"AND fact.ACCT_ID='W0004156'\r\n" + 
				"AND  fact.PAID_ALWD_CD= 'Paid'\r\n" + 
				"/*AND(fact.DMSTC_FRGN_IND= <Parameters.ForeignVsDomestic>  or  <Parameters.ForeignVsDomestic>='All' or  <Parameters.ForeignVsDomestic>='')\r\n" + 
				"and (fact.PROV_NTWK_TYPE_CD=<Parameters.Network Utilization> or <Parameters.Network Utilization>='All' or <Parameters.Network Utilization>='')\r\n" + 
				"and (fact.NTWK_TIER_CD= <Parameters.Tiered Product Arrangements> or  <Parameters.Tiered Product Arrangements>='All' or  <Parameters.Tiered Product Arrangements>='')*/\r\n" + 
				"GROUP BY \r\n" + 
				"fact.MCID, \r\n" + 
				"fact.ACCT_ID,\r\n" + 
				"YEAR_ID,               \r\n" + 
				"MBR_CVRG_TYPE_CD,\r\n" + 
				"AGE_BAND_DESC, \r\n" + 
				"MBR_GNDR_CD,\r\n" + 
				"RPTG_MBR_RLTNSHP_CD, \r\n" + 
				"CBSA_NM,\r\n" + 
				"ST_CD,\r\n" + 
				"ZIP_CD,\r\n" + 
				"BDTC_CTGRY_CD\r\n" + 
				"\r\n" + 
				"UNION ALL \r\n" + 
				"\r\n" + 
				"SELECT \r\n" + 
				"fact.MCID, \r\n" + 
				"fact.ACCT_ID,\r\n" + 
				"tp.YEAR_ID, \r\n" + 
				"fact.MBR_CVRG_TYPE_CD,\r\n" + 
				"AGE_BAND_DESC,\r\n" + 
				"MBR_GNDR_NM AS MBR_GNDR_CD,\r\n" + 
				"RPTG_MBR_RLTNSHP_CD, \r\n" + 
				"CBSA_NM,\r\n" + 
				"ST_CD,\r\n" + 
				"ZIP_CD,\r\n" + 
				"BDTC_CTGRY_CD,\r\n" + 
				"SUM(0) AS TOTL_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_INPAT_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_OUTPAT_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_ED_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_PROF_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_AVDBL_ED_COST_AMT,\r\n" + 
				"SUM(0) AS TOTL_RX_COST_AMT,\r\n" + 
				"--admits\r\n" + 
				"SUM(0) AS INPAT_ADMT_CNT,\r\n" + 
				"SUM(0) AS OUTPAT_ADMT_CNT,\r\n" + 
				"SUM(0) AS ED_ADMT_CNT,\r\n" + 
				"SUM(0) AS PROF_ADMT_CNT,\r\n" + 
				"SUM(0) AS AVDBL_ED_VST_CNT,\r\n" + 
				"SUM(0) AS ACUT_INPAT_ADMT_CNT,\r\n" + 
				"SUM(0) AS ACUT_INPAT_ADMT_DAYS_CNT,\r\n" + 
				"sum(0) as Admit,\r\n" + 
				"Count( Distinct fact.ELGBLTY_CY_MNTH_END_NBR ) AS MemberMonthsDenominator,\r\n" + 
				"SUM( CASE WHEN fact.MBR_CVRG_TYPE_CD = 'Medical' THEN fact.MBR_CVRG_CNT ELSE 0 END +\r\n" + 
				"                CASE WHEN fact.MBR_CVRG_TYPE_CD = 'RX' THEN RX_ONLY.RX_CNT ELSE 0 END )  AS MemberMonthsTotal,\r\n" + 
				"sum(MBR_CVRG_CNT) as MEMBER_MONTHS_COVERAGE\r\n" + 
				"FROM CII_MBRSHP_DSHBRD fact\r\n" + 
				"/*INNER JOIN ACIISST_USER_SGMNTN SGMNT ON fact.ACCT_ID=SGMNT.ACCT_ID AND fact.SGMNTN_DIM_KEY=SGMNT.ACIISST_SGMNTN_DIM_KEY \r\n" + 
				"AND ACIISST_USER_ID= <Parameters.USER_ID>\r\n" + 
				"AND SESN_ID=<Parameters.Session_Id>\r\n" + 
				"AND fact.ACCT_ID= 'W0003618'*/\r\n" + 
				"INNER JOIN\r\n" + 
				"(\r\n" + 
				"SELECT \r\n" + 
				"YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609 END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN  201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND YEAR_ID <= 1\r\n" + 
				") tp ON  fact.ELGBLTY_CY_MNTH_END_NBR >=tp.StartYearMonth and  fact.ELGBLTY_CY_MNTH_END_NBR <=tp.EndYearMonth\r\n" + 
				"AND fact.ACCT_ID= 'W0004156'\r\n" + 
				"LEFT JOIN\r\n" + 
				"( SELECT fact.ACCT_ID,TP.YEAR_ID,\r\n" + 
				"MCID,SUM( CASE WHEN MBR_CVRG_TYPE_CD='RX' THEN MBR_CVRG_CNT END) AS RX_CNT,\r\n" + 
				"ELGBLTY_CY_MNTH_END_NBR\r\n" + 
				"FROM CII_MBRSHP_DSHBRD fact \r\n" + 
				"/*INNER JOIN ACIISST_USER_SGMNTN SGMNT ON fact.ACCT_ID=SGMNT.ACCT_ID AND fact.SGMNTN_DIM_KEY=SGMNT.ACIISST_SGMNTN_DIM_KEY \r\n" + 
				"AND ACIISST_USER_ID= <Parameters.USER_ID>\r\n" + 
				"AND SESN_ID=<Parameters.Session_Id>*/\r\n" + 
				"INNER JOIN\r\n" + 
				"(\r\n" + 
				"SELECT \r\n" + 
				"YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609 END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN  201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND YEAR_ID <= 1\r\n" + 
				") tp ON  fact.ELGBLTY_CY_MNTH_END_NBR >=tp.StartYearMonth and  fact.ELGBLTY_CY_MNTH_END_NBR <=tp.EndYearMonth \r\n" + 
				"where  fact.ACCT_ID= 'W0004156'\r\n" + 
				"GROUP BY  MCID,ELGBLTY_CY_MNTH_END_NBR, fact.ACCT_ID, TP.YEAR_ID\r\n" + 
				"HAVING SUM( CASE WHEN MBR_CVRG_TYPE_CD='Medical' THEN MBR_CVRG_CNT END)=0\r\n" + 
				")RX_ONLY ON fact.MCID = RX_ONLY.MCID\r\n" + 
				"AND fact.ELGBLTY_CY_MNTH_END_NBR = RX_ONLY.ELGBLTY_CY_MNTH_END_NBR\r\n" + 
				"AND fact.ACCT_ID= RX_ONLY.ACCT_ID\r\n" + 
				"AND TP.YEAR_ID=RX_ONLY.YEAR_ID\r\n" + 
				"AND fact.ACCT_ID= 'W0004156'\r\n" + 
				"GROUP BY \r\n" + 
				"fact.MCID, \r\n" + 
				"fact.ACCT_ID,\r\n" + 
				"tp.YEAR_ID, \r\n" + 
				"MBR_CVRG_TYPE_CD,\r\n" + 
				"AGE_BAND_DESC,\r\n" + 
				"MBR_GNDR_NM,\r\n" + 
				"RPTG_MBR_RLTNSHP_CD, \r\n" + 
				"CBSA_NM,\r\n" + 
				"ST_CD,\r\n" + 
				"ZIP_CD,\r\n" + 
				"BDTC_CTGRY_CD\r\n" + 
				") CLM_MBRSHP\r\n" + 
				"left JOIN -- HCC\r\n" + 
				"(\r\n" + 
				"SELECT              \r\n" + 
				"ACCT_ID,   tp.YEAR_ID, MCID,\r\n" + 
				"SUM(TOTL_COST_AMT) AS HCC_COST \r\n" + 
				"\r\n" + 
				"FROM CII_CLMS_DSHBRD fact                \r\n" + 
				"INNER JOIN \r\n" + 
				"(\r\n" + 
				"SELECT YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609  END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN   201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth,\r\n" + 
				"'2019-08-' AS StaticEndDate, 888812 as HighMonth,  111101 as lowMonth,  'Paid' as PaidIncurred,\r\n" + 
				"EXTRACT (YEAR FROM \r\n" + 
				"(ADD_MONTHS((CAST (CAST(StaticEndDate as CHAR(6))||'01' AS DATE)), 0)))*100\r\n" + 
				"+ EXTRACT (MONTH FROM \r\n" + 
				"(ADD_MONTHS((CAST (CAST(StaticEndDate as CHAR(6))||'01' AS DATE)), 0)))  as LagMonth,  -- replace 3 with parm 0\r\n" + 
				"case when  'Paid'='Paid'  then  LowMonth else StartYearMonth end as Srvc_Start_Year_month ,\r\n" + 
				"case WHEN 'Paid'='Paid' then HighMonth else EndYearMonth end as Srvc_End_Year_month,\r\n" + 
				"Case when 'Paid'='Paid'  then EndYearMonth  when 'Paid'='Incurred' and 0=0 then LagMonth else HighMonth end as Rptg_Paid_End_Year_Mnth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND Year_ID <= 1\r\n" + 
				") TP  ON fact.CLM_SRVC_YEAR_MNTH_NBR >= TP.Srvc_Start_Year_month AND fact.CLM_SRVC_YEAR_MNTH_NBR<= TP.Srvc_End_Year_month\r\n" + 
				"AND fact.RPTG_PAID_YEAR_MNTH_NBR >= TP.StartYearMonth AND fact.RPTG_PAID_YEAR_MNTH_NBR <=Rptg_Paid_End_Year_Mnth\r\n" + 
				"WHERE fact.acct_id='W0004156'\r\n" + 
				"AND  fact.PAID_ALWD_CD= 'Paid'\r\n" + 
				"\r\n" + 
				"GROUP BY                               \r\n" + 
				"fact.ACCT_ID, MCID, YEAR_ID)HCC\r\n" + 
				"ON CLM_MBRSHP.MCID=HCC.MCID\r\n" + 
				"AND CLM_MBRSHP.ACCT_ID=HCC.ACCT_ID\r\n" + 
				"AND CLM_MBRSHP.YEAR_ID=HCC.YEAR_ID\r\n" + 
				"\r\n" + 
				"LEFT OUTER JOIN \r\n" + 
				"(select \r\n" + 
				"MCID,  RISK_STG_VAL_TXT , YEAR_ID\r\n" + 
				"from \r\n" + 
				"(select \r\n" + 
				"MCID, YEAR_ID, RISK_STG_VAL_TXT,  ELGBLTY_CY_MNTH_END_NBR,\r\n" + 
				"ROW_NUMBER() OVER (PARTITION BY MCID, YEAR_ID ORDER BY ELGBLTY_CY_MNTH_END_NBR desc ,\r\n" + 
				"RPTG_MBR_RLTNSHP_CD,\r\n" + 
				"MBR_GNDR_NM (CHAR(4)),\r\n" + 
				"CASE WHEN AGE_IN_YRS_NBR = -1 THEN 999 ELSE AGE_IN_YRS_NBR  END, \r\n" + 
				"fact.SGMNTN_DIM_KEY DESC,\r\n" + 
				"CASE WHEN ST_CD = 'NA' THEN 'ZZ' ELSE ST_CD END) as RNK \r\n" + 
				"from CII_MBRSHP_DSHBRD fact\r\n" + 
				"INNER JOIN         \r\n" + 
				"(SELECT \r\n" + 
				"YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609 END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN  201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND YEAR_ID <= 1\r\n" + 
				") tp ON  fact.ELGBLTY_CY_MNTH_END_NBR >=tp.StartYearMonth and  fact.ELGBLTY_CY_MNTH_END_NBR <=tp.EndYearMonth \r\n" + 
				"WHERE fact.ACCT_ID='W0004156'\r\n" + 
				"and MBR_CVRG_TYPE_CD='Medical'\r\n" + 
				") X where RNK=1  \r\n" + 
				"GROUP BY MCID,  RISK_STG_VAL_TXT , YEAR_ID\r\n" + 
				")HLTH1 on CLM_MBRSHP.MCID=HLTH1.MCID AND CLM_MBRSHP.YEAR_ID = HLTH1.YEAR_ID\r\n" + 
				"\r\n" + 
				"LEFT JOIN\r\n" + 
				"(SELECT MCID, fact.ACCT_ID, tp.YEAR_ID, MAXIMUM ( TRDTNL_IND ) AS isTraditional ,\r\n" + 
				"  MAXIMUM ( ENHNCD_IND ) AS isEnhanced, MAXIMUM ( EXPNDD_IND ) AS isExpanded ,\r\n" + 
				"( CASE \r\n" + 
				" WHEN max(TRDTNL_IND) = 1 THEN 'Primary'\r\n" + 
				"WHEN max(TRDTNL_IND) = 0  AND max(ENHNCD_IND) = 1 THEN 'Enhanced'\r\n" + 
				"WHEN max(TRDTNL_IND) = 0 AND max(ENHNCD_IND) = 0  AND max(EXPNDD_IND) = 1 THEN 'Comprehensive Only' \r\n" + 
				" ELSE 'Not Engaged' \r\n" + 
				"END  ) AS EngagementType \r\n" + 
				"FROM CII_CLNCL_ENGGMNT_DTL fact \r\n" + 
				"INNER JOIN \r\n" + 
				"(SELECT \r\n" + 
				"YEAR_ID ,\r\n" + 
				"(CASE WHEN YEAR_ID=1 THEN  201809 WHEN YEAR_ID=2 THEN 201709 ELSE  201609 END) AS StartYearMonth\r\n" + 
				",(CASE WHEN YEAR_ID=1 THEN  201908 WHEN YEAR_ID=2 THEN  201808 ELSE  201708 END) AS EndYearMonth\r\n" + 
				"FROM ACIISST_PERIOD_KEY\r\n" + 
				"WHERE LKUP_ID=1 AND YEAR_ID <= 1\r\n" + 
				") tp ON  fact.YEAR_MNTH_NBR >=tp.StartYearMonth and  fact.YEAR_MNTH_NBR <=tp.EndYearMonth \r\n" + 
				" AND fact.ACCT_ID = 'W0004156' \r\n" + 
				"GROUP BY MCID, fact.ACCT_ID, YEAR_ID\r\n" + 
				")EN_PRD ON CLM_MBRSHP.MCID = EN_PRD.MCID AND CLM_MBRSHP.ACCT_ID = EN_PRD.ACCT_ID \r\n" + 
				"AND CLM_MBRSHP.YEAR_ID = EN_PRD.YEAR_ID\r\n" + 
				"GROUP BY\r\n" + 
				"CLM_MBRSHP.YEAR_ID ,\r\n" + 
				"CLM_MBRSHP.MBR_CVRG_TYPE_CD ,\r\n" + 
				"CLM_MBRSHP.AGE_BAND_DESC ,\r\n" + 
				"CLM_MBRSHP.MBR_GNDR_CD ,\r\n" + 
				"CLM_MBRSHP.RPTG_MBR_RLTNSHP_CD ,\r\n" + 
				"CLM_MBRSHP.CBSA_NM  ,\r\n" + 
				"CLM_MBRSHP.ST_CD ,\r\n" + 
				"CLM_MBRSHP.ZIP_CD ,\r\n" + 
				"BDTC_CTGRY_CD,\r\n" + 
				"HLTH1.RISK_STG_VAL_TXT ,\r\n" + 
				"(CASE WHEN HCC_COST >= 25000 THEN 'Yes'  ELSE 'No'   \r\n" + 
				"       END),\r\n" + 
				"COALESCE(EN_PRD.IsTraditional, 0) , \r\n" + 
				"COALESCE(EN_PRD.IsEnhanced, 0) , \r\n" + 
				"COALESCE(EN_PRD.IsExpanded, 0) , \r\n" + 
				"COALESCE(EN_PRD.EngagementType, 'Not Engaged'),\r\n" + 
				"Report_Display";
		
		
		ProjectionOnlySqlParser parser = new ProjectionOnlySqlParser();
		parser.processQuery(query);
		
	}
}


