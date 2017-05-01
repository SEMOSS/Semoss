package prerna.sablecc2.reactor.planner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.util.ArrayUtilityMethods;

public class LoadClient extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	public static final String ASSIGNMENT_NOUN = "assignment";
	public static final String VALUE_NOUN = "value";
	public static final String SEPARATOR_NOUN = "separator";

	private int total = 0;
	private int error = 0;
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return this.parentReactor;
	}

	@Override
	public NounMetadata execute()
	{
		// run through all the results in the iterator
		// and append them into a new plan
		PKSLPlanner newPlan = createPlanner();
		return new NounMetadata(newPlan, PkslDataTypes.PLANNER);
	}
	
	
	private PKSLPlanner createPlanner() {
		// generate our lazy translation
		// which only ingests the routines
		// without executing
		PlannerTranslation plannerT = new PlannerTranslation();
		
		// get the iterator we are loading
		IRawSelectWrapper iterator = (IRawSelectWrapper) getIterator();
		String[] headers = iterator.getDisplayVariables();
		
		int[] assignmentIndices = getAssignmentIndices(headers);
		int valIndex = getValueIndex(headers);
		int typeIndex = getTypeIndex(headers);
		String separator = getSeparator();

		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			Object[] values = nextData.getValues();
			
			//grab the assignment variable, or the alias
			String assignment = getAssignment(values, assignmentIndices, separator);
			
			//grab the value we are assigning to that variable/alias
			String value = getValue(values, valIndex);
			
			//if the value is a formula add to the pksl planner
			if(isFormula(values, typeIndex)) {
				String pkslString = generatePKSLString(assignment, value);
				System.out.println(pkslString);
				addPkslToPlanner(plannerT, pkslString);
			} 
			
			//else we just want to add the value of the constant/decimal directly to the planner
			else {
				System.out.println(assignment+"::"+value);
				if(assignment.equalsIgnoreCase("A1120_PG_1_MAPPING__18_INTEREST")) {
					System.out.println("abc");
				}
				addVariable(plannerT.planner, assignment, value);
			}
		}
		
//		for(String pkslString : getUndefinedVariablesPksls(plannerT.planner)) {
//			try {
//				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
//				Start tree = p.parse();
//				tree.apply(plannerT);
//			} catch (ParserException | LexerException | IOException e) {
//				e.printStackTrace();
//			}
//			total++;
//		}
//		
//		for(String pkslString : getRemainingPksls()) {
//			plannerT.planner.addVariable(pkslString, new NounMetadata(0, PkslDataTypes.CONST_DECIMAL));
//			total++;
//		}
		
		// grab the planner from the new translation
		LOGGER.info("****************    "+total+"      *************************");
		LOGGER.info("****************    "+error+"      *************************");
		
		return plannerT.planner;
	}
	
	private void addPkslToPlanner(PlannerTranslation plannerT, String pkslString) {
		// as we iterate through
		// run the values through the planner
//		String fileName = "C:\\Workspace\\Semoss_Dev\\failedToAddpksls.txt";
//		BufferedWriter bw = null;
//		FileWriter fw = null;
//		
//		try {
//			fw = new FileWriter(fileName);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		bw = new BufferedWriter(fw);
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
			Start tree = p.parse();
			tree.apply(plannerT);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
			error++;
//			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//			System.out.println(pkslString);
//			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//			try {
//				bw.write("PARSE ERROR::::   "+pkslString+"\n");
//			} catch (IOException e1) {
//				e1.printStackTrace();
//			}
		} catch(Exception e) {
			e.printStackTrace();
			error++;
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			System.out.println(pkslString);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//			try {
//				bw.write("EVAL ERROR::::   "+pkslString+"\n");
//			} catch (IOException e1) {
//				e1.printStackTrace();
//			}
		}
		total++;
		
//		try {
//		bw.close();
//		fw.close();
//	} catch (IOException e) {
//		e.printStackTrace();
//	}
	}
	
	/**
	 * 
	 * @param planner
	 * @param assignment
	 * @param value
	 * 
	 * This method will directly add the variable to the planner instead of adding an assignment pksl
	 */
	private void addVariable(PKSLPlanner planner, String assignment, Object value) {
		if(value instanceof Number) {
			NounMetadata noun = new NounMetadata(((Number)value).doubleValue(), PkslDataTypes.CONST_DECIMAL);
			planner.addVariable(assignment, noun);
		} else if(value instanceof String) {
			if(value.toString().trim().startsWith("\"") || value.toString().trim().startsWith("\'")) {
				//we have a literal
				String literal = value.toString().trim();
				literal = literal.substring(1, literal.length()-1).trim();
				NounMetadata noun = new NounMetadata(literal, PkslDataTypes.CONST_STRING);
				planner.addVariable(assignment, noun);
			} else {
				// try to convert to a number
				try {
					double doubleValue = Double.parseDouble(value.toString().trim());
					NounMetadata noun = new NounMetadata(doubleValue, PkslDataTypes.CONST_DECIMAL);
					planner.addVariable(assignment, noun);
				} catch(NumberFormatException e) {
					// confirmed that it is not a double
					// and that we have a column
					NounMetadata noun = new NounMetadata(value.toString().trim(),PkslDataTypes.COLUMN);
					planner.addVariable(assignment, noun);
				}
			}
		} else {
			//i don't know
			System.out.println("Defining a variable that is not a number or string!");
		}
	}
	
	/**
	 * 
	 * @param assignment
	 * @param value
	 * @return
	 * 
	 * returns a pksl query given an assignment variable and the value
	 */
	private String generatePKSLString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	private boolean isFormula(Object[] values, int typeIndex) {
		String value = values[typeIndex].toString();
		return "formula".equalsIgnoreCase(value);
	}

	
	/**
	 * 
	 * @param values
	 * @param assignmentIndices
	 * @param separator
	 * @return
	 */
	private String getAssignment(Object[] values, int[] assignmentIndices, String separator) {
		StringBuilder pkslBuilder = new StringBuilder();
		int numAssignments = assignmentIndices.length;
		for(int i = 0; i < assignmentIndices.length; i++) {
			// add the name
			pkslBuilder.append(values[assignmentIndices[i]]);
			if( (i+1) != numAssignments) {
				// concatenate the multiple ones with the defined value
				pkslBuilder.append(separator);
			}
		}
		
		return pkslBuilder.toString();
	}
	
	
	private String getValue(Object[] values, int valIndex) {
		return values[valIndex].toString().trim();
	}
	
//	private List<String> getRemainingPksls() {
//		List<String> pksls = new ArrayList<>();
//		// from roots
//		pksls.add("aSch_C_Mapping__Dividends_on_certain_preferred_stock_of_20__or_more_owned_public_utilities__");
//		pksls.add("aLine_26___Other_Ded__Impact__Total");
//		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_stock_described_in_Regulations_section_1_7874_1_d__that_it_holds_in_a_corporation_to_whom_it_paid_disqualified_interest__If__Yes___enter_the_adjusted_basis_of_that_stock");
//		pksls.add("aSch_C_Mapping__Dividends_from_domestic_corporations_received_by_a_small_business_investment_company_operating_under_the_Small_Business_Investment_Act_of_195");
//		pksls.add("aSch_C_Mapping__Dividends_from_certain_FSCs");
//		pksls.add("aForm_8926_Mapping__Adjusted_taxable_income__Combine_lines_3a_through_3f__If_zero_or_less__enter__0_");
//		pksls.add("aForm_8926_Mapping__Enter_any_disqualified_interest_paid_or_accrued_by_the_corporation_on_indebtedness_subject_to_a_disqualified_guarantee");
//		pksls.add("aCurrent_Provision_Mapping__Border_Adjustability");
//		pksls.add("aCurrent_Provision_Mapping__Amortization");
//		pksls.add("aSch_C_Mapping__Dividends_from_affiliated_group_members_");
//		pksls.add("aSch_J_Impact__21__Total_payments_and_credits_");
//		pksls.add("aSch_C_Mapping__Dividends_on_debt_financed_stock_of_domestic_and_foreign_corporations");
//		pksls.add("aSch_C_Mapping__Dividends_from_less_than_20__owned_domestic_corporations__other_than_debt_financed_stock_");
//		pksls.add("aForm_8926_Mapping__Enter_any_interest_paid_or_accrued_by_a_taxable_REIT_subsidiary__as_defined_in_section_856_l___of_a_real_estate_investment_trust_to_such_trust");
//		pksls.add("aSch_C_Mapping__Dividends_from_less_than_20__owned_foreign_corporations_and_certain_FSCs");
//		pksls.add("a1120_Pg_1_Mapping__Form_1125_A_Ln_7_Inventory_at_End_of_Year");
//		pksls.add("aCurrent_Provision_Mapping__Full_Expensing_of_Assets");
//		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_stock_it_holds_in_foreign_subsidiaries__If__Yes___enter_the_adjusted_basis_of_that_stock");
//		pksls.add("aForm_8926_Mapping__Enter_any_disqualified_interest_disallowed_under_section_163_j__for_prior_tax_years_that_is_treated_as_paid_or_accrued_in_the_current_tax_year");
//		pksls.add("aSch_C_Mapping__Dividends_from_20__or_more_owned_foreign_corporations_and_certain_FSCs");
//		pksls.add("aForm_8926_Mapping__Enter_any_additional_adjustments_the_corporation_has_made_to_its_taxable_income__loss___other_than_those_listed_on_lines_3b_through_3e_above__in_arriving_at_its_adjusted_taxable_income__see_instructions_attach_schedule_");
//		pksls.add("aForm_8926_Impact__Amount_of_interest_deduction_disallowed_under_section_163_j__for_the_current_tax_year_and_carried_forward_to_the_next_tax_year");
//		pksls.add("aCurrent_Prov_Impact__Statutory_Rate");
//		pksls.add("aForm_8926_Mapping__Amount_of_interest_deduction_disallowed_under_section_163_j__for_the_current_tax_year_and_carried_forward_to_the_next_tax_year");
//		pksls.add("aSch_C_Mapping__Dividends_from_wholly_owned_foreign_subsidiaries");
//		pksls.add("aForm_8926_Mapping__Unused_excess_limitation_carryforward_from_the_prior_2_tax_years");
//		pksls.add("aCurrent_Provision_Mapping__Business_Expenses__Book_Basis_");
//		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_any_intangible_assets__If__Yes___enter_the_adjusted_basis_of_those_intangible_assets");
//		pksls.add("aLine_26___Other_Deductions__Total_");
//		pksls.add("aForm_8926_Mapping__Excess_Interest_Expense_Carry_Forward");
//		pksls.add("aForm_8926_Impact__Enter_the_interest_paid_or_accrued_by_the_corporation_for_the_tax_year");
//		pksls.add("aForm_8926_Mapping__Enter_any_unused_excess_limitation_carried_forward_to_the_current_tax_year_from_the_prior_3_tax_years");
//		pksls.add("aForm_8926_Impact__Adjusted_taxable_income__Combine_lines_3a_through_3f__If_zero_or_less__enter__0_");
//		pksls.add("aForm_4562_Impact__28__Add_lines_25_through_27__enter_on_line_21_above_");
//		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_tangible_assets_it_directly_holds_that_are_located_in_a_foreign_country___see_instructions__If__Yes___enter_the_adjusted_basis_of_those_tangible_assets");
//		pksls.add("aSch_C_Mapping__Dividends_from_20__or_more_owned_domestic_corporations__other_than_debt_financed_stock______");
//		
//		pksls.add("aForm_8926_Impact__Debt_to_equity_ratio__Divide_line_1d_by_line_1e__see_instructions_= 0;");
//		
//		// other standalone nouns
//		pksls.add("aCurrent_Provision_Mapping__Other_Taxes");
//		pksls.add("aForm_3800_Impact__29__Subtract_line_28_from_27");
//		pksls.add("aForm_3800_Forecasting__29__Subtract_line_28_from_27");
//		pksls.add("aCurrent_Prov_Impact__Current_Federal_Income_Tax_Expense");
//		pksls.add("aCurrent_Prov_Impact__Other_Taxes");
//		pksls.add("aCurrent_Forecasting__Taxable_Income");
//		pksls.add("B");
//		pksls.add("aForm_8926_Impact__Enter_any_disqualified_interest_paid_or_accrued_by_the_corporation_to_a_related_person");
//		pksls.add("aForm_4562_Impact__12__Section_179_expense_deduction__add_lines_9_and_10_but_no_more_than_11_");
//		pksls.add("aCurrent_Forecasting__Pre_Tax_Book_Income__Loss_");
//		pksls.add("aForm_8827_Impact__7a__Excess_Regular_Tax_Liability_over_TMT");
//		pksls.add("aForm_3800_Impact__13__25__of_excess_of_line_12_over__25_000");
//		pksls.add("aSch_J_Impact__19a__Refundable_credits__Form_2439_");
//		pksls.add("aForm_3800_Forecasting__21__Subtract_line_17_from_line_20");
//		pksls.add("aCurrent_Forecasting__Other_Taxes");
//		pksls.add("aCurrent_Forecasting__Interest_expense");
//		pksls.add("aForm_3800_Mapping__21__Subtract_line_17_from_line_20");
//		pksls.add("aForm_3800_Forecasting__16__Subtract_line_15_from_line_11");
//		pksls.add("aSec__199_Forecasting__21__W_2_Wage_Limitation");
//		pksls.add("aSch_C_Mapping__Other_dividends_");
//		pksls.add("aSch_C_Mapping__IC_DISC_and_former_DISC_dividends_not_included_on_line_1__2__or_3");
//		pksls.add("aSch_C_Mapping__Foreign_dividend_gross_up_");
//		pksls.add("aSch_C_Mapping__Income_from_controlled_foreign_corporations_under_subpart_F__attach_Form_s__5471_");
//		pksls.add("aSch_C_Mapping__Dividends_on_certain_preferred_stock_of_less_than_20__owned_public_utilities");
//		pksls.add("aSch_C_Mapping__Total__Add_lines_1_through_8__See_instructions_for_limitation");
//		pksls.add("aForm_3800_Mapping__16__Subtract_line_15_from_line_11");
//		pksls.add("aCurrent_Provision_Mapping__Taxable_Income");
//		pksls.add("aForm_8903_Mapping__11__Taxable_Income__before_199_");
//		pksls.add("aForm_3800_Mapping__13__25__of_excess_of_line_12_over__25_000");
//		pksls.add("G");
//		pksls.add("aForm_8903_Mapping__21__W_2_Wage_Limitation");
//		pksls.add("aForm_3800_Mapping__29__Subtract_line_28_from_27");
//		pksls.add("aForm_3800_Impact__16__Subtract_line_15_from_line_11");
//		pksls.add("aForm_3800_Impact__26__Empowerment_zone_and_renewal_community_employment_credit");
//		pksls.add("aForm_3800_Mapping__13__25__of_excess_of_line_12_over__25_000");
//		pksls.add("aForm_8827_Forecasting__7a__Excess_Regular_Tax_Liability_over_TMT");
//		pksls.add("aForm_3800_Forecasting__13__25__of_excess_of_line_12_over__25_000");
//		pksls.add("aForm_8827_Mapping__7a__Excess_Regular_Tax_Liability_over_TMT");
//		
//		return pksls;
//	}

	
	/**************************** START GET PARAMETERS *****************************************/
	/**
	 * Get the optional separator if defined
	 * @return
	 */
	private String getSeparator() {
		String separator = "";
		// this is an optional key
		// if we need to concatenate multiple things together
		if(this.store.getNounKeys().contains(SEPARATOR_NOUN)) {
			separator = this.store.getNoun(SEPARATOR_NOUN).get(0).toString();
		}
		return separator;
	}

	/**
	 * Get the job input the reactor
	 * @return
	 */
	private Iterator getIterator() {
		List<Object> jobs = this.curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs != null && jobs.size() > 0) {
			Job job = (Job)jobs.get(0);
			return job.getIterator();
		}

		Job job = (Job) this.store.getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}

	/**
	 * Get the unique identifier for the group of pksl scripts to load
	 * @param iteratorHeaders
	 * @return
	 */
	private int[] getAssignmentIndices(String[] iteratorHeaders) {
		// assumption that we have only one column which contains the pksl queries
		// TODO: in future, maybe allow for multiple and do a "|" between them?
		GenRowStruct assignments = this.store.getNoun(ASSIGNMENT_NOUN);
		int numAssignmentCols = assignments.size();
		int[] assignmentIndices = new int[numAssignmentCols];
		for(int index = 0; index < numAssignmentCols; index++) {
			String assignmentName = assignments.get(index).toString();
			assignmentIndices[index] = ArrayUtilityMethods.arrayContainsValueAtIndex(iteratorHeaders, assignmentName);
		}
		return assignmentIndices;
	}
	
	/**
	 * Get the index which contains the pksl scripts to load
	 * @return
	 */
	private int getValueIndex(String[] iteratorHeaders) {
		// assumption that we have only one column which contains the pksl queries
		// TODO: in future, maybe allow for multiple and do a "|" between them?
		String valueName = this.store.getNoun(VALUE_NOUN).get(0).toString();
		int valueIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(iteratorHeaders, valueName);
		return valueIndex;
	}
	
	private int getTypeIndex(String[] iteratorHeaders) {
		String typeName = "Type_1";
		int typeIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(iteratorHeaders, typeName);
		return typeIndex;
	}
	
	/*****************END GET PARAMETERS********************/

}
