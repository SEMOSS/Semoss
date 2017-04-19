package prerna.sablecc2.reactor.planner;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.sablecc2.reactor.storage.MapStore;
import prerna.util.ArrayUtilityMethods;

public class LoadClient extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	public static final String ASSIGNMENT_NOUN = "assignment";
	public static final String VALUE_NOUN = "value";
	public static final String SEPARATOR_NOUN = "separator";

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
		//replace this planner with generated planner
//		this.planner = newPlan;

		return new NounMetadata(newPlan, PkslDataTypes.PLANNER);
	}
	
	
	private PKSLPlanner createPlanner() {
		// generate our lazy translation
		// which only ingests the routines
		// without executing
		PlannerTranslation plannerT = new PlannerTranslation();
		
		// get the iterator we are loading
		IRawSelectWrapper iterator = (IRawSelectWrapper)getIterator();
		String[] headers = iterator.getDisplayVariables();
		
		int[] assignmentIndices = getAssignmentIndices(headers);
		int valIndex = getValueIndex(headers);
		String separator = getSeparator();
		
		// as we iterate through
		// run the values through the planner
		String fileName = "C:\\Workspace\\Semoss_Dev\\failedToAddpksls.txt";
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(fileName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		
		int count = 0;
		int total = 0;
		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			String pkslString = generatePKSLString(nextData.getValues(), assignmentIndices, valIndex, separator);
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(plannerT);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
				count++;
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				try {
					bw.write("PARSE ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			} catch(Exception e) {
				e.printStackTrace();
				count++;
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				try {
					bw.write("EVAL ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			total++;
		}
		
		for (String pkslString : getRemainingPksls()) {
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(plannerT);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
				count++;
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				try {
					bw.write("PARSE ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			} catch(Exception e) {
				e.printStackTrace();
				count++;
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println(pkslString);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				try {
					bw.write("EVAL ERROR::::   "+pkslString+"\n");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			total++;
		}
		
		try {
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// grab the planner from the new translation
		System.out.println("****************    "+total+"      *************************");
		System.out.println("****************    "+count+"      *************************");
		return plannerT.planner;
	}
	
	private String generatePKSLString(Object[] values, int[] assignmentIndices, int valIndex, String separator) {
		StringBuilder pkslBuilder = new StringBuilder();
		// add the assignment
		int numAssignments = assignmentIndices.length;
		for(int i = 0; i < numAssignments; i++) {
			// add the name
			pkslBuilder.append(values[assignmentIndices[i]]);
			if( (i+1) != numAssignments) {
				// concatenate the multiple ones with the defined value
				pkslBuilder.append(separator);
			}
		}
		// add the equals and value
		pkslBuilder.append(" = ").append(values[valIndex]).append(";");
		
		// return the new pksl
		return pkslBuilder.toString();
	}
	
	
	private List<String> getRemainingPksls() {
		List<String> pksls = new ArrayList<>();
		// from roots
		pksls.add("aSch_C_Mapping__Dividends_on_certain_preferred_stock_of_20__or_more_owned_public_utilities__ = 0;");
		pksls.add("aLine_26___Other_Ded__Impact__Total = 0;");
		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_stock_described_in_Regulations_section_1_7874_1_d__that_it_holds_in_a_corporation_to_whom_it_paid_disqualified_interest__If__Yes___enter_the_adjusted_basis_of_that_stock = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_domestic_corporations_received_by_a_small_business_investment_company_operating_under_the_Small_Business_Investment_Act_of_195 = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_certain_FSCs = 0;");
		pksls.add("aForm_8926_Mapping__Adjusted_taxable_income__Combine_lines_3a_through_3f__If_zero_or_less__enter__0_ = 0;");
		pksls.add("aForm_8926_Mapping__Enter_any_disqualified_interest_paid_or_accrued_by_the_corporation_on_indebtedness_subject_to_a_disqualified_guarantee = 0;");
		pksls.add("aCurrent_Provision_Mapping__Border_Adjustability = 0;");
		pksls.add("aCurrent_Provision_Mapping__Amortization = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_affiliated_group_members_ = 0;");
		pksls.add("aSch_J_Impact__21__Total_payments_and_credits_ = 0;");
		pksls.add("aSch_C_Mapping__Dividends_on_debt_financed_stock_of_domestic_and_foreign_corporations = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_less_than_20__owned_domestic_corporations__other_than_debt_financed_stock_ = 0;");
		pksls.add("aForm_8926_Mapping__Enter_any_interest_paid_or_accrued_by_a_taxable_REIT_subsidiary__as_defined_in_section_856_l___of_a_real_estate_investment_trust_to_such_trust = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_less_than_20__owned_foreign_corporations_and_certain_FSCs = 0;");
		pksls.add("a1120_Pg_1_Mapping__Form_1125_A_Ln_7_Inventory_at_End_of_Year = 0;");
		pksls.add("aCurrent_Provision_Mapping__Full_Expensing_of_Assets = 0;");
		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_stock_it_holds_in_foreign_subsidiaries__If__Yes___enter_the_adjusted_basis_of_that_stock = 0;");
		pksls.add("aForm_8926_Mapping__Enter_any_disqualified_interest_disallowed_under_section_163_j__for_prior_tax_years_that_is_treated_as_paid_or_accrued_in_the_current_tax_year = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_20__or_more_owned_foreign_corporations_and_certain_FSCs = 0;");
		pksls.add("aForm_8926_Mapping__Enter_any_additional_adjustments_the_corporation_has_made_to_its_taxable_income__loss___other_than_those_listed_on_lines_3b_through_3e_above__in_arriving_at_its_adjusted_taxable_income__see_instructions_attach_schedule_ = 0;");
		pksls.add("aForm_8926_Impact__Amount_of_interest_deduction_disallowed_under_section_163_j__for_the_current_tax_year_and_carried_forward_to_the_next_tax_year = 0;");
		pksls.add("aCurrent_Prov_Impact__Statutory_Rate = 0;");
		pksls.add("aForm_8926_Mapping__Amount_of_interest_deduction_disallowed_under_section_163_j__for_the_current_tax_year_and_carried_forward_to_the_next_tax_year = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_wholly_owned_foreign_subsidiaries = 0;");
		pksls.add("aForm_8926_Mapping__Unused_excess_limitation_carryforward_from_the_prior_2_tax_years = 0;");
		pksls.add("aCurrent_Provision_Mapping__Business_Expenses__Book_Basis_ = 0;");
		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_any_intangible_assets__If__Yes___enter_the_adjusted_basis_of_those_intangible_assets = 0;");
		pksls.add("aLine_26___Other_Deductions__Total_ = 0;");
		pksls.add("aForm_8926_Mapping__Excess_Interest_Expense_Carry_Forward = 0;");
		pksls.add("aForm_8926_Impact__Enter_the_interest_paid_or_accrued_by_the_corporation_for_the_tax_year = 0;");
		pksls.add("aForm_8926_Mapping__Enter_any_unused_excess_limitation_carried_forward_to_the_current_tax_year_from_the_prior_3_tax_years = 0;");
		pksls.add("aForm_8926_Impact__Adjusted_taxable_income__Combine_lines_3a_through_3f__If_zero_or_less__enter__0_ = 0;");
		pksls.add("aForm_4562_Impact__28__Add_lines_25_through_27__enter_on_line_21_above_ = 0;");
		pksls.add("aForm_8926_Mapping__Is_the_corporation_including_as_part_of_its_assets_on_line_1b_tangible_assets_it_directly_holds_that_are_located_in_a_foreign_country___see_instructions__If__Yes___enter_the_adjusted_basis_of_those_tangible_assets = 0;");
		pksls.add("aSch_C_Mapping__Dividends_from_20__or_more_owned_domestic_corporations__other_than_debt_financed_stock______ = 0;");
		
		pksls.add("aForm_8926_Impact__Debt_to_equity_ratio__Divide_line_1d_by_line_1e__see_instructions_= 0;");
		
		// other standalone nouns
		pksls.add("aCurrent_Provision_Mapping__Other_Taxes = 0;");
		pksls.add("aForm_3800_Impact__29__Subtract_line_28_from_27 = 0;");
		pksls.add("aForm_3800_Forecasting__29__Subtract_line_28_from_27 = 0;");
		pksls.add("aCurrent_Prov_Impact__Current_Federal_Income_Tax_Expense = 0;");
		pksls.add("aCurrent_Prov_Impact__Other_Taxes = 0;");
		pksls.add("aCurrent_Forecasting__Taxable_Income = 0;");
		pksls.add("B = 0;");
		pksls.add("aForm_8926_Impact__Enter_any_disqualified_interest_paid_or_accrued_by_the_corporation_to_a_related_person = 0;");
		pksls.add("aForm_4562_Impact__12__Section_179_expense_deduction__add_lines_9_and_10_but_no_more_than_11_ = 0;");
		pksls.add("aCurrent_Forecasting__Pre_Tax_Book_Income__Loss_ = 0;");
		pksls.add("aForm_8827_Impact__7a__Excess_Regular_Tax_Liability_over_TMT = 0;");
		pksls.add("aForm_3800_Impact__13__25__of_excess_of_line_12_over__25_000 = 0;");
		pksls.add("aSch_J_Impact__19a__Refundable_credits__Form_2439_ = 0;");
		pksls.add("aForm_3800_Forecasting__21__Subtract_line_17_from_line_20 = 0;");
		pksls.add("aCurrent_Forecasting__Other_Taxes = 0;");
		pksls.add("aCurrent_Forecasting__Interest_expense = 0;");
		pksls.add("aForm_3800_Mapping__21__Subtract_line_17_from_line_20 = 0;");
		pksls.add("aForm_3800_Forecasting__16__Subtract_line_15_from_line_11 = 0;");
		pksls.add("aSec__199_Forecasting__21__W_2_Wage_Limitation = 0;");
		pksls.add("aSch_C_Mapping__Other_dividends_ = 0;");
		pksls.add("aSch_C_Mapping__IC_DISC_and_former_DISC_dividends_not_included_on_line_1__2__or_3 = 0;");
		pksls.add("aSch_C_Mapping__Foreign_dividend_gross_up_ = 0;");
		pksls.add("aSch_C_Mapping__Income_from_controlled_foreign_corporations_under_subpart_F__attach_Form_s__5471_ = 0;");
		pksls.add("aSch_C_Mapping__Dividends_on_certain_preferred_stock_of_less_than_20__owned_public_utilities = 0;");
		pksls.add("aSch_C_Mapping__Total__Add_lines_1_through_8__See_instructions_for_limitation = 0;");
		pksls.add("aForm_3800_Mapping__16__Subtract_line_15_from_line_11 = 0;");
		pksls.add("aCurrent_Provision_Mapping__Taxable_Income = 0;");
		pksls.add("aForm_8903_Mapping__11__Taxable_Income__before_199_ = 0;");
		pksls.add("aForm_3800_Mapping__13__25__of_excess_of_line_12_over__25_000 = 0;");
		pksls.add("G = 0;");
		pksls.add("aForm_8903_Mapping__21__W_2_Wage_Limitation = 0;");
		pksls.add("aForm_3800_Mapping__29__Subtract_line_28_from_27 = 0;");
		pksls.add("aForm_3800_Impact__16__Subtract_line_15_from_line_11 = 0;");
		pksls.add("aForm_3800_Impact__26__Empowerment_zone_and_renewal_community_employment_credit = 0;");
		pksls.add("aForm_3800_Mapping__13__25__of_excess_of_line_12_over__25_000 = 0;");
		pksls.add("aForm_8827_Forecasting__7a__Excess_Regular_Tax_Liability_over_TMT = 0;");
		pksls.add("aForm_3800_Forecasting__13__25__of_excess_of_line_12_over__25_000 = 0;");
		pksls.add("aForm_8827_Mapping__7a__Excess_Regular_Tax_Liability_over_TMT = 0;");
		
		return pksls;
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

		Job job = (Job)this.getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}

	private InMemStore getInMemoryStore() {
		InMemStore inMemStore = null;
//		GenRowStruct grs = getNounStore().getNoun(this.IN_MEM_STORE);
//		if(grs != null) {
//			inMemStore = (InMemStore) grs.get(0);
//		} else {
			GenRowStruct grs = getNounStore().getNoun(PkslDataTypes.IN_MEM_STORE.toString());
			if(grs != null) {
				inMemStore = (InMemStore) grs.get(0);
			} else {
				inMemStore = new MapStore();
			}
//		}
		
		return inMemStore;
	}
	/*****************END GET PARAMETERS********************/
	
	
	/*****************DEBUG METHODS********************/
	//method to see the structure of the plan
	private void printPlan(PKSLPlanner planner) {
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		GraphTraversal<Vertex, Vertex> getAllV = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(getAllV.hasNext()) {
			// get the vertex
			Vertex v = getAllV.next();
			System.out.println(">> " + v.property(PKSLPlanner.TINKER_ID).value().toString());

			// all of the vertex inputs
			Iterator<Edge> inputEdges = v.edges(Direction.IN);
			while(inputEdges.hasNext()) {
				Edge inputE = inputEdges.next();
				Vertex inputV = inputE.outVertex();
				System.out.println("\tinput >> " + inputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}

			// all of the vertex inputs
			Iterator<Edge> outputEdges = v.edges(Direction.OUT);
			while(outputEdges.hasNext()) {
				Edge outputE = outputEdges.next();
				Vertex outputV = outputE.inVertex();
				System.out.println("\toutput >> " + outputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}
		}
	}

}
