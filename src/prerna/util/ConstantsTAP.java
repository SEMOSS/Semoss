/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

/**
 * This class contains the constants referenced elsewhere in the code that are TAP specific.
 */
public final class ConstantsTAP {
	
	private ConstantsTAP() {
		
	}
	
	// Distance Downstream
	public static final String INSERT_DOWNSTREAM_BUTTON = "btnInsertDownstream";
	public static final String DISTANCE_DOWNSTREAM_QUERY = "DISTANCE_DOWNSTREAM_QUERY";
	public static final String SOA_ALPHA_VALUE_TEXT_BOX = "soaAlphaValueTextField";
	public static final String APPRECIATION_TEXT_BOX = "appreciationValueTextField";
	public static final String DEPRECIATION_TEXT_BOX = "depreciationValueTextField";

	public static final String INSERT_SYS_SUSTAINMENT_BUDGET_BUTTON = "btnInsertBudgetProperty";
	public static final String SYSTEM_SUSTAINMENT_BUDGET_INSERT_QUERY = "SYSTEM_SUSTAINMENT_BUDGET_INSERT_QUERY";
	
	// Update Active Systems Constants
	public static final String GET_ALL_SYSTEMS_QUERY = "GET_ALL_SYSTEMS_QUERY";
	public static final String GET_DECOMMISSIONED_SYSTEMS_QUERY = "GET_ALL_DECOMMISSIONED_SYSTEMS_QUERY";
	
	// TAP Service Aggregation to Core
	public static final String TAP_SERVICES_AGGREGATION_BUTTON = "btnAggregateTapServicesIntoTapCore";
	public static final String TAP_SERVICES_AGGREGATION_SERVICE_COMBO_BOX = "selectTapServicesComboBox";
	public static final String TAP_SERVICES_AGGREGATION_CORE_COMBO_BOX = "selectTapCoreForAggregationComboBox";
	
	// Create Future Interface DB
	public static final String TAP_Core_Data_FUTURE_INTERFACE_DATABASE_CORE_COMBO_BOX = "selectHRCoreForFutureInterfaceDBComboBox";
	public static final String TAP_FUTURE_INTERFACE_DATABASE_COMBO_BOX = "selectFutureInterfaceComboBox";
	public static final String TAP_FUTURE_COST_INTERFACE_DATABASE_COMBO_BOX = "selectFutureCostInterfaceComboBox";
	public static final String TAP_FUTURE_INTERFACE_DATABASE_BUTTON = "btnCreateFutureInterfaceDatabase";

	// Central System Sys-BP Sys-Activity Aggregation Thresholds
	public static final String DATA_OBJECT_THRESHOLD_VALUE_TEXT_BOX = "dataObjectThresholdValueTextField";
	public static final String BLU_THRESHOLD_VALUE_TEXT_BOX = "bluThresholdValueTextField";
	
	// EA Properties to TAP_Core_Data
	public static final String TAP_EA_PROPERTY_CREATOR_BUTTON = "addEAPropertyButton";	
	
	// Source Select Report Queries
	public static final String RFP_NAME_FIELD = "RFPNameField";
	public static final String FUNCTIONAL_AREA_CHECKBOX_1 = "HSDCheckBox";
	public static final String FUNCTIONAL_AREA_CHECKBOX_2 = "HSSCheckBox";
	public static final String FUNCTIONAL_AREA_CHECKBOX_3 = "FHPCheckBox";
	public static final String FUNCTIONAL_AREA_CHECKBOX_4 = "DHMSMCheckBox";
	public static final String SOURCE_SELECT_PANEL = "sourceSelectPanel";
	public static final String SOURCE_SELECT_REPORT_QUERY = "SOURCE_SELECT_REPORT_QUERY";
	public static final String SOURCE_SELECT_REPORT_QUERY_HSD = "SOURCE_SELECT_REPORT_QUERY_HSD";
	public static final String SOURCE_SELECT_REPORT_QUERY_HSS = "SOURCE_SELECT_REPORT_QUERY_HSS";
	public static final String SOURCE_SELECT_REPORT_QUERY_FHP = "SOURCE_SELECT_REPORT_QUERY_FHP";
	public static final String SOURCE_SELECT_REPORT_QUERY_DHMSM = "SOURCE_SELECT_REPORT_QUERY_DHMSM";
	public static final String SOURCE_SELECT_REPORT_NAME = "SOURCE_SELECT_REPORT_NAME";
	public static final String SOURCE_SELECT_REPORT_TASK_EFFECT_QUERY = "SOURCE_SELECT_REPORT_TASK_EFFECT_QUERY";
	
	// Analysis of Vendors' Response to RFP
	public static final String VENDOR_BV_TV_QUERY = "VENDOR_BV_TV_QUERY";
	public static final String VENDOR_CUSTOM_COST_QUERY = "VENDOR_CUSTOM_COST_QUERY";
	public static final String VENDOR_HWSW_COST_QUERY = "VENDOR_HWSW_COST_QUERY";
	
	public static final String VENDOR_FULFILL_LEVEL_1 = "VENDOR_FULFILL_LEVEL_SUPPORTS_OUT_OF_BOX";
	public static final String VENDOR_FULFILL_LEVEL_2 = "VENDOR_FULFILL_LEVEL_SUPPORTS_WITH_CONFIGURATION";
	public static final String VENDOR_FULFILL_LEVEL_3 = "VENDOR_FULFILL_LEVEL_SUPPORTS_WITH_CUSTOMIZATION";
	public static final String VENDOR_FULFILL_LEVEL_4 = "VENDOR_FULFILL_LEVEL_DOES_NOT_SUPPORT";
	
	public static final String VENDOR_HEAT_MAP_HTML = "VENDOR_HEAT_MAP_HTML";
	public static final String VENDOR_HEAT_MAP_REQUIREMENTS_QUERY = "VENDOR_HEAT_MAP_REQUIREMENTS_QUERY";
	

	// Fact Sheet Report Queries
	public static final String FACT_SHEET_REPORT_SYSTEM_TOGGLE_COMBO_BOX = "factSheetReportSystemToggleComboBox";
	public static final String FACT_SHEET_REPORT_TYPE_TOGGLE_COMBO_BOX = "factSheetReportTypeToggleComboBox";
	public static final String FACT_SHEET_SYSTEM_DROP_DOWN_PANEL = "factSheetReportSysDropDownPanel";
	public static final String FACT_SHEET_SYSTEM_SELECT_COMBO_BOX = "factSheetReportSysComboBox";
	
	// Fact Sheet Constants
	public static final String SYSTEM_SW_QUERY = "SYSTEM_SW_QUERY";
	public static final String SYSTEM_HW_QUERY = "SYSTEM_HW_QUERY";
	public static final String SYSTEM_MATURITY_QUERY = "SYSTEM_MATURITY_QUERY";
	public static final String LIST_OF_INTERFACES_QUERY = "LIST_OF_INTERFACES_QUERY";
	public static final String DATA_PROVENANCE_QUERY = "DATA_PROVENANCE_QUERY";
	public static final String BUSINESS_LOGIC_QUERY = "BUSINESS_LOGIC_QUERY";
	public static final String SITE_LIST_QUERY = "SITE_LIST_QUERY";
	public static final String BUDGET_QUERY = "BUDGET_QUERY";
	public static final String SYS_SIM_QUERY = "SYS_SIM_QUERY";
	public static final String SYS_QUERY = "SYS_QUERY";
	public static final String POC_QUERY = "POC_QUERY";
	public static final String VALUE_QUERY = "VALUE_QUERY";
	public static final String UNIQUE_DATA_PROVENANCE_QUERY = "UNIQUE_DATA_PROVENANCE_QUERY";
	public static final String UNIQUE_BUSINESS_LOGIC_QUERY = "UNIQUE_BUSINESS_LOGIC_QUERY";
	public static final String SYSTEM_DESCRIPTION_QUERY = "SYSTEM_DESCRIPTION_QUERY";
	public static final String SYSTEM_NAME_QUERY = "SYSTEM_NAME_QUERY";
	public static final String SYSTEM_HIGHLIGHTS_QUERY = "SYSTEM_HIGHLIGHTS_QUERY";
	public static final String USER_TYPES_QUERY = "USER_TYPES_QUERY";
	public static final String USER_INTERFACES_QUERY = "USER_INTERFACES_QUERY";
	public static final String BUSINESS_PROCESS_QUERY = "BUSINESS_PROCESS_QUERY";
	public static final String PPI_QUERY = "PPI_QUERY";
	public static final String CAPABILITIES_SUPPORTED_QUERY = "CAPABILITIES_SUPPORTED_QUERY";
	public static final String LPI_SYSTEMS_QUERY = "LPI_SYSTEMS_QUERY";
	public static final String LPNI_SYSTEMS_QUERY = "LPNI_SYSTEMS_QUERY";
	public static final String HIGH_SYSTEMS_QUERY = "HIGH_SYSTEMS_QUERY";
	public static final String REFERENCE_REPOSITORY_QUERY = "REFERENCE_REPOSITORY_QUERY";
	public static final String RTM_QUERY = "RTM_QUERY";
	public static final String DHMSM_DATA_PROVIDED_PERCENT = "DHMSM_DATA_PROVIDED_PERCENT";
	public static final String DHMSM_BLU_PROVIDED_PERCENT = "DHMSM_BLU_PROVIDED_PERCENT";	
	
	// Tasker Generation Constants
	public static final String TASKER_GENERATION_SYSTEM_COMBO_BOX = "taskerGenerationSysComboBox";
	public static final String ACTIVITY_QUERY = "ACTIVITY_QUERY";
	public static final String BLU_QUERY = "BLU_QUERY";
	public static final String DATA_QUERY = "DATA_QUERY";
	public static final String TERROR_QUERY = "TERROR_QUERY";
		
	// Capability Fact Sheet Constants
	public static final String CAPABILITY_GROUP_QUERY = "CAPABILITY_GROUP_QUERY";
	public static final String MISSION_OUTCOME_QUERY = "MISSION_OUTCOME_QUERY";
	public static final String CONOPS_SOURCE_QUERY = "CONOPS_SOURCE_QUERY";
	public static final String TASK_COUNT_QUERY = "TASK_COUNT_QUERY";
	public static final String BP_COUNT_QUERY = "BP_COUNT_QUERY";
	public static final String BR_COUNT_QUERY = "BR_COUNT_QUERY";
	public static final String BS_COUNT_QUERY = "BS_COUNT_QUERY";
	public static final String TR_COUNT_QUERY = "TR_COUNT_QUERY";
	public static final String TS_COUNT_QUERY = "TS_COUNT_QUERY";
	public static final String DATA_COUNT_QUERY = "DATA_COUNT_QUERY";
	public static final String BLU_COUNT_QUERY = "BLU_COUNT_QUERY";
	public static final String SYSTEM_COUNT_QUERY = "SYSTEM_COUNT_QUERY";
	public static final String FUNCTIONAL_GAP_COUNT_QUERY = "FUNCTIONAL_GAP_COUNT_QUERY";
	public static final String PARTICIPANT_QUERY = "PARTICIPANT_QUERY";
	public static final String DATE_GENERATED_QUERY = "DATE_GENERATED_QUERY";
	public static final String CAPABILITY_SIM_QUERY = "CAPABILITY_SIM_QUERY";
	public static final String CAPABILITY_QUERY = "CAPABILITY_QUERY";
	public static final String DATA_OBJECT_QUERY = "DATA_OBJECT_QUERY";
	public static final String FUNCTIONAL_GAP_QUERY = "FUNCTIONAL_GAP_QUERY";
	public static final String TASK_QUERY = "TASK_QUERY";
	public static final String BP_QUERY = "BP_QUERY";
	public static final String BR_QUERY = "BR_QUERY";
	public static final String BS_QUERY = "BS_QUERY";
	public static final String TR_QUERY = "TR_QUERY";
	public static final String TS_QUERY = "TS_QUERY";
	
	public static final String CAP_PROVIDE_SYSTEM_PROVIDE_DATA_QUERY = "CAP_PROVIDE_SYSTEM_PROVIDE_DATA_QUERY";
	public static final String CAP_PROVIDE_SYSTEM_CONSUME_DATA_QUERY = "CAP_PROVIDE_SYSTEM_CONSUME_DATA_QUERY";
	public static final String CAP_CONSUME_SYSTEM_PROVIDE_DATA_QUERY = "CAP_CONSUME_SYSTEM_PROVIDE_DATA_QUERY";
	public static final String CAP_PROVIDE_SYSTEM_PROVIDE_DATA_COUNT_QUERY = "CAP_PROVIDE_SYSTEM_PROVIDE_DATA_COUNT_QUERY";
	public static final String CAP_PROVIDE_SYSTEM_CONSUME_DATA_COUNT_QUERY = "CAP_PROVIDE_SYSTEM_CONSUME_DATA_COUNT_QUERY";
	public static final String CAP_CONSUME_SYSTEM_PROVIDE_DATA_COUNT_QUERY = "CAP_CONSUME_SYSTEM_PROVIDE_DATA_COUNT_QUERY";
	public static final String CAP_PROVIDE_SYSTEM_PROVIDE_MISSING_DATA_QUERY = "CAP_PROVIDE_SYSTEM_PROVIDE_MISSING_DATA_QUERY";
	public static final String CAP_CONSUME_SYSTEM_PROVIDE_MISSING_DATA_QUERY = "CAP_CONSUME_SYSTEM_PROVIDE_MISSING_DATA_QUERY";
	public static final String CAP_PROVIDE_SYSTEM_PROVIDE_MISSING_DATA_COUNT_QUERY = "CAP_PROVIDE_SYSTEM_PROVIDE_MISSING_DATA_COUNT_QUERY";
	public static final String CAP_CONSUME_SYSTEM_PROVIDE_MISSING_DATA_COUNT_QUERY = "CAP_CONSUME_SYSTEM_PROVIDE_MISSING_DATA_COUNT_QUERY";
	
	// forms source file aggregation
	public static final String FORMS_SOURCE_FILE_AGGREGATION_COMBO_BOX = "FORMS_SOURCE_FILE_AGGREGATION_COMBO_BOX";
	public static final String FORMS_SOURCE_FILE_DIRECTORY = "FORMS_SOURCE_FILE_DIRECTORY";
	
	// Relationship Insert Constants
	public static final String LOGIC_TYPE = "relInferLogicTypeComboBox";
	
	
	
}
