/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Enumeration;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import prerna.ui.components.specific.tap.ServiceSelectPanel;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

//TODO: current error, when selecting a tier, select all option does not become unselected

/**
 * Selects which services to be selected in the ServiesSelectPanel based on their Tier
 */
public class ServiceTierSelectorListener extends AbstractListener {
	JCheckBox tierCheck1;
	JCheckBox tierCheck2;
	JCheckBox tierCheck3;

	/**
	 * Selects services based on the which Tier the user wants to include in the Transition Cost Report
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		tierCheck1 = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TIER1_CHECKBOX);
		tierCheck2 = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TIER2_CHECKBOX);
		tierCheck3 = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TIER3_CHECKBOX);
		ArrayList serArrayList = new ArrayList();
		ServiceSelectPanel serPanel = (ServiceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_SERVICE_PANEL);
		
		if (tierCheck1.isSelected())
		{
			for (int i=0;i<getTierList(1).length;i++)
				serArrayList.add(getTierList(1)[i]);
		}
		if (tierCheck2.isSelected())
		{
			for (int i=0;i<getTierList(2).length;i++)
				serArrayList.add(getTierList(2)[i]);
		}
		if (tierCheck3.isSelected())
		{
			for (int i=0;i<getTierList(3).length;i++)
				serArrayList.add(getTierList(3)[i]);
		}
		//unselect all
		Enumeration<String> enumKey = serPanel.checkBoxHash.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
			JCheckBox checkBox = (JCheckBox) serPanel.checkBoxHash.get(key);
			checkBox.setSelected(false);

		}
		for (int i = 0; i< serArrayList.size(); i++)
		{
			JCheckBox checkBox = (JCheckBox) serPanel.checkBoxHash.get(serArrayList.get(i));
			checkBox.setSelected(true);
		}
	}

	//TODO: should find a way to automate this list below	

	/**
	 * Determines which services are within Tier 1, Tier 2, and Tier 3
	 * @param tierInt 		Integer referring to which services to highlight from the selected Tier
	 * @return String[] 	Containing the list of services for the Tier selected
	 */
	public String[] getTierList(Integer tierInt)
	{
		String[] serLister = null;
		if(tierInt == 1)
		{
			String[] serList = {"Get-SetPatientHistory",
					"Add_Encounter_Data_to_Encounter_History",
					"CRUD_Consult_Report",
					"Get_Encounter_Data",
					"CRUD_Labor_Order",
					"CRUD_Procedure_Order",
					"Associate_Patient_ID_with_Family",
					"Find_Untreated_Patient",
					"Merge_Patient_Records",
					"Review_Medical_Status",
					"CRUD_Recommendations",
					"CRUD_Appointment",
					"Get_Guidelines_and_Protocols",
					"AlertOnAbnormalValue",
					"CRUD_Transfer_Order",
					"CRUD_Lab_Test_Results",
					"CRUD_Medication_Order",
					"CRUD_Order",
					"CRUD_Patient_Demographic_Data",
					"CRUD_Admission_Information",
					"CRUD_Patient_Information_in_Facility",
					"Get_Healthcare_Plan_Details(out-of-pocket_cost,_limitations,_guidelines)",
					"GetPatient_(Patient_Identity:_Person_Search)",
					"Get_Facility_Resources",
					"Assess_Situation",
					"CRUD_Family_Preferences",
					"CRUD_Patient_Preferences",
					"Run_Analysis",
					"CRUD_Referral"};
			return serList;
		}
		if(tierInt == 2)
		{
			String[] serList = {"ManageServiceAuthorization",
					"CRUD_Single_Patient_(Mark_Erroneous,_Associate_Patient_with_Record,_Create_Patient_with_Unknown_Identity,_Associate_Multiple_Identifiers)",
					"Equipment_Monitoring_Service",
					"Get-Set_Care-Treatment_Plan",
					"Account_Monitoring_Service",
					"Process_Payment",
					"Get_Resource_Schedule",
					"CRUD_Patient_Consent_and_Authorization",
					"Compare_Assessment_Against_Standards_and_Practices",
					"CRUD_Accounts_Receivable_Data",
					"Notify_External_Source",
					"Notify_Providers_and_Managers",
					"Availability_Information",
					"CRUD_Facility_Wait_List",
					"Get_Facility_Capacity_Utilization",
					"Report_Accounts_Receivable",
					"Transfer_Delinquent_Account",
					"CRUD_Facility",
					"Get-Set_Pharmacy_Data",
					"Amend_Health_Information",
					"Compare_with_Assessment_with_Problem_List",
					"Evaluate_Outcomes",
					"Evaluate_Results",
					"CRUD_Budgets_and_Authorization",
					"CRUD_Immunization_List",
					"Cross_check_Medication_with_Medication_Allergies",
					"Alert_on_Wrong_Dosage",
					"Send-Receive_Image",
					"Contract_Monitoring_Service",
					"Materiel_Monitoring_Service",
					"Assemblage_Monitoring_Service",
					"CRUD_Advanced_Directives",
					"CRUD_Patient_Baggage_Information",
					"Find_Facility",
					"Get_Informed_Consent",
					"Get-Set_Resuscitation_Order",
					"ManagePatientAdmission",
					"Send-Transfer_Admission_Information",
					"CRUD_Clinical_Measurements"};
			return serList;
		}
		else if(tierInt == 3)
		{
			String[] serList = {"Get_Facility_and_Team_Schedules",
					"Get-Set_Performance_andAccountability_Measures",
					"Get-Set_Care_Provider",
					"Get-Set_Resource",
					"CRUD_Patient_Allergy_List",
					"CRUD_Documentation_or_Note",
					"Alert_on_Missing_Information",
					"Alert_on_Preventative_Data_and_Wellness_Due_Date",
					"Alert_Patient",
					"Alert_Provider",
					"Alert_Providers_On_Administration_Error",
					"Get_Patient_Data",
					"CRUD_Provider_Information",
					"Get-Set_Eligibility_Information",
					"CRUD_Exposure_Data",
					"Customize_Care_Plan",
					"Get-Set_Care_Plan_Decision_Point",
					"Get_Analysis_Results",
					"Get-Set_Clinical_Guideline",
					"Set_Results_as_Certified",
					"Validate_Educational_Material",
					"Get_Care_Plan",
					"Get_Preventative_Care_and_Wellness_Information",
					"Set_Alert_on_Preventative_Data_and_Wellness_Due_Date",
					"Assess_Facility_Efficiency",
					"Display_Efficiency_Information",
					"Recommend_Order_Set",
					"Generate_Referral",
					"Generate_Report_Discharge_Summary-Health_Report",
					"Get-Set_Referrals",
					"Recommend_Referral",
					"Send_Authorization_or_Referral",
					"Send_Referral_To_Provider",
					"Get-Set_Blood_and_Blood_Product",
					"CRUD_Task",
					"CRUD_Patient_Instructions",
					"CRUD_Financial_and_Administrative_Data",
					"Recommend_Appropriate_Dose",
					"CRUD_Team_Wait_List",
					"CRUD_Team",
					"Find_Team",
					"Get_Provider_Role_in_Team",
					"Get_Team_Members_for_Provider",
					"Check_Policy_Compliance",
					"Set_Preferences_for_Treatment_Plan",
					"CRUD_Industrial_Hygiene_data",
					"CRUD_Patient_Adverse_Reaction_List",
					"CRUD_Employee_Information",
					"Validate_Certification_Token",
					"Get_Medication_Recommendations",
					"Publish_Equipment_Readiness_Reference_Data_Updates",
					"Check_Medication_Availability",
					"Fill_Prescription",
					"CRUD_Screening_Questionnaire",
					"Assign_Permissions_to_Other_Providers",
					"CRUD_Recruiting_Information",
					"Get-SetPatientInsuranceOptions",
					"Get-Set_Educational_Material",
					"CRUD_Patient_Guidance_and_Reminder",
					"Customize_Care_Pathway",
					"CRUD_Managed_Care_Contracting_data",
					"Asset_Management_Service",
					"Inventory_Monitoring_Service",
					"CRUD_Employee_Benefits",
					"No_Service_Available",
					"Get-Set-Check_Authentication",
					"Create_New_Item_Request",
					"Create_Requisition",
					"Generate_Order_Cancellation",
					"Generate_Order_Status_Request",
					"Generate_Order_Update",
					"Generate_Order",
					"Update_Requisition",
					"MergePatientRecord",
					"Get-Set_Intervention"};
			return serList;
		}
		return serLister;
	}

	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
