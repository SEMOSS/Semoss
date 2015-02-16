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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.VertexFilterData;
import prerna.util.Constants;

/**
 * This class performs various calculations that are necessary for the SOA transition.
 */
public class SOATransitionCalculator {
	ArrayList<String>  serList1;
	ArrayList<String>  serList2;
	ArrayList<String>  serList3;
	Double hourlyCost = 150.0;
	Double maintenanceRate = 0.18;
	Double inflationRate = 0.018;
	Double interfaceCostPerYear = 100000.0;
	/**
	 * Constructor for SOATransitionCalculator.
	 */
	public SOATransitionCalculator()
	{
		defineServiceTiers();
	}
	/**
	 * Defines the service tiers in various array lists.
	 */
	public void defineServiceTiers()
	{

		ArrayList<String> serListing1 = new ArrayList<String>(Arrays.asList("Get-SetPatientHistory",
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
				"CRUD_Referral"));
		ArrayList<String> serListing2 = new ArrayList<String>(Arrays.asList("ManageServiceAuthorization",
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
				"CRUD_Clinical_Measurements"));
		ArrayList<String> serListing3 = new ArrayList<String>(Arrays.asList("Get_Facility_and_Team_Schedules",
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
				"Get-Set_Intervention"));
		serList1 = serListing1;
		serList2 = serListing2;
		serList3 = serListing3;
	}
	
	/**
	 * Gets estimates for the SOA transition taking into account maintenance costs and the inflation rate.
	 * @param list 			ArrayList containing information about services, LOE.
	
	 * @return double[] 	List of estimates for SOA service transition. */
	public double[] processEstimates(ArrayList<Object[]> list)
	{
		double[] returnDouble = new double[7];
		int yearIdx = 1;
//		int serIdx = 3;
		for (int i=0; i<list.size();i++)
		{
			Object[] listArray = list.get(i);
			//third element from query is service name
			String serName = (String) listArray[3];
			//fourth element is loe
			Double loe = (Double)listArray[4];
			if (serList1.contains(serName))
			{
				yearIdx = 0;
			}
			else if (serList2.contains(serName))
			{
				yearIdx = 1;
			}
			else if (serList3.contains(serName))
			{
				yearIdx = 2;
			}
			returnDouble[yearIdx] = returnDouble[yearIdx]+ loe *hourlyCost;
		}
		//add maintenance cost 
		
		for (int i= 2; i>=0;i--)
		{
			//number of years of maintenance for the 2nd and 3rd year decrease
			if(returnDouble[i]==0)
			{
				continue;
			}
			for (int j= i+1;j<7;j++)
			{
				
				returnDouble[j]=returnDouble[j]+returnDouble[i]*maintenanceRate*Math.pow(1+inflationRate,  j-i);
			}
		}
		return returnDouble;	
	}
	
	/**
	 * Factors in interface cost and inflation rate to calculate costs of ICD maintenance
	 * @param oldFilterData 	Vertex filter data that has nodes pertaining to ICDs.
	
	 * @return double[]			List of ICD maintenance costs. */
	public double[] processICDMaintenance(VertexFilterData oldFilterData)
	{

		Vector<SEMOSSVertex> icdV = oldFilterData.getNodes("InterfaceControlDocument");
		
		double[] retDouble = new double[7];
		double totalMainCost = icdV.size()*interfaceCostPerYear;		
		
		for (int i = 0; i<retDouble.length;i++)
		{
			retDouble[i]=totalMainCost*Math.pow(inflationRate+1, i);
		}

		return retDouble;
	}
	
	
	/**
	 * Updates cost of ICD maintenance with SOA transition.
	 * @param oldFilterData 	Data with ICD maintenance costs pre-SOA transition.
	 * @param newFilterData 	Updated data of ICD maintenance costs.
	
	 * @return double[] 		List of maintenance costs for ICDs with SOA. */
	public double[] processSOAICDMaintenance(VertexFilterData oldFilterData, VertexFilterData newFilterData)
	{

		Vector<SEMOSSVertex> icdV = oldFilterData.getNodes("InterfaceControlDocument");
		Vector<SEMOSSEdge> icdDataEdgeV = oldFilterData.getEdges("Payload");
		Vector<SEMOSSEdge> dataSerEdgeV = newFilterData.getEdges("Exposes");
		Vector<String> icdNameV = createStringVectorFromVertexVector(icdV);
		Vector<String> icdDataNameV = createStringVectorFromEdgeVector(icdDataEdgeV);
		Vector<String> dataSerNameV = createStringVectorFromEdgeVector(dataSerEdgeV);
		
		double mCostPerYear = icdV.size()*interfaceCostPerYear;
		double[] retDouble = {mCostPerYear, mCostPerYear, mCostPerYear, 0, 0, 0, 0};
				
		//change to vectors with names only
		for (int i = 0; i<icdNameV.size();i++)
		{
			String icdName = icdNameV.get(i);
			String dataName = "";
			//find dataobject for each icd
			for (int j = 0; j<icdDataNameV.size();j++)
			{
				String[] icdDataEdgeSplit = icdDataNameV.get(j).split(":");	
				if (icdDataEdgeSplit[0].equals(icdName))
				{
					dataName = icdDataEdgeSplit[1];
					break;
				}
			}
			
			//find what ser provide this data object
			String serName = "";
			int serIdx = 0;
			for (int j=0; j<dataSerNameV.size();j++)
			{
				String[] dataSerEdgeSplit = dataSerNameV.get(j).split(":");
				if (dataSerEdgeSplit[1].equals(dataName))
				{
					serName = dataSerEdgeSplit[0];
					//data may need multiple services to provide it, so icd maintenance needs to continue until last service is built
					int newIdx = getSerIdx(serName);
					if (newIdx>serIdx)
					{
						serIdx = newIdx;
					}
				}
			}
			if (serIdx!=0)
			{
			//subtract cost starting at year which interface can be decommissioned, serIdx only 1 or 2
				for(int j=serIdx;j<3;j++)
				{
					retDouble[j]= retDouble[j]-100000;
				}
			}
			
		}
		
		return retDouble;
	}
	
	/**
	 * Given a vector of DBCM vertexes, creates a vector containing strings of the names of the nodes.
	 * @param v Vector<DBCMVertex>
	
	 * @return Vector */
	public Vector createStringVectorFromVertexVector(Vector<SEMOSSVertex> v)
	{
		Vector newV = new Vector();
		for (int i = 0;i<v.size();i++)
		{
			String name = (String) v.get(i).getProperty(Constants.VERTEX_NAME);
			newV.addElement(name);
		}
		return newV;
	}
	/**
	 * Given a vector of DBCM edges, creates a vector containing strings of the names of the edges.
	 * @param v Vector<DBCMEdge>
	
	 * @return Vector */
	public Vector createStringVectorFromEdgeVector(Vector<SEMOSSEdge> v)
	{
		Vector newV = new Vector();
		for (int i = 0;i<v.size();i++)
		{
			String name = (String) v.get(i).getProperty(Constants.EDGE_NAME);
			newV.addElement(name);
		}
		return newV;
	}
	
	/**
	 * Gets the tier for a particular system.
	 * @param service 	Name of the service in string form.
	
	 * @return Integer 	Service index of the given system. */
	public Integer getSerIdx(String service)
	{
		int ret = 0;
		if(serList1.contains(service))
		{
			ret = 1;
			return ret;
		}
		if(serList2.contains(service))
		{
			ret = 2;
			return ret;
		}
		if(serList3.contains(service))
		{
			//nothing needs to be done if it's tier 3
			
			ret = 0;
			return ret;
		}
		return ret;
	}
}

