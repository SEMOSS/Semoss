package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;

public class AOACostSavingsPlaySheet extends GridPlaySheet {

	private ITableDataFrame origDataFrame;
	private ArrayList<Object> data = new ArrayList<Object>();
	private Map<String, Map<String, Map<String, Double>>> costSavingsTable = new Hashtable<String, Map<String, Map<String,Double>>>(); //mapping vendor to package to package's cost category info
	private Map<String, Map<String, Double>> vendorCategoryCostSum = new HashMap<String,Map<String,Double>>(); //mapping vendor to category to cost per category
	private Map<String,Map<String,Double>> vendorSiteFCC = new HashMap<String,Map<String,Double>>(); //mapping vendor to site to FCC
	private Map<String,Map<String,Map<String,Double>>>vendorSavings = new HashMap<String,Map<String,Map<String,Double>>>(); //mapping vendor to year to category savings
	private Map<String,String> checkedPackages = new HashMap<String,String>(); 
	private String setVendor = "QFlow";
	
	public static final String LICENSE_KEY = "License";
	public static final String HARDWARE_KEY = "Hardware";
	public static final String DEPLOYMENT_KEY = "Deployment";
	public static final String MAINTENANCE_KEY = "Maintenance";
	public static final String MODIFICATION_KEY = "Modification";
	public static final String TOTAL_KEY = "Total";
	public static final String LICENSE_KEY_100HOSPITALS = "License_100Hospitals";
	public static final String HARDWARE_KEY_100HOSPITALS = "Hardware_100Hospitals";
	public static final String DEPLOYMENT_KEY_100HOSPITALS = "Deployment_100Hospitals";
	public static final String MAINTENANCE_KEY_100HOSPITALS = "Maintenance_100Hospitals";
	public static final String MODIFICATION_KEY_100HOSPITALS = "Modification_100Hospitals";
	public static final String TOTAL_KEY_100HOSPITALS = "Total_100Hospitals";
	public static final String LICENSE_KEY_100CLINICS = "License_100Clinics";
	public static final String HARDWARE_KEY_100CLINICS = "Hardware_100Clinics";
	public static final String DEPLOYMENT_KEY_100CLINICS = "Deployment_100Clinics";
	public static final String MAINTENANCE_KEY_100CLINICS = "Maintenance_100Clinics";
	public static final String MODIFICATION_KEY_100CLINICS = "Modification_100Clinics";
	public static final String TOTAL_KEY_100CLINICS = "Total_100Clinics";

	//QFlow Inputs
	int Hospitals_QF= 54;
	int Clinics_QF= 373;
	int Regions_QF= 5;
	int DpH_QF= 589;
	int UpD_QF= 4712;
	double KpD_QF= 883.5;

	int Departments_QF=(DpH_QF*Hospitals_QF) + Clinics_QF;
	int Users_QF= UpD_QF*Departments_QF;
	int Kiosks_QF= (int)KpD_QF*Departments_QF;

	//QMatic Inputs
	int Hospitals_QM= 54;
	int Clinics_QM= 373;
	int Regions_QM= 5;
	int DpH_QM= 589;
	int UpD_QM= 4712;
	double KpD_QM= 883.5;

	int Departments_QM=(DpH_QM*Hospitals_QM) + Clinics_QM;
	int Users_QM= UpD_QM*Departments_QM;
	int Kiosks_QM= (int)KpD_QM*Departments_QM;

	@Override
	public void createData (){
		// Reading the vendor-site-FCC query and assigning variable values
		String vendorSiteFCCQuery = "SELECT DISTINCT ?Vendor ?Site ?FCC_Cost " + 
				"WHERE{" + 
				"{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}" + 
				"{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>;}" + 
				"{?Vendor <http://semoss.org/ontologies/Relation/DeployedAt> ?Site;}" + 
				"{?Site <http://semoss.org/ontologies/Relation/Contains/FCC_Cost> ?FCC_Cost;}" + 
				"}"; 

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, vendorSiteFCCQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String vendorTo = (String) ss.getVar(names[0]);
			String site = (String) ss.getVar(names[1]);
			double FCC = (double)(ss.getVar(names[2]));

			//Add vendor-site-FCC to vendorSiteFCC HashMap
			Map<String,Double> siteToFCC = new HashMap <String,Double>();
			siteToFCC.put(site,FCC);
			if(vendorSiteFCC.containsKey(vendorTo)){
				siteToFCC= vendorSiteFCC.get(vendorTo);
				if(siteToFCC.containsKey(site)){
					throw new IllegalArgumentException("Site already exists...why?");
				} else {
					siteToFCC.put(site,FCC);
				}
			}	else{
				siteToFCC.put(site,FCC);
				vendorSiteFCC.put(vendorTo,siteToFCC);
			}
		}

		//Reading the cost query and assigning variable values
		String COST_SAVINGS_QUERY = "SELECT DISTINCT ?Vendor ?Package ?License ?Hardware ?Deployment ?Maintenance ?Modification ?Total ?License_100Hospitals ?Hardware_100Hospitals ?Deployment_100Hospitals ?Maintenance_100Hospitals ?Modification_100Hospitals ?Total_100Hospitals ?License_100Clinics ?Hardware_100Clinics ?Deployment_100Clinics ?Maintenance_100Clinics ?Modification_100Clinics ?Total_100Clinics " 
				+"WHERE {{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}"
				+"{?Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package>;}" 
				+"{?Vendor <http://semoss.org/ontologies/Relation/Supports> ?Package;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/License> ?License;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Hardware> ?Hardware;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Deployment> ?Deployment;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Modification> ?Modification;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Maintenance> ?Maintenance;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Total> ?Total;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/License_100Hospitals> ?License_100Hospitals;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Hardware_100Hospitals> ?Hardware_100Hospitals;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Deployment_100Hospitals> ?Deployment_100Hospitals;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Modification_100Hospitals> ?Modification_100Hospitals;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Maintenance_100Hospitals> ?Maintenance_100Hospitals;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Total_100Hospitals> ?Total_100Hospitals;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/License_100Clinics> ?License_100Clinics;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Hardware_100Clinics> ?Hardware_100Clinics;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Deployment_100Clinics> ?Deployment_100Clinics;}" 
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Modification_100Clinics> ?Modification_100Clinics;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Maintenance_100Clinics> ?Maintenance_100Clinics;}"
				+"{?Package <http://semoss.org/ontologies/Relation/Contains/Total_100Clinics> ?Total_100Clinics;}"
				+"}";
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper(engine, COST_SAVINGS_QUERY);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext()) {
			ISelectStatement ss = wrapper2.next();;
			String vendor = (String) ss.getVar(names2[0]);
			String packageName = (String) ss.getVar(names2[1]);
			double license, hardware, deployment, maintenance, modification, total, license100Hospitals, hardware100Hospitals,
			deployment100Hospitals, maintenance100Hospitals, modification100Hospitals, total100Hospitals, deployment100Clinics, 
			hardware100Clinics, license100Clinics, maintenance100Clinics, modification100Clinics, total100Clinics;
			if (ss.getVar(names2[2]) instanceof Double) {
				license = (double) ss.getVar(names2[2]);
				hardware = (double) ss.getVar(names2[3]);
				deployment = (double) ss.getVar(names2[4]);
				maintenance = (double) ss.getVar(names2[5]);
				modification = (double) ss.getVar(names2[6]);
				total = (double) ss.getVar(names2[7]);
				license100Hospitals = (double) ss.getVar(names2[8]);
				hardware100Hospitals =  (double) ss.getVar(names2[9]);
				deployment100Hospitals = (double) ss.getVar(names2[10]);
				maintenance100Hospitals = (double) ss.getVar(names2[11]);
				modification100Hospitals = (double) ss.getVar(names2[12]);
				total100Hospitals = (double) ss.getVar(names2[13]);
				license100Clinics = (double) ss.getVar(names2[14]);
				hardware100Clinics = (double) ss.getVar(names2[15]);
				deployment100Clinics = (double) ss.getVar(names2[16]);
				maintenance100Clinics = (double) ss.getVar(names2[17]);
				modification100Clinics = (double) ss.getVar(names2[18]);
				total100Clinics = (double) ss.getVar(names2[19]);
			}
			else {
				license = (Double.parseDouble((String) ss.getVar(names2[2])));
				hardware = (Double.parseDouble((String) ss.getVar(names2[3])));
				deployment = (Double.parseDouble((String) ss.getVar(names2[4])));
				maintenance = (Double.parseDouble((String) ss.getVar(names2[5])));
				modification = (Double.parseDouble((String) ss.getVar(names2[6])));
				total = (Double.parseDouble((String) ss.getVar(names2[7])));
				license100Hospitals = (Double.parseDouble((String) ss.getVar(names2[8])));
				hardware100Hospitals = (Double.parseDouble((String) ss.getVar(names2[9])));
				deployment100Hospitals = (Double.parseDouble((String) ss.getVar(names2[10])));
				maintenance100Hospitals =(Double.parseDouble((String) ss.getVar(names2[11])));
				modification100Hospitals = (Double.parseDouble((String) ss.getVar(names2[12])));
				total100Hospitals = (Double.parseDouble((String) ss.getVar(names2[13])));
				license100Clinics = (Double.parseDouble((String) ss.getVar(names2[14])));
				hardware100Clinics = (Double.parseDouble((String) ss.getVar(names2[15])));
				deployment100Clinics = (Double.parseDouble((String) ss.getVar(names2[16])));
				maintenance100Clinics =(Double.parseDouble((String) ss.getVar(names2[17])));
				modification100Clinics = (Double.parseDouble((String) ss.getVar(names2[18])));
				total100Clinics = (Double.parseDouble((String) ss.getVar(names2[19])));
			}
			
			//Check to see if package is checked
			if(!checkedPackages.containsKey(packageName) || checkedPackages.isEmpty()){
				checkedPackages.put(packageName, "true"); 
			}

			//Add checked package info to costSavingsTable
			Map<String, Map<String, Double>> packageToCategoryInfo = new HashMap<String,Map<String,Double>>(); //linking each package to its category costs
			Map<String,Double> categoryInfo = new HashMap<String,Double>(); //for each package, all of the category costs 
			
			categoryInfo.put(LICENSE_KEY, license);
			categoryInfo.put(HARDWARE_KEY, hardware);
			categoryInfo.put(DEPLOYMENT_KEY, deployment);
			categoryInfo.put(MAINTENANCE_KEY, maintenance);
			categoryInfo.put(MODIFICATION_KEY, modification);
			categoryInfo.put(TOTAL_KEY, total);
			categoryInfo.put(LICENSE_KEY_100HOSPITALS, license100Hospitals);
			categoryInfo.put(HARDWARE_KEY_100HOSPITALS, hardware100Hospitals);
			categoryInfo.put(DEPLOYMENT_KEY_100HOSPITALS, deployment100Hospitals);
			categoryInfo.put(MAINTENANCE_KEY_100HOSPITALS, maintenance100Hospitals);
			categoryInfo.put(MODIFICATION_KEY_100HOSPITALS, modification100Hospitals);
			categoryInfo.put(TOTAL_KEY_100HOSPITALS, total100Hospitals);
			categoryInfo.put(LICENSE_KEY_100CLINICS, license100Clinics);
			categoryInfo.put(HARDWARE_KEY_100CLINICS, hardware100Clinics);
			categoryInfo.put(DEPLOYMENT_KEY_100CLINICS, deployment100Clinics);
			categoryInfo.put(MAINTENANCE_KEY_100CLINICS, maintenance100Clinics);
			categoryInfo.put(MODIFICATION_KEY_100CLINICS, modification100Clinics);
			categoryInfo.put(TOTAL_KEY_100CLINICS, total100Clinics);

			if(costSavingsTable.containsKey(vendor)) {
				packageToCategoryInfo = costSavingsTable.get(vendor);
				if(packageToCategoryInfo.containsKey(packageName)) {
					throw new IllegalArgumentException("Package already exists...why?");
				} else {
					packageToCategoryInfo.put(packageName, categoryInfo);
				}
			} else {
				packageToCategoryInfo.put(packageName, categoryInfo);
				costSavingsTable.put(vendor, packageToCategoryInfo);
			}
		}
	}
	public void runAnalytics () {
		for (String vendor: costSavingsTable.keySet()){
			Map<String,Map<String,Double>> selectedVendorCostSavings = costSavingsTable.get(vendor);
			//declare category sum variables
			double licenseCostSum = 0;
			double hardwareCostSum = 0;
			double deploymentCostSum = 0;
			double maintenanceCostSum = 0;
			double modificationCostSum = 0;
			double totalCostSum = 0;
			double licenseCost100HospitalsSum = 0;
			double hardwareCost100HospitalsSum = 0;
			double deploymentCost100HospitalsSum = 0;
			double maintenanceCost100HospitalsSum = 0;
			double modificationCost100HospitalsSum = 0;
			double totalCost100HospitalsSum = 0;
			double licenseCost100ClinicsSum = 0;
			double hardwareCost100ClinicsSum = 0;
			double deploymentCost100ClinicsSum = 0;
			double maintenanceCost100ClinicsSum = 0;
			double modificationCost100ClinicsSum = 0;
			double totalCost100ClinicsSum = 0;

			//sum all of the category costs for selected packages 
			for (String packages : selectedVendorCostSavings.keySet()){
				System.out.println(packages);
				if (checkedPackages.get(packages).equals("true")){
					Map<String,Double> selectedPackagesCostSavings = selectedVendorCostSavings.get(packages);
					licenseCostSum += selectedPackagesCostSavings.get(LICENSE_KEY);
					hardwareCostSum += selectedPackagesCostSavings.get(HARDWARE_KEY);
					deploymentCostSum += selectedPackagesCostSavings.get(DEPLOYMENT_KEY);
					maintenanceCostSum += selectedPackagesCostSavings.get(MAINTENANCE_KEY);
					modificationCostSum += selectedPackagesCostSavings.get(MODIFICATION_KEY);
					totalCostSum += selectedPackagesCostSavings.get(TOTAL_KEY);
					licenseCost100HospitalsSum+= selectedPackagesCostSavings.get(LICENSE_KEY_100HOSPITALS);
					hardwareCost100HospitalsSum+= selectedPackagesCostSavings.get(HARDWARE_KEY_100HOSPITALS);
					deploymentCost100HospitalsSum+= selectedPackagesCostSavings.get(DEPLOYMENT_KEY_100HOSPITALS);
					maintenanceCost100HospitalsSum+= selectedPackagesCostSavings.get(MAINTENANCE_KEY_100HOSPITALS);
					modificationCost100HospitalsSum+= selectedPackagesCostSavings.get(MODIFICATION_KEY_100HOSPITALS);
					totalCost100HospitalsSum += selectedPackagesCostSavings.get(TOTAL_KEY_100HOSPITALS);
					licenseCost100ClinicsSum+= selectedPackagesCostSavings.get(LICENSE_KEY_100CLINICS);
					hardwareCost100ClinicsSum+= selectedPackagesCostSavings.get(HARDWARE_KEY_100CLINICS);
					deploymentCost100ClinicsSum+= selectedPackagesCostSavings.get(DEPLOYMENT_KEY_100CLINICS);
					maintenanceCost100ClinicsSum+= selectedPackagesCostSavings.get(MAINTENANCE_KEY_100CLINICS);
					modificationCost100ClinicsSum+= selectedPackagesCostSavings.get(MODIFICATION_KEY_100CLINICS);
					totalCost100ClinicsSum += selectedPackagesCostSavings.get(TOTAL_KEY_100CLINICS);
				}
			}
			//map each vendor to its category costs (category costs are calculated based on selected packages)
			Map<String,Double> categoryCostSum = new HashMap<String,Double>();
			categoryCostSum.put(LICENSE_KEY, licenseCostSum);
			categoryCostSum.put(HARDWARE_KEY, hardwareCostSum);
			categoryCostSum.put(DEPLOYMENT_KEY, deploymentCostSum);
			categoryCostSum.put(MAINTENANCE_KEY, maintenanceCostSum);
			categoryCostSum.put(MODIFICATION_KEY, modificationCostSum);
			categoryCostSum.put(TOTAL_KEY, totalCostSum);
			categoryCostSum.put(LICENSE_KEY_100HOSPITALS, licenseCost100HospitalsSum);
			categoryCostSum.put(HARDWARE_KEY_100HOSPITALS, hardwareCost100HospitalsSum);
			categoryCostSum.put(DEPLOYMENT_KEY_100HOSPITALS, deploymentCost100HospitalsSum);
			categoryCostSum.put(MAINTENANCE_KEY_100HOSPITALS, maintenanceCost100HospitalsSum);
			categoryCostSum.put(MODIFICATION_KEY_100HOSPITALS, modificationCost100HospitalsSum);
			categoryCostSum.put(TOTAL_KEY_100HOSPITALS, totalCost100HospitalsSum);
			categoryCostSum.put(LICENSE_KEY_100CLINICS, licenseCost100ClinicsSum);
			categoryCostSum.put(HARDWARE_KEY_100CLINICS, hardwareCost100ClinicsSum);
			categoryCostSum.put(DEPLOYMENT_KEY_100CLINICS, deploymentCost100ClinicsSum);
			categoryCostSum.put(MAINTENANCE_KEY_100CLINICS, maintenanceCost100ClinicsSum);
			categoryCostSum.put(MODIFICATION_KEY_100CLINICS, modificationCost100ClinicsSum);
			categoryCostSum.put(TOTAL_KEY_100CLINICS, totalCost100ClinicsSum);
			vendorCategoryCostSum.put(vendor,categoryCostSum); //category cost summations are correct! 
		}

		//calculate category % for original set up for Q-Matic
		double percentLicenseQM= 0;
		double percentHardwareQM= 0;
		double percentDeploymentQM= 0;
		double percentMaintenanceQM =0;
		double percentModificationQM=0;
		percentLicenseQM = (vendorCategoryCostSum.get("Q-Matic").get(LICENSE_KEY))/(vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY)); 
		percentHardwareQM= (vendorCategoryCostSum.get("Q-Matic").get(HARDWARE_KEY))/(vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY));
		percentDeploymentQM= (vendorCategoryCostSum.get("Q-Matic").get(DEPLOYMENT_KEY))/(vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY));
		percentMaintenanceQM= (vendorCategoryCostSum.get("Q-Matic").get(MAINTENANCE_KEY))/(vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY));
		percentModificationQM= (vendorCategoryCostSum.get("Q-Matic").get(MODIFICATION_KEY))/(vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY));

		//calculate category % for original set up for QFlow
		double percentLicenseQF= 0;
		double percentHardwareQF= 0;
		double percentDeploymentQF= 0;
		double percentMaintenanceQF =0;
		double percentModificationQF=0;
		percentLicenseQF = (vendorCategoryCostSum.get("QFlow").get(LICENSE_KEY))/(vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY));
		percentHardwareQF= (vendorCategoryCostSum.get("QFlow").get(HARDWARE_KEY))/(vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY));
		percentDeploymentQF= (vendorCategoryCostSum.get("QFlow").get(DEPLOYMENT_KEY))/(vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY));
		percentMaintenanceQF= (vendorCategoryCostSum.get("QFlow").get(MAINTENANCE_KEY))/(vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY));
		percentModificationQF= (vendorCategoryCostSum.get("QFlow").get(MODIFICATION_KEY))/(vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY));

		//factor calculations for Q-Matic
		double avgHospital_QM =0;
		double avgClinic_QM = 0;
		double factorQM = 0;
		double totalFCC_QM = 0;  
		Map<String,Double> siteToFCC_QMatic = vendorSiteFCC.get("Q-Matic");
		for (String site: siteToFCC_QMatic.keySet()){
			totalFCC_QM += siteToFCC_QMatic.get(site);
		}
		double totalFCC_QF =0; //Make sure it is total FCC for only QFlow sites 
		Map<String,Double> siteToFCC_QFlow = vendorSiteFCC.get("QFlow");
		
		for (String site: siteToFCC_QFlow.keySet()){
			totalFCC_QF += siteToFCC_QFlow.get(site);
		}
		double totalFCC = totalFCC_QM+totalFCC_QF; //total FCC Costs for all QMatic and QFlow Sites 
		
		avgHospital_QM = (vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY_100HOSPITALS))/100;
		avgClinic_QM = (vendorCategoryCostSum.get("Q-Matic").get(TOTAL_KEY_100CLINICS))/100;
		factorQM = ((Hospitals_QM*avgHospital_QM)+(Clinics_QM*avgClinic_QM))/totalFCC;

		//factor category calculations for Q-Matic
		double QM_License_Factor =0;
		double QM_Hardware_Factor = 0;
		double QM_Deployment_Factor =0;
		double QM_Maintenance_Factor =0;
		double QM_Modification_Factor=0;

		QM_License_Factor= percentLicenseQM*factorQM;
		QM_Hardware_Factor= percentHardwareQM*factorQM;
		QM_Deployment_Factor= percentDeploymentQM*factorQM;
		QM_Maintenance_Factor= percentMaintenanceQM*factorQM;
		QM_Modification_Factor = percentModificationQM*factorQM;

		//factor calculations for QFlow
		double avgHospital_QF =0;
		double avgClinic_QF = 0;
		double factorQF = 0;

		avgHospital_QF = (vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY_100HOSPITALS))/100;
		avgClinic_QF = (vendorCategoryCostSum.get("QFlow").get(TOTAL_KEY_100CLINICS))/100;
		factorQF = ((Hospitals_QF*avgHospital_QF)+(Clinics_QF*avgClinic_QF))/totalFCC;

		//factor category calculations for Q-Flow
		double QF_License_Factor =0;
		double QF_Hardware_Factor = 0;
		double QF_Deployment_Factor =0;
		double QF_Maintenance_Factor =0;
		double QF_Modification_Factor=0;

		QF_License_Factor= percentLicenseQF*factorQF;
		QF_Hardware_Factor= percentHardwareQF*factorQF;
		QF_Deployment_Factor= percentDeploymentQF*factorQF;
		QF_Maintenance_Factor= percentMaintenanceQF*factorQF;
		QF_Modification_Factor = percentModificationQF*factorQF;

		//calculating category costs for each site (that has Q-Matic)
		//setting QM category summation variables
		double Site_Hardware_QM_Sum =0;
		double Site_Licene_QM_Sum =0;
		double Site_Deployment_QM_Sum=0;
		double Site_Maintenance_QM_Sum=0;
		double Site_Modification_QM_Sum=0;

		//setting QF category summation variables 
		double Site_Hardware_QMtoQF =0;
		double Site_License_QMtoQF=0;
		double Site_Deployment_QMtoQF= 0;
		double Site_Maintenance_QMtoQF =0;
		double Site_Modification_QMtoQF =0;

		for(String site : siteToFCC_QMatic.keySet()){
			//will calculate category costs for each site
			double Site_Hardware_QM = QM_Hardware_Factor*siteToFCC_QMatic.get(site);
			double Site_License_QM =QM_License_Factor*siteToFCC_QMatic.get(site);
			double Site_Deployment_QM =QM_Deployment_Factor*siteToFCC_QMatic.get(site);
			double Site_Maintenance_QM =QM_Maintenance_Factor*siteToFCC_QMatic.get(site);
			double Site_Modification_QM =QM_Modification_Factor*siteToFCC_QMatic.get(site);

			//calculating category summation costs
			Site_Hardware_QM_Sum += Site_Hardware_QM;
			Site_Licene_QM_Sum += Site_License_QM;
			Site_Deployment_QM_Sum += Site_Deployment_QM;
			Site_Maintenance_QM_Sum += Site_Maintenance_QM;
			Site_Modification_QM_Sum += Site_Modification_QM;


			//calculating transition cost to go from QMatic to QFlow
			Site_Hardware_QMtoQF += QF_Hardware_Factor*siteToFCC_QMatic.get(site);
			Site_License_QMtoQF += QF_License_Factor*siteToFCC_QMatic.get(site);
			Site_Deployment_QMtoQF += QF_Deployment_Factor*siteToFCC_QMatic.get(site);
			Site_Maintenance_QMtoQF += QF_Maintenance_Factor*siteToFCC_QMatic.get(site);
			Site_Modification_QMtoQF += QF_Modification_Factor*siteToFCC_QMatic.get(site);

		}

		//calculating category costs for each site (that has QFlow)

		//setting QF category summation variables
		double Site_Hardware_QF_Sum =0;
		double Site_Licene_QF_Sum =0;
		double Site_Deployment_QF_Sum=0;
		double Site_Maintenance_QF_Sum=0;
		double Site_Modification_QF_Sum=0;

		//setting QM category summation variables 		
		double Site_Hardware_QFtoQM =0;
		double Site_License_QFtoQM=0;
		double Site_Deployment_QFtoQM= 0;
		double Site_Maintenance_QFtoQM =0;
		double Site_Modification_QFtoQM =0;

		for(String site: siteToFCC_QFlow.keySet()){
			//will calculate costs for each site
			double Site_Hardware_QF = QF_Hardware_Factor*siteToFCC_QFlow.get(site);
			double Site_License_QF =QF_License_Factor*siteToFCC_QFlow.get(site);
			double Site_Deployment_QF =QF_Deployment_Factor*siteToFCC_QFlow.get(site);
			double Site_Maintenance_QF =QF_Maintenance_Factor*siteToFCC_QFlow.get(site);
			double Site_Modification_QF =QF_Modification_Factor*siteToFCC_QFlow.get(site);

			//calculating category summation costs
			Site_Hardware_QF_Sum += Site_Hardware_QF;
			Site_Licene_QF_Sum += Site_License_QF;
			Site_Deployment_QF_Sum += Site_Deployment_QF;
			Site_Maintenance_QF_Sum += Site_Maintenance_QF;
			Site_Modification_QF_Sum += Site_Modification_QF;

			//calculating transition cost to go from QFlow to QMatic
			Site_Hardware_QFtoQM += QM_Hardware_Factor*siteToFCC_QFlow.get(site);
			Site_License_QFtoQM += QM_License_Factor*siteToFCC_QFlow.get(site);
			Site_Deployment_QFtoQM += QM_Deployment_Factor*siteToFCC_QFlow.get(site);
			Site_Maintenance_QFtoQM += QM_Maintenance_Factor*siteToFCC_QFlow.get(site);
			Site_Modification_QFtoQM += QM_Modification_Factor*siteToFCC_QFlow.get(site);
		}

		//savings for transition to QFlow
		//year 1 savings
		double Y1_QF_Savings_Hardware =-.9*Site_Hardware_QMtoQF;
		double Y1_QF_Savings_License= (Site_Licene_QM_Sum-.8*Site_License_QMtoQF)/5;
		double Y1_QF_Savings_Deployment = -Site_Deployment_QMtoQF;
		double Y1_QF_Savings_Maintenance = (Site_Maintenance_QM_Sum - .8*Site_Maintenance_QMtoQF)/5;
		double Y1_QF_Savings_Modification = -Site_Modification_QMtoQF;
		double Y1_QF_Savings_Total = Y1_QF_Savings_Hardware +  Y1_QF_Savings_License + Y1_QF_Savings_Deployment + Y1_QF_Savings_Maintenance + Y1_QF_Savings_Modification;

		//Mapping Year1 of Savings to Category Savings
		HashMap<String,Double> QFlowCategorySavingsY1 = new HashMap<String,Double>();
		QFlowCategorySavingsY1.put(HARDWARE_KEY, Y1_QF_Savings_Hardware);
		QFlowCategorySavingsY1.put(LICENSE_KEY, Y1_QF_Savings_License);
		QFlowCategorySavingsY1.put(DEPLOYMENT_KEY, Y1_QF_Savings_Deployment);
		QFlowCategorySavingsY1.put(MAINTENANCE_KEY, Y1_QF_Savings_Maintenance);
		QFlowCategorySavingsY1.put(MODIFICATION_KEY, Y1_QF_Savings_Modification);
		QFlowCategorySavingsY1.put(TOTAL_KEY, Y1_QF_Savings_Total);

		//year 2 savings
		double Y2_QF_Savings_Hardware=0;
		double Y2_QF_Savings_License= (Site_Licene_QM_Sum-.8*Site_License_QMtoQF)/5;
		double Y2_QF_Savings_Deployment=0;
		double Y2_QF_Savings_Maintenance=(Site_Maintenance_QM_Sum - .8*Site_Maintenance_QMtoQF)/5;
		double Y2_QF_Savings_Modification=0;
		double Y2_QF_Savings_Total=Y2_QF_Savings_Hardware +  Y2_QF_Savings_License + Y2_QF_Savings_Deployment + Y2_QF_Savings_Maintenance + Y2_QF_Savings_Modification;

		//Mapping Year2 of Savings to Category Savings
		HashMap<String,Double> QFlowCategorySavingsY2 = new HashMap<String,Double>();
		QFlowCategorySavingsY2.put(HARDWARE_KEY, Y2_QF_Savings_Hardware);
		QFlowCategorySavingsY2.put(LICENSE_KEY, Y2_QF_Savings_License);
		QFlowCategorySavingsY2.put(DEPLOYMENT_KEY, Y2_QF_Savings_Deployment);
		QFlowCategorySavingsY2.put(MAINTENANCE_KEY, Y2_QF_Savings_Maintenance);
		QFlowCategorySavingsY2.put(MODIFICATION_KEY, Y2_QF_Savings_Modification);
		QFlowCategorySavingsY2.put(TOTAL_KEY, Y2_QF_Savings_Total);

		//year 3 savings
		double Y3_QF_Savings_Hardware=0;
		double Y3_QF_Savings_License= (Site_Licene_QM_Sum-.8*Site_License_QMtoQF)/5;
		double Y3_QF_Savings_Deployment= 0;
		double Y3_QF_Savings_Maintenance= (Site_Maintenance_QM_Sum - .8*Site_Maintenance_QMtoQF)/5;
		double Y3_QF_Savings_Modification= 0;
		double Y3_QF_Savings_Total= Y3_QF_Savings_Hardware +  Y3_QF_Savings_License + Y3_QF_Savings_Deployment + Y3_QF_Savings_Maintenance + Y3_QF_Savings_Modification;

		//Mapping Year3 of Savings to Category Savings
		HashMap<String,Double> QFlowCategorySavingsY3 = new HashMap<String,Double>();
		QFlowCategorySavingsY3.put(HARDWARE_KEY, Y3_QF_Savings_Hardware);
		QFlowCategorySavingsY3.put(LICENSE_KEY, Y3_QF_Savings_License);
		QFlowCategorySavingsY3.put(DEPLOYMENT_KEY, Y3_QF_Savings_Deployment);
		QFlowCategorySavingsY3.put(MAINTENANCE_KEY, Y3_QF_Savings_Maintenance);
		QFlowCategorySavingsY3.put(MODIFICATION_KEY, Y3_QF_Savings_Modification);
		QFlowCategorySavingsY3.put(TOTAL_KEY, Y3_QF_Savings_Total);

		//year 4 savings
		double Y4_QF_Savings_Hardware=0;
		double Y4_QF_Savings_License= (Site_Licene_QM_Sum-.8*Site_License_QMtoQF)/5;
		double Y4_QF_Savings_Deployment= 0;
		double Y4_QF_Savings_Maintenance= (Site_Maintenance_QM_Sum - .8*Site_Maintenance_QMtoQF)/5;
		double Y4_QF_Savings_Modification= 0;
		double Y4_QF_Savings_Total=Y4_QF_Savings_Hardware +  Y4_QF_Savings_License + Y4_QF_Savings_Deployment + Y4_QF_Savings_Maintenance + Y4_QF_Savings_Modification;

		//Mapping Year4 of Savings to Category Savings
		HashMap<String,Double> QFlowCategorySavingsY4 = new HashMap<String,Double>();
		QFlowCategorySavingsY4.put(HARDWARE_KEY, Y4_QF_Savings_Hardware);
		QFlowCategorySavingsY4.put(LICENSE_KEY, Y4_QF_Savings_License);
		QFlowCategorySavingsY4.put(DEPLOYMENT_KEY, Y4_QF_Savings_Deployment);
		QFlowCategorySavingsY4.put(MAINTENANCE_KEY, Y4_QF_Savings_Maintenance);
		QFlowCategorySavingsY4.put(MODIFICATION_KEY, Y4_QF_Savings_Modification);
		QFlowCategorySavingsY4.put(TOTAL_KEY, Y4_QF_Savings_Total);

		//year 5 savings
		double Y5_QF_Savings_Hardware=0;
		double Y5_QF_Savings_License=(Site_Licene_QM_Sum-.8*Site_License_QMtoQF)/5;
		double Y5_QF_Savings_Deployment= 0;
		double Y5_QF_Savings_Maintenance=(Site_Maintenance_QM_Sum - .8*Site_Maintenance_QMtoQF)/5;
		double Y5_QF_Savings_Modification= 0;
		double Y5_QF_Savings_Total=Y5_QF_Savings_Hardware +  Y5_QF_Savings_License + Y5_QF_Savings_Deployment + Y5_QF_Savings_Maintenance + Y5_QF_Savings_Modification;

		//Mapping Year5 of Savings to Category Savings
		HashMap<String,Double> QFlowCategorySavingsY5 = new HashMap<String,Double>();
		QFlowCategorySavingsY5.put(HARDWARE_KEY, Y5_QF_Savings_Hardware);
		QFlowCategorySavingsY5.put(LICENSE_KEY, Y5_QF_Savings_License);
		QFlowCategorySavingsY5.put(DEPLOYMENT_KEY, Y5_QF_Savings_Deployment);
		QFlowCategorySavingsY5.put(MAINTENANCE_KEY, Y5_QF_Savings_Maintenance);
		QFlowCategorySavingsY5.put(MODIFICATION_KEY, Y5_QF_Savings_Modification);
		QFlowCategorySavingsY5.put(TOTAL_KEY, Y5_QF_Savings_Total);

		// adding savings to HashMap
		HashMap<String,Map<String,Double>> QFlowYearSavings = new HashMap<String,Map<String,Double>>(); //mapping QFlow to Year of Savings
		QFlowYearSavings.put("Y1", QFlowCategorySavingsY1);
		QFlowYearSavings.put("Y2", QFlowCategorySavingsY2);
		QFlowYearSavings.put("Y3", QFlowCategorySavingsY3);
		QFlowYearSavings.put("Y4", QFlowCategorySavingsY4);
		QFlowYearSavings.put("Y5", QFlowCategorySavingsY5);
		vendorSavings.put("QFlow",QFlowYearSavings);

		//savings for transition to QMatic 
		//year 1 savings
		double Y1_QM_Savings_Hardware =-.9*Site_Hardware_QFtoQM;
		double Y1_QM_Savings_License= (Site_Licene_QF_Sum-.8*Site_License_QFtoQM)/5;
		double Y1_QM_Savings_Deployment = -Site_Deployment_QFtoQM;
		double Y1_QM_Savings_Maintenance = (Site_Maintenance_QF_Sum - .8*Site_Maintenance_QFtoQM)/5;
		double Y1_QM_Savings_Modification = -Site_Modification_QFtoQM;
		double Y1_QM_Savings_Total = Y1_QM_Savings_Hardware +  Y1_QM_Savings_License + Y1_QM_Savings_Deployment + Y1_QM_Savings_Maintenance + Y1_QM_Savings_Modification;

		//Mapping Year1 of Savings to Category Savings
		HashMap<String,Double> QMaticCategorySavingsY1 = new HashMap<String,Double>();
		QMaticCategorySavingsY1.put(HARDWARE_KEY, Y1_QM_Savings_Hardware);
		QMaticCategorySavingsY1.put(LICENSE_KEY, Y1_QM_Savings_License);
		QMaticCategorySavingsY1.put(DEPLOYMENT_KEY, Y1_QM_Savings_Deployment);
		QMaticCategorySavingsY1.put(MAINTENANCE_KEY, Y1_QM_Savings_Maintenance);
		QMaticCategorySavingsY1.put(MODIFICATION_KEY, Y1_QM_Savings_Modification);
		QMaticCategorySavingsY1.put(TOTAL_KEY, Y1_QM_Savings_Total);

		//year 2 savings
		double Y2_QM_Savings_Hardware=0;
		double Y2_QM_Savings_License= (Site_Licene_QF_Sum-.8*Site_License_QFtoQM)/5;
		double Y2_QM_Savings_Deployment=0;
		double Y2_QM_Savings_Maintenance=(Site_Maintenance_QF_Sum - .8*Site_Maintenance_QFtoQM)/5;
		double Y2_QM_Savings_Modification=0;
		double Y2_QM_Savings_Total=Y2_QM_Savings_Hardware +  Y2_QM_Savings_License + Y2_QM_Savings_Deployment + Y2_QM_Savings_Maintenance + Y2_QM_Savings_Modification;

		//Mapping Year2 of Savings to Category Savings
		HashMap<String,Double> QMaticCategorySavingsY2 = new HashMap<String,Double>();
		QMaticCategorySavingsY2.put(HARDWARE_KEY, Y2_QM_Savings_Hardware);
		QMaticCategorySavingsY2.put(LICENSE_KEY, Y2_QM_Savings_License);
		QMaticCategorySavingsY2.put(DEPLOYMENT_KEY, Y2_QM_Savings_Deployment);
		QMaticCategorySavingsY2.put(MAINTENANCE_KEY, Y2_QM_Savings_Maintenance);
		QMaticCategorySavingsY2.put(MODIFICATION_KEY, Y2_QM_Savings_Modification);
		QMaticCategorySavingsY2.put(TOTAL_KEY, Y2_QM_Savings_Total);

		//year 3 savings
		double Y3_QM_Savings_Hardware=0;
		double Y3_QM_Savings_License= (Site_Licene_QF_Sum-.8*Site_License_QFtoQM)/5;
		double Y3_QM_Savings_Deployment= 0;
		double Y3_QM_Savings_Maintenance= (Site_Maintenance_QF_Sum - .8*Site_Maintenance_QFtoQM)/5;
		double Y3_QM_Savings_Modification= 0;
		double Y3_QM_Savings_Total= Y3_QM_Savings_Hardware +  Y3_QM_Savings_License + Y3_QM_Savings_Deployment + Y3_QM_Savings_Maintenance + Y3_QM_Savings_Modification;

		//Mapping Year3 of Savings to Category Savings
		HashMap<String,Double> QMaticCategorySavingsY3 = new HashMap<String,Double>();
		QMaticCategorySavingsY3.put(HARDWARE_KEY, Y3_QM_Savings_Hardware);
		QMaticCategorySavingsY3.put(LICENSE_KEY, Y3_QM_Savings_License);
		QMaticCategorySavingsY3.put(DEPLOYMENT_KEY, Y3_QM_Savings_Deployment);
		QMaticCategorySavingsY3.put(MAINTENANCE_KEY, Y3_QM_Savings_Maintenance);
		QMaticCategorySavingsY3.put(MODIFICATION_KEY, Y3_QM_Savings_Modification);
		QMaticCategorySavingsY3.put(TOTAL_KEY, Y3_QM_Savings_Total);

		//year 4 savings
		double Y4_QM_Savings_Hardware=0;
		double Y4_QM_Savings_License= (Site_Licene_QF_Sum-.8*Site_License_QFtoQM)/5;
		double Y4_QM_Savings_Deployment= 0;
		double Y4_QM_Savings_Maintenance= (Site_Maintenance_QF_Sum - .8*Site_Maintenance_QFtoQM)/5;
		double Y4_QM_Savings_Modification= 0;
		double Y4_QM_Savings_Total=Y4_QM_Savings_Hardware +  Y4_QM_Savings_License + Y4_QM_Savings_Deployment + Y4_QM_Savings_Maintenance + Y4_QM_Savings_Modification;

		//Mapping Year4 of Savings to Category Savings
		HashMap<String,Double> QMaticCategorySavingsY4 = new HashMap<String,Double>();
		QMaticCategorySavingsY4.put(HARDWARE_KEY, Y4_QM_Savings_Hardware);
		QMaticCategorySavingsY4.put(LICENSE_KEY, Y4_QM_Savings_License);
		QMaticCategorySavingsY4.put(DEPLOYMENT_KEY, Y4_QM_Savings_Deployment);
		QMaticCategorySavingsY4.put(MAINTENANCE_KEY, Y4_QM_Savings_Maintenance);
		QMaticCategorySavingsY4.put(MODIFICATION_KEY, Y4_QM_Savings_Modification);
		QMaticCategorySavingsY4.put(TOTAL_KEY, Y4_QM_Savings_Total);

		//year 5 savings
		double Y5_QM_Savings_Hardware=0;
		double Y5_QM_Savings_License=(Site_Licene_QF_Sum-.8*Site_License_QFtoQM)/5;
		double Y5_QM_Savings_Deployment= 0;
		double Y5_QM_Savings_Maintenance=(Site_Maintenance_QF_Sum - .8*Site_Maintenance_QFtoQM)/5;
		double Y5_QM_Savings_Modification= 0;
		double Y5_QM_Savings_Total=Y5_QM_Savings_Hardware +  Y5_QM_Savings_License + Y5_QM_Savings_Deployment + Y5_QM_Savings_Maintenance + Y5_QM_Savings_Modification;


		//Mapping Year5 of Savings to Category Savings
		HashMap<String,Double> QMaticCategorySavingsY5 = new HashMap<String,Double>();
		QMaticCategorySavingsY5.put(HARDWARE_KEY, Y5_QM_Savings_Hardware);
		QMaticCategorySavingsY5.put(LICENSE_KEY, Y5_QM_Savings_License);
		QMaticCategorySavingsY5.put(DEPLOYMENT_KEY, Y5_QM_Savings_Deployment);
		QMaticCategorySavingsY5.put(MAINTENANCE_KEY, Y5_QM_Savings_Maintenance);
		QMaticCategorySavingsY5.put(MODIFICATION_KEY, Y5_QM_Savings_Modification);
		QMaticCategorySavingsY5.put(TOTAL_KEY, Y5_QM_Savings_Total);

		// adding savings to HashMap
		HashMap<String,Map<String,Double>> QMaticYearSavings = new HashMap<String,Map<String,Double>>(); //mapping QMatic to Year of Savings
		QMaticYearSavings.put("Y1", QMaticCategorySavingsY1);
		QMaticYearSavings.put("Y2", QMaticCategorySavingsY2);
		QMaticYearSavings.put("Y3", QMaticCategorySavingsY3);
		QMaticYearSavings.put("Y4", QMaticCategorySavingsY4);
		QMaticYearSavings.put("Y5", QMaticCategorySavingsY5);
		vendorSavings.put("QMatic",QMaticYearSavings);


		//format to send to Front-End
		String[] newHeaders = new String[]{"Vendor", "Year", "Hardware", "Software", "Deployment", "Maintenance", "Modification", "Total"};
		dataFrame = new H2Frame(newHeaders);
		String vendorFE="";
		String yearFE="";
		int hardwareFE =0;
		int softwareFE=0;
		int deploymentFE=0;
		int maintenanceFE=0;
		int modificationFE=0;
		int totalFE=0;
		for(String vendor : vendorSavings.keySet()) {
			if (vendor.equals(setVendor)){
				vendorFE = vendor;
				Map<String,Map<String,Double>> yearSavings = vendorSavings.get(vendorFE);
				for(String year: yearSavings.keySet()){
					yearFE = year;
						hardwareFE += vendorSavings.get(vendorFE).get(yearFE).get(HARDWARE_KEY);
						softwareFE += vendorSavings.get(vendorFE).get(yearFE).get(LICENSE_KEY);
						deploymentFE += vendorSavings.get(vendorFE).get(yearFE).get(DEPLOYMENT_KEY);
						maintenanceFE += vendorSavings.get(vendorFE).get(yearFE).get(MAINTENANCE_KEY);
						modificationFE += vendorSavings.get(vendorFE).get(yearFE).get(MODIFICATION_KEY);
						totalFE += vendorSavings.get(vendorFE).get(yearFE).get(TOTAL_KEY);
						
						Object[] row = new Object[]{vendorFE, yearFE, hardwareFE, softwareFE, deploymentFE, maintenanceFE, modificationFE, totalFE};
						dataFrame.addRow(row, newHeaders);
						data.add(row);
				}
			}
		}

	}


	//set CheckedPackages according to user input
	public void setCheckedPackages (Map<String,String> checkedPackages){
		this.checkedPackages = checkedPackages;
		data = new ArrayList<Object>();
	}
	
	//set Vendor chosen in front end
	public void setVendor (String v){
		this.setVendor = v;
	}
	
	//send info to Front-End
	public ArrayList<Object> getData() {
		return data; 
	}
}
