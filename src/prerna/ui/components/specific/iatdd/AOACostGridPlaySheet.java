package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.ui.components.playsheets.GridPlaySheet;

public class AOACostGridPlaySheet extends GridPlaySheet {

	private Map<String, Map<String, Map<String, Double>>> costHashTable = new Hashtable<String, Map<String, Map<String,Double>>>();
	private Map<String,String> checkedPackages = new HashMap<String,String>();
	private ITableDataFrame origDataFrame;
	private ArrayList<Object> data = new ArrayList<Object>();
	private Map<String, Map<String, Double>> vendorHash = new Hashtable<String, Map<String, Double>>();

	public static final String LICENSE_KEY = "License";
	public static final String HARDWARE_KEY = "Hardware";
	public static final String DEPLOYMENT_KEY = "Deployment";
	public static final String MAINTENANCE_KEY = "Maintenance";
	public static final String MODIFICATION_KEY = "Modification";

	public void runAnalytics(){
		costHashTable = new Hashtable<String, Map<String, Map<String,Double>>>();
		if(origDataFrame == null) {
			origDataFrame = dataFrame;
		}
		String[] headers = origDataFrame.getColumnHeaders();
		Iterator<IHeadersDataRow> it = origDataFrame.iterator();	
		while(it.hasNext()) {
			Object[] row = it.next().getValues();

			//getting all the costs per product per vendor  
			String vendor = (String) row [6];
			String packages = (String) row[5];
			double licensecost = 0;
			double hardwarecost = 0;
			double deploymentcost = 0;
			double maintenancecost = 0;
			double modificationcost = 0;
			//row2
			if (row[2] instanceof String){
				licensecost = Double.parseDouble((String) row[2]);
			}
			else { 
				licensecost = ((double) row [2]);
			}
			
			//row3
			if (row[1] instanceof String){
				hardwarecost = Double.parseDouble((String) row[1]);
			}
			else { 
				hardwarecost = ((double) row [1]);
			}
			
			//row4
			if (row[0] instanceof String){
				deploymentcost = Double.parseDouble((String) row[0]);
			}
			else { 
				deploymentcost = ((double) row [0]);
			}
			
			//row5
			if (row[3] instanceof String){
				maintenancecost = Double.parseDouble((String) row[3]);
			}
			else { 
				maintenancecost = ((double) row [3]);
			}
			
			//row6
			if (row[4] instanceof String){
				modificationcost = Double.parseDouble((String) row[4]);
			}
			else { 
				modificationcost = ((double) row [4]);
			}

			Hashtable<String, Double> costTable = new Hashtable<String, Double> (); 

			//checking to see what products are checked 
			boolean checked = true;
			if(checkedPackages.containsKey(packages)){
				if(checkedPackages.get(packages).equals("true"))
					checked = true;
				else
					checked = false;
			} 

			// if the product is checked, will add the product and its costs to the CostHashTable 
			Map<String, Map<String, Double>> innerHash = null;
			if(checked) {
				costTable.put(LICENSE_KEY, licensecost);
				costTable.put(HARDWARE_KEY, hardwarecost);
				costTable.put(DEPLOYMENT_KEY, deploymentcost);
				costTable.put(MAINTENANCE_KEY, maintenancecost);
				costTable.put(MODIFICATION_KEY, modificationcost);

				if(costHashTable.containsKey(vendor)) {
					innerHash = costHashTable.get(vendor);
					if(innerHash.containsKey(packages)) {
						throw new IllegalArgumentException("Package already exists...why?");
					} else {
						innerHash.put(packages, costTable);
					}
				} else {
					innerHash = new Hashtable<String, Map<String, Double>>();
					innerHash.put(packages, costTable);
					costHashTable.put(vendor, innerHash);
				}
			}
			//if product is checked, will add the product and 0 for its costs to CostHashTable 
			else {
				costTable.put(LICENSE_KEY, 0.00);
				costTable.put(HARDWARE_KEY, 0.00);
				costTable.put(DEPLOYMENT_KEY, 0.00);
				costTable.put(MAINTENANCE_KEY, 0.00);
				costTable.put(MODIFICATION_KEY, 0.00);

				if(costHashTable.containsKey(vendor)) {
					innerHash = costHashTable.get(vendor);
					if(innerHash.containsKey(packages)) {
						throw new IllegalArgumentException("Package already exists...why?");
					} else {
						innerHash.put(packages, costTable);
					}
				} else {
					innerHash = new Hashtable<String, Map<String, Double>>();
					innerHash.put(packages, costTable);
					costHashTable.put(vendor, innerHash);
				}
			}
		}

		//sum up all the checked products' costs according to breakdown 
		for (String vendor : costHashTable.keySet()){
			Map<String, Map<String,Double>> selectedVendor = costHashTable.get(vendor);
			double licensecostSum = 0;
			double hardwarecostSum = 0;
			double deploymentcostSum = 0;
			double maintenancecostSum = 0;
			double modificationcostSum = 0;

			for (String packages : selectedVendor.keySet()){
				Map<String,Double> selectedPackages = selectedVendor.get(packages);
				licensecostSum += selectedPackages.get(LICENSE_KEY);
				hardwarecostSum += selectedPackages.get(HARDWARE_KEY);
				deploymentcostSum += selectedPackages.get(DEPLOYMENT_KEY);
				maintenancecostSum += selectedPackages.get(MAINTENANCE_KEY);
				modificationcostSum += selectedPackages.get(MODIFICATION_KEY);
			}
			Map<String,Double> innerHash2 = new HashMap<String,Double>();
			innerHash2.put(LICENSE_KEY, licensecostSum);
			innerHash2.put(HARDWARE_KEY, hardwarecostSum);
			innerHash2.put(DEPLOYMENT_KEY, deploymentcostSum);
			innerHash2.put(MAINTENANCE_KEY, maintenancecostSum);
			innerHash2.put(MODIFICATION_KEY, modificationcostSum);
			vendorHash.put(vendor, innerHash2);
		}

		//format to send to Front-End
		String[] newHeaders = new String[]{headers[0], headers[2], headers[3], headers[4], headers[5], headers[6]};
		dataFrame = new H2Frame(newHeaders);
		for(String vendor : vendorHash.keySet()) {
			Double license = vendorHash.get(vendor).get(LICENSE_KEY);
			Double hardware = vendorHash.get(vendor).get(HARDWARE_KEY);
			Double deployment = vendorHash.get(vendor).get(DEPLOYMENT_KEY);
			Double maintenance = vendorHash.get(vendor).get(MAINTENANCE_KEY);
			Double modification = vendorHash.get(vendor).get(MODIFICATION_KEY);
			Object[] row = new Object[]{vendor, license, hardware, deployment, maintenance, modification};
			dataFrame.addRow(row, newHeaders);
			Hashtable<String, Object> rowValues = new Hashtable<String, Object>();
			rowValues.put(headers[6], vendor);
			rowValues.put(headers[2], license);
			rowValues.put(headers[1], hardware);
			rowValues.put(headers[0], deployment);
			rowValues.put(headers[3], maintenance);
			rowValues.put(headers[4], modification);
			data.add(rowValues);
		}
	}

	@Override
	public void processQueryData() {
		super.processQueryData();
	}

	//set CheckedPackages according to user input
	public void setCheckedPackages (Map<String,String> checkedpackages){
		this.checkedPackages = checkedpackages;
		data = new ArrayList<Object>();
	}

	public void calculateCost () {
		costHashTable.get("Vendor");
	}
	
	public ArrayList<Object> getData() {
		return data;
	}
}

