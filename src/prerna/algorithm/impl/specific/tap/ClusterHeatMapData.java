package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

public class ClusterHeatMapData {

	private ArrayList<String> sysList;
	private ArrayList<String> dataList;
	private ArrayList<String> bluList;
	private double[] systemIsModernized;
	private String[] sysLPI;
	private String[] sysMHSSpecific;
	private int[] sysTheater;
	private int[] sysGarrison;
	private int[] provideDataBLUNow;
	private int[] provideDataBLUFuture;
	private int[][] sysDataMatrix;
	private int[][] sysBLUMatrix;

	public void setData(ArrayList<String> sysList, ArrayList<String> dataList, ArrayList<String> bluList, double[] systemIsModernized, String[] sysLPI, String[] sysMHSSpecific, int[] sysTheater, int[] sysGarrison, int[] provideDataBLUNow, int[] provideDataBLUFuture, int[][] sysDataMatrix, int[][] sysBLUMatrix) {
		this.sysList = sysList;
		this.dataList = dataList;
		this.bluList = bluList;
		this.systemIsModernized = systemIsModernized;
		this.sysLPI = sysLPI;
		this.sysMHSSpecific = sysMHSSpecific;
		this.sysTheater = sysTheater;
		this.sysGarrison = sysGarrison;
		this.provideDataBLUNow = provideDataBLUNow;
		this.provideDataBLUFuture = provideDataBLUFuture;
		this.sysDataMatrix = sysDataMatrix;
		this.sysBLUMatrix = sysBLUMatrix;
	}
	
	public void createFile() {
		String dataHash = createData();
		String columnMetadataHash = createColumnMetadata();
		String metadataHash = createMetadata();
		
		System.out.println("dataHash");
		System.out.println(dataHash);
		System.out.println("columnMetadataHash");
		System.out.println(columnMetadataHash);
		System.out.println("metadataHash");
		System.out.println(metadataHash);	
	}
	
	private String convertArrayToRow(Object[] list) {
		String row = "";
		for(int i=0;i<list.length;i++)
			row+=list[i]+",";
		if(row.length()>0)
			row = row.substring(0,row.length()-1)+"\n";
		return row;
	}

	public String createData() {
		String data = "";
		
		String[] headers = new String[1+dataList.size()+bluList.size()];
		headers[0] = "System";
        for(int i=0;i<dataList.size();i++)
        	headers[1+i] = dataList.get(i);   
        for(int i=0;i<bluList.size();i++)
        	headers[1+dataList.size()+i] = bluList.get(i);      
        
        data += convertArrayToRow(headers);
        
        for(int sysInd=0;sysInd<sysList.size();sysInd++) {
        	Object[] rowList = new Object[1+dataList.size()+bluList.size()];
        	rowList[0] = sysList.get(sysInd);
            for(int i=0;i<sysDataMatrix[0].length;i++)
            	rowList[1+i] = sysDataMatrix[sysInd][i];   
            for(int i=0;i<sysBLUMatrix[0].length;i++)
            	rowList[1+dataList.size()+i] = sysBLUMatrix[sysInd][i]; 
            data += convertArrayToRow(rowList);
        }
        return data;
	}
	
	//still add in theater/garrison lpi/lpni/high
	private String createMetadata() {
		String metadata = "";
		
		String[] headers;
		if(sysTheater==null||sysGarrison==null)
			headers = new String[4];
		else
			headers = new String[6];
		headers[0] = "System";
		headers[1] = "Action";
		headers[2] = "Probability";
		headers[3] = "MHS Specific";
		if(sysTheater!=null&&sysGarrison!=null) {
			headers[4] = "Deployed in Theater";
			headers[5] = "Deployed in Garrison";
		}
		metadata += convertArrayToRow(headers);
	    
        for(int sysInd=0;sysInd<sysList.size();sysInd++) {
        	Object[] rowList = new Object[headers.length];
        	rowList[0] = sysList.get(sysInd);
			if(systemIsModernized[sysInd]>0)
				rowList[1] = "Modernize";
			else
				rowList[1] = "Decommission";
			rowList[2] = sysLPI[sysInd];
			rowList[3] = sysMHSSpecific[sysInd];
			if(sysTheater!=null&&sysGarrison!=null) {
				rowList[4] = sysTheater[sysInd];
				rowList[5] = sysGarrison[sysInd];
			}
			metadata += convertArrayToRow(rowList);
        }

		return metadata;
	}
	
	private String createColumnMetadata() {
		String columnMetadata = "";
		
		Object[] dataBLU = new Object[1+dataList.size()+bluList.size()];
		dataBLU[0] = "Data or BLU";
		for(int i = 0;i<dataList.size();i++)
			dataBLU[1+i] = "Data";
		for(int i = 0;i<bluList.size();i++)
			dataBLU[1+dataList.size()+i] = "BLU";
		columnMetadata += convertArrayToRow(dataBLU);
		
		Object[] future = new Object[1+dataList.size()+bluList.size()];
		future[0] = "Provided in Future";
		for(int i = 0;i<provideDataBLUFuture.length;i++)
			future[1+i] = provideDataBLUFuture[i];
		columnMetadata += convertArrayToRow(future);
		
		Object[] now = new Object[1+dataList.size()+bluList.size()];
		now[0] = "Provided Now";
		for(int i = 0;i<provideDataBLUNow.length;i++)
			now[1+i] = provideDataBLUNow[i];
		columnMetadata += convertArrayToRow(now);
		
		return columnMetadata;
	}


}

