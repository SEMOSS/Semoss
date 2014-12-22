/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.impl.specific.tap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ClusterHeatMapData {

	private final String dataFile = "clusterData.csv";
	private final String metaDataFile = "clusterMetaData.csv";
	private final String columnMetaDataFile = "clusterColumnMetaData.csv";
	private final String clusterScript = "inchlib_cluster.py";
	private final String clusterMapData = "inchlibTest.json";
	
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
	private String fileLoc;
	private boolean success = true;

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
	
	
	
	public String createFile() {
		String dataHash = createData();
		String columnMetadataHash = createColumnMetadata();
		String metadataHash = createMetadata();
		
		
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\html\\MHS-RDFSemossCharts\\app\\data\\";
		String dataFilePath = fileLoc + dataFile;
		String metaDataFilePath = fileLoc + metaDataFile;
		String columnMetaDataFilePath = fileLoc + columnMetaDataFile;
		
		try{
			FileWriter data = new FileWriter(dataFilePath);
			data.write(dataHash);
			data.flush();
			data.close();
			FileWriter metadata = new FileWriter(metaDataFilePath);
			metadata.write(metadataHash);
			metadata.flush();
			metadata.close();
			FileWriter columnMetadata = new FileWriter(columnMetaDataFilePath);
			columnMetadata.write(columnMetadataHash);
			columnMetadata.flush();
			columnMetadata.close();
		} catch(IOException e){
			e.printStackTrace();
		}
				
		try {
			Runtime.getRuntime().exec("python --version");			
		} catch (Exception e){
			e.printStackTrace();
			Utility.showError(e.getMessage());
			success = false;
		}
		

		
		if(success){
			try {
				Path path = Paths.get(fileLoc + clusterMapData);
				Files.deleteIfExists(path);
				ProcessBuilder pb = new ProcessBuilder("python", clusterScript);
				pb.directory(new File(fileLoc));
				Process p = pb.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		StringBuffer data = new StringBuffer("");
		int ch = 0;
		
		if(success){
			boolean fileFlag = false;
			File checker = new File(fileLoc + clusterMapData);
			while(!fileFlag){
				if(checker.isFile()){
					fileFlag = true;
				}
			}
			FileInputStream in = null;
			try{
				in = new FileInputStream(fileLoc + clusterMapData);
				while((ch = in.read()) != -1){
					data.append((char) ch);
				}
				in.close();
			} catch(FileNotFoundException e){
				e.printStackTrace();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		return data.toString();
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

