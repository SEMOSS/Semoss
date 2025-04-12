package test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelTest {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String downloads = "C:\\Users\\mahkhalil\\Downloads";
		String fileName = "PVCaRT_Six_Month_Procurement_History_Group_3_2024-11-19.xlsx";
		String excelPath = downloads+"\\"+fileName;
		
        IOUtils.setByteArrayMaxOverride(1_000_000_000);

		long start = System.currentTimeMillis();
		try (FileInputStream fs = new FileInputStream(excelPath);
				XSSFWorkbook workbook = new XSSFWorkbook(fs)) {
			System.out.println("loaded");
		}
		long end = System.currentTimeMillis();
		System.out.println("Total time = " + (end-start));
	}
}
