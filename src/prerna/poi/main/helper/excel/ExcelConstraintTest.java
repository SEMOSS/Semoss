package prerna.poi.main.helper.excel;

import java.util.Arrays;
import java.util.List;

import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;

public class ExcelConstraintTest {
	private static void getDataValidation(Sheet sheet) {
		List<? extends DataValidation> validations = sheet.getDataValidations();
		for (DataValidation dv : validations) {
			DataValidationConstraint constraint = dv.getValidationConstraint();
			CellRangeAddressList region = dv.getRegions();
			CellRangeAddress[] cellRangeAddresses = region.getCellRangeAddresses();
			for (CellRangeAddress rangeAddress : cellRangeAddresses) {
				String address = rangeAddress.formatAsString();
				System.out.println("Range of validation constraint: " + address);
			}
			boolean allowEmptyCells = dv.getEmptyCellAllowed();
			int validationType = constraint.getValidationType();
			System.out.println("Validation type: " + validationType);
			System.out.println("Allow empty cells: " + allowEmptyCells);
			if (validationType == DataValidationConstraint.ValidationType.ANY) {

			} else if (validationType == DataValidationConstraint.ValidationType.INTEGER) {
				constraintValues(constraint);
			} else if (validationType == DataValidationConstraint.ValidationType.DECIMAL) {
				constraintValues(constraint);
			} else if (validationType == DataValidationConstraint.ValidationType.LIST) {
				String[] values = constraint.getExplicitListValues();
				System.out.println("Values: " + Arrays.toString(values));
			} else if (validationType == DataValidationConstraint.ValidationType.DATE) {
				constraintValues(constraint);
			} else if (validationType == DataValidationConstraint.ValidationType.TIME) {
				constraintValues(constraint);
			} else if (validationType == DataValidationConstraint.ValidationType.TEXT_LENGTH) {
				constraintValues(constraint);
			} else if (validationType == DataValidationConstraint.ValidationType.FORMULA) {

			}

			System.out.println();
		}
	}


	private static void constraintValues(DataValidationConstraint constraint) {
		int operator = constraint.getOperator();
		String formula1 = constraint.getFormula1();
		String formula2 = constraint.getFormula2();
		System.out.println("Operator: " + operator);
		System.out.println("Formula1: "+formula1);
		if (formula2 != null) {
			System.out.println("Formula2: " + formula2);
		}
	}
	

	public static void main(String[] args) {
		String fileLocation = "C:\\Documents\\dropDown.xlsx";
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(fileLocation);

		Sheet sheet = helper.getSheet("Sheet1");
		getDataValidation(sheet);

	}
}
