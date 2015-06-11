package prerna.ui.components.specific.tap;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;



	/** Matrix Class
	 *   This class is intended for Matrix multiplication, masking, and addition for matrices. The intent
	 *   is to allow custom column and row headers such that the ordering of them not necessary.
	 *   
	 * @author jvidalis Joseph Vidalis jvidalis@deloitte.com
	 * @author rluthar Rishi Luthar rluthar@deloitte.com
	 */
public class MatrixHashMap {
	private HashMap<String, HashMap<String,Double>> data;
	private Set<String> rows;
	private Set<String> columns;

	/**
	 *   This is the constructor for the Matrix class. The input is the HashMap<String, HashMap<String, Double>>
	 *   This is how data is generally stored within the matrix class. The constructor also breaks out the rows
	 *   and columns into Set<String>. 
	 * @param Input HashMap<String, HashMap<String, Double>> matrix representation
	 */
	public MatrixHashMap(HashMap<String, HashMap<String,Double>> Input)
	{
		data = Input;
		rows = new HashSet<String>();
		columns = new HashSet<String>();
		if( !Input.keySet().isEmpty() )
		{
			for(String row: Input.keySet()) {
				rows.add(row);
				if( !Input.get(row).keySet().isEmpty() )
				{
					for(String column: data.get(row).keySet()) {
						columns.add(column);
					}
				}
			}
		}
		
	}
	
	/**
	 * 	 Returns the matrix itself.
	 * @return HashMap<String, HashMap<String, Double>> matrix 
	 */
	public HashMap<String, HashMap<String,Double>> getMatrix(){return data;}
	/**
	 * 	 Returns the row headers of the matrix.
	 * @return Set<String> rowHeaders
	 */
	public Set<String> getRows(){return rows;}
	/**
	 * 	 Returns the row headers of the matrix.
	 * @return Set<String> columnHeaders
	 */
	public Set<String> getColumns(){return columns;}
	
	/**
	 * Returns the row as a HashMap<String,Double> of the Matrix
	 * @param row String value representing the row header of the row requested
	 * @return values HashMap<String,Double> 
	 */
	public HashMap<String,Double> getRow( String row )
	{
		return this.getMatrix().get(row);
	}
	
	/**
	 * Returns the column as a HashMap<String,Double> of the Matrix
	 * @param column String value representing the column header of the column requested
	 * @return values HashMap<String,Double> 
	 */
	public HashMap<String,Double> getColumn( String col )
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		HashMap<String,Double> result = new HashMap<String,Double>();
		for( String row : matrix.keySet())
		{
			if( matrix.get(row).keySet().contains(col) )
			{
				result.put(row, matrix.get(row).get(col));
			}
		}
		return result;
	}
	
	/**
	 * Returns the sum values of each row as a HashMap<String,Double> of the Matrix
	 * 
	 * @return values HashMap<String,Double>  
	 */
	public MatrixHashMap getRowSum()
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		HashMap<String, HashMap<String,Double>> outerResult = new HashMap<String, HashMap<String,Double>> ();
		for( String row : matrix.keySet())
		{
			HashMap<String,Double> result = new HashMap<String,Double>();
			Double sum = 0.0;
			for( String col : matrix.get(row).keySet())
			{
				sum += matrix.get(row).get(col);
			}
			result.put("Sum",sum);
			outerResult.put(row,result);
		}
		return new MatrixHashMap(outerResult);
	}
	/**
	 * Returns the sum values of each column as a HashMap<String,Double> of the Matrix
	 * 
	 * @return values HashMap<String,Double>  
	 */
	public MatrixHashMap getColumnSum() {
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		HashMap<String,Double> Result = new HashMap<String,Double>();
		HashMap<String, HashMap<String,Double>> outerResult = new HashMap<String, HashMap<String,Double>> ();
		for( String row : matrix.keySet())
		{
			for( String col : matrix.get(row).keySet())
			{
				if( Result.keySet().contains(col))
					Result.put(col, Result.get(col)+matrix.get(row).get(col));
				else
					Result.put(col, matrix.get(row).get(col));
			}
		}
		outerResult.put("Sum",Result);
		return new MatrixHashMap(outerResult);
	}
	
	/**
	 * Returns the complete sum of every value in the matrix
	 * 
	 * @return Sum
	 */
	public Double getSum()
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		Double sum = 0.0;
		for( String row : matrix.keySet())
		{
			for( String col : matrix.get(row).keySet())
			{
				sum += matrix.get(row).get(col);
			}
		}
		return sum;
	}
	
	/**
	 * Returns a boolean that is true if every matrix value expresses the following logic as true:
	 *  -epsilon < value < epsilon
	 * @param epsilon
	 * @return
	 */
	public boolean isZeros( Double epsilon )
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		for( String row : matrix.keySet())
		{
			for( String col : matrix.get(row).keySet())
			{
				if( matrix.get(row).get(col) > epsilon || matrix.get(row).get(col) < -1*epsilon){return false;}
			}
		}
		return true;
	}
	
	/**
	 * Returns a boolean that is true if every matrix value expresses the following logic as true:
	 *  -.000001 < value < .000001
	 * @param epsilon
	 * @return
	 */
	public boolean isZeros()
	{
		return this.isZeros(.000001);
	}
	
	/**
	 * Returns a string representation of the matrix
	 * @return String
	 */
	public String toString()
	{
		String ret = "";
		for (String keyOne : data.keySet()) 
		{
			ret += keyOne + " : ";
			for (String keyTwo : data.get(keyOne).keySet()) {ret+="("+keyTwo + ":" + data.get(keyOne).get(keyTwo).toString()+")";}
			ret += "\n";
		}
		return ret;
	}
	
	/**
	 * This adds two HashMap<String,Double> vectors together and returns a new HashMap<String,Double> back.
	 * 
	 * @param HashMap<String, Double> firstHashMap
	 * @param HashMap<String, Double> secondHashMap
	 * @return HashMap<String, Double>
	 */
	private HashMap<String, Double> addVector(HashMap<String,Double> firstHashMap, HashMap<String,Double> secondHashMap )
	{
		HashMap<String, Double> finalMap = new HashMap<String, Double>();
		for (String keyOne : firstHashMap.keySet()) {
			finalMap.put(keyOne, firstHashMap.get(keyOne));
		}
		//Add Number Two
		for (String keyTwo : secondHashMap.keySet()) {
			if (!finalMap.containsKey(keyTwo))
				finalMap.put(keyTwo, secondHashMap.get(keyTwo));
			else
				finalMap.put(keyTwo, secondHashMap.get(keyTwo) + finalMap.get(keyTwo));
		}
		return finalMap;
	}
	
	/**
	 * This adds a Matrix to this matrix. The result is a new Matrix.
	 * 
	 * @param Matrix
	 * @return Matrix
	 */
	public MatrixHashMap addMatrix(MatrixHashMap secondMatrix)
	{
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> firstHashMap = this.getMatrix();
		HashMap<String, HashMap<String, Double>> secondHashMap = secondMatrix.getMatrix();
		for (String keyOne : firstHashMap.keySet()) {
			finalMap.put(keyOne, firstHashMap.get(keyOne));
		}
		
		for (String keyTwo : secondHashMap.keySet()) {
			if( !finalMap.containsKey(keyTwo) )
				finalMap.put(keyTwo, secondHashMap.get(keyTwo));
			else
				finalMap.put(keyTwo, addVector(secondHashMap.get(keyTwo), finalMap.get(keyTwo)));
		}
		
		return new MatrixHashMap(finalMap);
	}

	/**
	 * Multiplies a matrix to this matrix. The result is a new Matrix. Assumed orientation is
	 *  ThisMatrix * InputMatrix
	 * 
	 * @param InputMatrix
	 * @return Matrix
	 */
	public MatrixHashMap multiplyMatrix(MatrixHashMap secondMatrix)
	{
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> firstHashMap = this.getMatrix();
		HashMap<String, HashMap<String, Double>> secondHashMapT = secondMatrix.transposeMatrix().getMatrix();
		for (String keyOne : firstHashMap.keySet()) {
			if (!finalMap.containsKey(keyOne)) {
				if (firstHashMap.get(keyOne) != null) {
					finalMap.put(keyOne, new HashMap<String, Double>());
				} else {
					continue;
				}
			}
			for (String keyTwo : secondHashMapT.keySet()) {
				if (secondHashMapT.get(keyTwo) != null) {
				Double score = 0.0;
				for (String innerKey : secondHashMapT.get(keyTwo).keySet()) {
					if (firstHashMap.get(keyOne).get(innerKey) != null && secondHashMapT.get(keyTwo).get(innerKey) != null) {
						score += secondHashMapT.get(keyTwo).get(innerKey) * firstHashMap.get(keyOne).get(innerKey);
					}
				}
					finalMap.get(keyOne).put(keyTwo, score);
				} else {
					continue;
				}
			}
		}		
		return new MatrixHashMap(finalMap);
	}
	
	
	/**
	 * Returns the transpose of this matrix, a matrix in which the rows become the columns and the columns 
	 *  become the rows.
	 * 
	 * @return Matrix
	 */
	public MatrixHashMap transposeMatrix()
	{
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> firstHashMap = this.getMatrix();
		for (String keyOne : firstHashMap.keySet())
		{
			HashMap<String, Double> innerHash = firstHashMap.get(keyOne);
			for (String keyTwo : innerHash.keySet())
			{
				if( finalMap.containsKey(keyTwo) )
				{
					HashMap<String, Double> innerFinalHash = finalMap.get(keyTwo);
					innerFinalHash.put(keyOne, innerHash.get(keyTwo));
					finalMap.put(keyTwo, innerFinalHash);
				}
				else
				{
					HashMap<String, Double> innerFinalHash = new HashMap<String, Double>();
					innerFinalHash.put(keyOne, innerHash.get(keyTwo));
					finalMap.put(keyTwo, innerFinalHash);
				}
			}
		}
		return new MatrixHashMap(finalMap);
	}
	
	/**
	 * Returns a new Matrix that is this matrix masked by the input matrix. Mask works by keeping any value in 
	 *  this matrix where there is a value > 0 in the same row, column of the input matrix.
	 * @param mask Matrix used as a mask to this matrix
	 * @return Matrix
	 */
	public MatrixHashMap maskMatrix(MatrixHashMap mask)
	{
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> firstHashMap = this.getMatrix();
		HashMap<String, HashMap<String, Double>> secondHashMap = mask.getMatrix();
		for(String keyOne : firstHashMap.keySet() )
		{
			//System.out.println("FirstKey: "+ keyOne);
			if( secondHashMap.containsKey(keyOne))
			{
				for(String keyTwo : firstHashMap.get(keyOne).keySet() )
				{
					
					if( secondHashMap.get(keyOne).containsKey(keyTwo))
					{
						if(secondHashMap.get(keyOne).get(keyTwo) > 0 )
						{
							Double value = firstHashMap.get(keyOne).get(keyTwo);
							if( finalMap.containsKey(keyOne))
							{
								HashMap<String, Double> innerHash = finalMap.get(keyOne);
								innerHash.put(keyTwo, value);
								finalMap.put(keyOne,innerHash);
							}
							else
							{
								HashMap<String, Double> innerHash = new HashMap<String,Double>();
								innerHash.put(keyTwo, value);
								finalMap.put(keyOne,innerHash);
							}
						}
					}
				}
			}
		}
		return new MatrixHashMap(finalMap);
	}
	
	
	/**
	 * Masks the matrix along the rows based on the row headers inputed as an Set<String>. The
	 *  result is a new matrix. Row headers present in the Set are those that will be present in
	 *  the matrix iff the rows headers already existed in the matrix.
	 *  
	 * @param mask Set<String> set of row headers.
	 * @return Matrix
	 */
	public MatrixHashMap maskRows(Set<String> mask)
	{
		HashMap<String, HashMap<String, Double>> finalMap = new HashMap<String, HashMap<String, Double>>();
		HashMap<String, HashMap<String, Double>> firstHashMap = this.getMatrix();
		for( String keyOne : firstHashMap.keySet() )
		{
			if( mask.contains(keyOne))
			{
				HashMap<String, Double> innermap = firstHashMap.get(keyOne);
				finalMap.put(keyOne, innermap);
			}
		}
		return new MatrixHashMap(finalMap);
	}
	
	/**
	 * Masks the matrix along the rows based on the column headers inputed as an Set<String>. The
	 *  result is a new matrix. Column headers present in the Set are those that will be present in
	 *  the matrix iff the Column headers already existed in the matrix. WARNING: This method will be much
	 *  slower than filtering by rows due to the nature of the data structure. One may be able to avoid using
	 *  this method by structuring their algorithms such that filtering can be done on rows.
	 *  
	 * @param mask Set<String> set of column headers.
	 * @return Matrix
	 */
	public MatrixHashMap maskColumns(Set<String> mask)
	{
		return this.transposeMatrix().maskRows(mask).transposeMatrix();
	}
	
	/**
	 * Takes a set of Strings to apply as a Mask to the columns of the matrix. The return is a new mask 
	 *  in which the remaining rows that are not equal to zero become a new mask set of strings.
	 * 
	 * @param maskSet Set<String>
	 * @return
	 */
	public Set<String> getRowMask(Set<String> maskSet) {
		MatrixHashMap maskRows = this.maskColumns(maskSet).getRowSum();
		Set<String> mask = new HashSet<>();
		
		for(String row: maskRows.getRows()) {
			if(maskRows.getRow(row).get("Sum") > 0.0) {
				mask.add(row);
			}
		}
		
		return mask;
	}
	
	/**
	 * Takes a set of Strings to apply as a Mask to the rows of the matrix. The return is a new mask 
	 *  in which the remaining columns that are not equal to zero become a new mask set of strings.
	 * 
	 * @param maskSet Set<String>
	 * @return
	 */
	public Set<String> getColumnMask(Set<String> maskSet) {
		
		MatrixHashMap maskRows = this.maskRows(maskSet).getColumnSum();
		Set<String> mask = new HashSet<>();
		
		for(String col: maskRows.getColumns()) {
			if(maskRows.getColumn(col).get("Sum") > 0.0) {
				mask.add(col);
			}
		}
		
		return mask;
	}
	
	/**
	 * Multiples the rows of the matrix by a scaler vector in the form of a HashMap<String,Double>. The
	 *  result is the matrix in which each value is multiplied by the value in the same row as the vector.
	 * @param scalarVector HashMap<String,Double>
	 * @return Matrix
	 */
 	public MatrixHashMap multiplyRowsByScalarVector( HashMap<String,Double> scalarVector)
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		HashMap<String,HashMap<String,Double>> result = new HashMap<String,HashMap<String,Double>>();
		for( String row : scalarVector.keySet())
		{
			if(matrix.keySet().contains(row))
			{
				HashMap<String, Double> innerHash = matrix.get(row);
				HashMap<String, Double> innerResult = new HashMap<String,Double>();
				for( String column : innerHash.keySet())
				{
					innerResult.put(column, innerHash.get(column) * scalarVector.get(row));
				}
				result.put(row, innerResult);
			}
		}
		return new MatrixHashMap(result);
	}
	
 	/**
	 * Multiples the rows of the matrix by a scalar in the form of a Double. The
	 *  result is the matrix in which each value is multiplied by the value of the scaler.
	 * @param scalar Double
	 * @return Matrix
	 */
	public MatrixHashMap multiplyByScalar( Double scalar )
	{
		HashMap<String,HashMap<String,Double>> matrix = this.getMatrix();
		HashMap<String,HashMap<String,Double>> result = new HashMap<String,HashMap<String,Double>>();
		for( String row : matrix.keySet())
		{
			HashMap<String, Double> innerHash = matrix.get(row);
			HashMap<String, Double> innerResult = new HashMap<String,Double>();
			for( String column : innerHash.keySet())
			{
				innerResult.put(column, innerHash.get(column) * scalar);
			}
			result.put(row, innerResult);
		}
		return new MatrixHashMap(result);
	}
	
	/**
	 * Determines if the input matrix is equal to this matrix within the allotted epsilon value.
	 * @param matrix2
	 * @param epsilon
	 * @return boolean
	 */
	public boolean isEqual(MatrixHashMap matrix2, double epsilon) {
		
		//if the two matrices are subsets of each other, then they are equal
		if(this.isSubset(matrix2, epsilon) && matrix2.isSubset(this, epsilon)) {
			return true;
		} else {
			return false;
		}
		
	}
	
	/**
	 * Determines if the input matrix is equal to this matrix within the allotted epsilon value of .00001.
	 * @param matrix2
	 * @return boolean
	 */
	public boolean isEqual(MatrixHashMap matrix2) 
	{
		return this.isEqual(matrix2, .000001);
	}
	
	/**
	 * Determines if the matrix is a subset of the second matrix within the allotted epsilon value.
	 * @param matrix2
	 * @param epsilon
	 * @return boolean
	 */
	public boolean isSubset(MatrixHashMap matrix2, double epsilon) {

		Set<String> rows2 = matrix2.getRows();
		Set<String> cols2 = matrix2.getColumns();
		
		//Is This matrix a subset of matrix2
		for(String row: this.rows) {
			
			HashMap<String, Double> row1 = this.getRow(row);
			HashMap<String, Double> row2;
			if(rows2.contains(row)) {
				row2 = matrix2.getRow(row);
			} else {
				double rowSum=0.0;
				for(String col: this.columns) {
					rowSum = rowSum + row1.get(col);
				}
				if(rowSum==0.0) {
					continue;
				}
				else {
					return false;
				}
			}
			
			double value1;
			double value2;
			for(String col: this.columns) {
				
				value1 = row1.get(col).doubleValue();
				if(cols2.contains(col)) {
					value2 = row2.get(col).doubleValue();
				} else {
					if(value1 == 0.0) {
						continue;
					} else {
						return false;
					}
				}
				double v = value1 - value2;
				if(Math.abs(v) > epsilon) {
					return false;
				}
			}
		}
		return true;
	}	

	
	/**
	 * Main contains uses of every function of the MatrixHashMap as test cases.
	 */
	public static void main(String[] args)
	{
		HashMap<String, Double> inner = new HashMap<String, Double>();
		HashMap<String, Double> inner2 = new HashMap<String, Double>();
		inner.put("1", 1.0);
		inner.put("2", 2.0);
		inner.put("3", 3.0);
		inner.put("4", 4.0);
		
		inner2.put("1", 2.0);
		inner2.put("2", 3.0);
		inner2.put("3", 4.0);
		inner2.put("4", 5.0);
		
		MatrixHashMap M = new MatrixHashMap(new HashMap<String,HashMap<String,Double>>());
		
		HashMap<String, HashMap<String,Double>> firstHashMap = new HashMap<String, HashMap<String,Double>>();
		firstHashMap.put("a", inner);
		firstHashMap.put("b", M.addVector(inner,inner));
		firstHashMap.put("c", inner2);
		firstHashMap.put("d", M.addVector(inner2,inner2));
		//firstHashMap.put("3", addVector(inner,inner2));
		
		MatrixHashMap M1 = new MatrixHashMap(firstHashMap);
		
		inner = new HashMap<String, Double>();
		inner2 = new HashMap<String, Double>();
		inner.put("1", 0.1);
		inner.put("2", 2.0);
		inner.put("3", 0.5);
		inner2.put("1", 1.0);
		inner2.put("2", 2.0);
		inner2.put("3", 3.0);
		
		
		
		HashMap<String, HashMap<String,Double>> secondHashMap = new HashMap<String, HashMap<String,Double>>();
		secondHashMap.put("a", inner);
		secondHashMap.put("b", M.addVector(inner,inner));
		secondHashMap.put("e", M.addVector(inner,inner2));
		
		MatrixHashMap M2 = new MatrixHashMap(secondHashMap);
		
		inner = new HashMap<String, Double>();
		inner.put("1", 0.0);
		inner.put("2", 0.0);
		inner.put("3", 0.0);
		
		HashMap<String, HashMap<String,Double>> thirdHashMap = new HashMap<String, HashMap<String,Double>>();
		thirdHashMap.put("a", inner);
		thirdHashMap.put("b", inner);
		thirdHashMap.put("c", inner);
		
		MatrixHashMap M3 = new MatrixHashMap(thirdHashMap);
		
		System.out.println("M1\n" +M1);
		System.out.println("M2\n" +M2);
		
		MatrixHashMap add = M1.addMatrix(M2);
		System.out.println("Added M1 and M2: \n" + add);
		
		MatrixHashMap M2T = M2.transposeMatrix();
		System.out.println("M2 Transposed (M2T): \n" + M2T);
		
		MatrixHashMap mult = M1.multiplyMatrix(M2T);
		System.out.println("Multiplied M1 and M2T: \n" + mult);
		
		System.out.println("getRow a of M1: \n" );
		print(M1.getRow("a"));
		
		System.out.println("getColumn 1 of M2: \n" );
		print(M2.getColumn("1"));
		
		System.out.println("M1.getRowSum(): \n" );
		System.out.println(M1.getRowSum());
		
		System.out.println("M1.getColumnSum(): \n" );
		System.out.println(M1.getColumnSum());
		
		System.out.println("M1.getSum(): \n" );
		System.out.println(M1.getSum());
		
		System.out.println("M3:\n"+M3);
		
		System.out.println("M3 is a zeros Matrix");
		System.out.println("M3.isZeros() = "+M3.isZeros());
		System.out.println("M3.getRows():");
		print(M3.getRows());
		System.out.println("M3.getColumns():");
		print(M3.getColumns());
		
		inner = new HashMap<String, Double>();
		inner2 = new HashMap<String, Double>();
		inner.put("1", 0.0);
		inner.put("2", 1.0);
		inner.put("3", 0.0);
		inner2.put("1", 1.0);
		inner2.put("2", 0.0);
		inner2.put("3", 1.0);
		HashMap<String,HashMap<String,Double>> m4 = new HashMap<String,HashMap<String,Double>>();
		m4.put("a", inner);
		m4.put("b", inner2);
		m4.put("c", inner);
		
		MatrixHashMap M4 = new MatrixHashMap(m4);
		System.out.println("Matrix M4: \n" + M4);
		
		System.out.println("Matrix M1 masked by M4: \n" + M1.maskMatrix(M4));
		
		Set<String> mask = new HashSet<String>();
		mask.add("e");
		mask.add("4");
		System.out.println("Matrix M1+M2 masked on rows by \"e\" \n" + add.maskRows(mask));
		System.out.println("Matrix M1+M2 masked on columns by \"4\" \n" + add.maskColumns(mask));
		
		System.out.println("Mask and Generate New Mask of Rows on M1+M2 by \"4\" \n");
		print(add.getRowMask(mask));
		
		System.out.println("Mask and Generate New Mask of Columns on M1+M2 by \"e\" \n");
		print(add.getColumnMask(mask));
		
		System.out.println("Multiplication of M1 by the scalar of 4\n" + M1.multiplyByScalar(4.0));
		
		HashMap<String,Double> scalar = new HashMap<String,Double>();
		scalar.put("a", 1.0);
		scalar.put("b", 2.0);
		scalar.put("c", 3.0);
		scalar.put("d", 4.0);
		System.out.println("Multiplication of M1 by the scalar Vector acrros rows [1, 2, 3, 4]\n" + M1.multiplyRowsByScalarVector(scalar));
		
		System.out.println("Does M1 equal M2:" + M1.isEqual(M2));
		System.out.println("Does M1 equal M1:" + M1.isEqual(M1));
		System.out.println("Does M2 equal M2:" + M2.isEqual(M2));
		System.out.println("Does M3 equal M1:" + M3.isEqual(M1));
	}
	
	private static void print( HashMap<String,Double> map)
	{
		for( String s : map.keySet())
			System.out.println(s + ":" + map.get(s));
	}
	
	private static void print( Set<String> S)
	{
		for( String s : S)
			System.out.println(s);
	}
}
