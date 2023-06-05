package engine;

import java.io.BufferedReader;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import exceptions.DBAppException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class DBApp {

	private int MaximumRowsCountinTablePage;
	private static String csvPath = "./src/data/metaData.csv";

	private String generatePath(String tableName) {
		return "./src/tables/" + tableName + "/" + tableName + ".class";
	}

	private String generateOctreePath(String tableName, String[] strarrColName) {
		return "./src/tables/" + tableName + "/" + strarrColName[0] + strarrColName[1] + strarrColName[2]  +"INDEX.class";
	}

	public void setCsv(Hashtable<String, String> htblColNameType, String key, String name,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {

		String path = "./src/data";
		File file = new File(path);
		file.mkdir();
		FileWriter csvWriter = new FileWriter(csvPath, true);
		for (String i : htblColNameType.keySet()) {
			String isKey = i.equals(key) ? "true" : "false";
			String value = htblColNameType.get(i).toString();
			String indexName = "null";
			String indexType = "null";
			String min = htblColNameMin.get(i).toString();
			String max = htblColNameMax.get(i).toString();
			csvWriter.append(name);
			csvWriter.append(",");
			csvWriter.append(i);
			csvWriter.append(",");
			csvWriter.append(value);
			csvWriter.append(",");
			csvWriter.append(isKey);
			csvWriter.append(",");
			csvWriter.append(indexName);
			csvWriter.append(",");
			csvWriter.append(indexType);
			csvWriter.append(",");
			csvWriter.append(min);
			csvWriter.append(",");
			csvWriter.append(max);
			csvWriter.append("\n");

		}
		csvWriter.flush();
		csvWriter.close();
	}

	public void init() {

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		// TODO --> DONE: Validate data types are in the allowed ones && table is not already created
		try {
		if (!checkType(htblColNameType,htblColNameMin, htblColNameMax )) {
			throw new DBAppException("The types are not compatible with the system");
		}
		File metaDataFile = new File(csvPath);
		if (metaDataFile.exists()) {
			if (checkTable(strTableName)) {
				throw new DBAppException("This table is already found");
			}
		}
			readConfig();
			Table newTable = new Table(strTableName, strClusteringKeyColumn, MaximumRowsCountinTablePage);
			setCsv(htblColNameType, strClusteringKeyColumn, strTableName, htblColNameMin, htblColNameMax);
			String path = "./src/tables";
			File file = new File(path);
			file.mkdir();
			file = new File(path + "/" + strTableName);
			file.mkdir();
			serialize(generatePath(strTableName), newTable);
		} catch (Exception e) {
			
			throw new DBAppException("Error in creating table");
		}
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		try {
			validateIndex(strTableName, strarrColName);
			String path = generatePath(strTableName);
			Table table = (Table) deserilize(path);
			Octree octree = new Octree(strTableName, strarrColName);
			populateOctree(octree, table);
			String octreePath = generateOctreePath(strTableName, strarrColName);
			table.indicesRef.add(octreePath);
			serialize(octreePath, octree);
			serialize(path,table);
			String indexName = strarrColName[0] + strarrColName[1] + strarrColName[2] + "INDEX";
			updateCsvFile(strTableName, strarrColName, indexName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException("error while creating index");
		}
	}
	
	public void validateIndex(String strTableName, String[] strarrColName) throws DBAppException {
		// make sure 3 columns are passed in strarrColName. 
		if (strarrColName.length != 3) {
			throw new DBAppException("index must be created on 3 columns");
		}
		// same octree is not already created.
		File indexFile = new File(generateOctreePath(strTableName, strarrColName));
		if (indexFile.exists()) {
			throw new DBAppException("same index already created");
		}
		// table is found 
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
		if (metadata.size() < 1) {
			throw new DBAppException("table not found");
		}
		// no shared columns between indices
		for (String[] s : metadata) {
			for (String colName: strarrColName) {
				if (s[1].equals(colName)) {
					if (!s[4].equals("null")) {
						throw new DBAppException("a column is already used in another index");
					}
				}
			}
		}
	}

	public void populateOctree(Octree octree, Table table) throws DBAppException, IOException{
		for (int i=0; i < table.pagesCount; i++) {
			String path = table.PagesRef.get(i);
			Page currentPage = (Page) DBApp.deserilize(path);
			for (Hashtable<String, Object> t : currentPage.tuples){
				octree.insert(t, path);
			}
		}
	}

	public void updateCsvFile(String tableName, String[] strarrColName, String indexName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvPath));
		String line;
		ArrayList<String[]> file = new ArrayList<String[]>();
		while ((line = br.readLine()) != null) {
			String[] lineArray = line.split(",");
			if (lineArray[0].equals(tableName)) {
				for (String col: strarrColName) {
					if (lineArray[1].equals(col)) {
						lineArray[4] = indexName;
						lineArray[5] = "Octree";
					}
				}
			}
			file.add(lineArray);
		}
		FileWriter csvWriter = new FileWriter(csvPath);
		file.forEach(arr -> {
			for (int i = 0; i < arr.length; i++) {
				try {
					csvWriter.append(arr[i]);
					csvWriter.append(",");
					if (i == arr.length - 1)
						csvWriter.append("\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		br.close();
		csvWriter.close();
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("Must have at least one sql term");
		if (arrSQLTerms.length != strarrOperators.length + 1) {
			throw new DBAppException("Wrong format, operators length do not match the sql terms");
		}
		try {
			String path = generatePath(arrSQLTerms[0]._strTableName);
			Table table = (Table) deserilize(path);
			return table.select(arrSQLTerms, strarrOperators).iterator();
		} catch (Exception e) {
			throw new DBAppException("error while selecting from table");
		}
	}
	
	// checks that the type of the columns while CREATING a table is either string, integer, double or date 
	// It also checks that the values entered in the columns of min and max are compatible with the SPECIFIED type
	public boolean checkType(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) { 
		for (String i : htblColNameType.keySet()) {
			String value = htblColNameType.get(i).toString();
			if (!(value.equalsIgnoreCase("java.lang.String")||value.equalsIgnoreCase("java.lang.Integer")||
					value.equalsIgnoreCase("java.lang.Double") || value.equalsIgnoreCase("java.util.Date"))) {
				return false; 
			}
			if (value.equalsIgnoreCase("java.lang.String")) {
				if (!((htblColNameMin.get(i) instanceof String && htblColNameMax.get(i) instanceof String))){
					return false;
				}
			}
			else if (value.equalsIgnoreCase("java.lang.Integer")) {
				try {
					int min = Integer.parseInt(htblColNameMin.get(i));
					int max = Integer.parseInt(htblColNameMax.get(i));
				}
				catch (NumberFormatException nfe ) {
					return false;
				}
			}
			else if (value.equalsIgnoreCase("java.lang.Double")) {
				try {
					double min = Double.parseDouble(htblColNameMin.get(i));
					double max = Double.parseDouble(htblColNameMax.get(i));
				}
				catch (NumberFormatException nfe ) {
					return false;
				}
			}
			else if (value.equalsIgnoreCase("java.lang.Date")) {
				try {
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");      
				    //Parsing the given String to Date object
				    Date min = formatter.parse(htblColNameMin.get(i));
				    Date max = formatter.parse(htblColNameMax.get(i));
				}
				catch (Exception e ) {
					return false;
				}
			}
		}
		return true;
	}
	
	// checks that the  table is already found or not 
	public boolean checkTable(String strTableName) {
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
		if (metadata.size()!=0) {
			return true; 
		}
		return false;
	}

	// return an arraylist conatianing all the columns of this table with their types, min and max as arrays each.
	public static ArrayList<String[]> getTableFromMetadata(String strTableName) {
		ArrayList<String[]> metadata = new ArrayList<String[]>();
		BufferedReader csvReader;
		try {
			csvReader = new BufferedReader(new FileReader(csvPath));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(strTableName)) {
					metadata.add(data);
				}
			}
			csvReader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return metadata;
	}

	// checks that the ACTUAL type of a column is the same as the SPECIFIED type of the value while INSERTING
	public static void checkColumnType(Object value, String colType, String colName) throws DBAppException {
		if (value instanceof String) {
			if (!colType.equalsIgnoreCase("java.lang.String"))
				throw new DBAppException("invalid data type of column " + colName);
		} else if (value instanceof Integer) {
			if (!colType.equalsIgnoreCase("java.lang.Integer"))
				throw new DBAppException("invalid data type of column " + colName);
		} else if (value instanceof Double) {
			if (!colType.equalsIgnoreCase("java.lang.Double"))
				throw new DBAppException("invalid data type of column " + colName);
		} else if (value instanceof Date) {
			if (!colType.equalsIgnoreCase("java.util.Date"))
				throw new DBAppException("invalid data type of column " + colName);
		} else
			throw new DBAppException("unsupported data type for column " + colName);
	}

	// comparing two objects but with handling them being of the same type 
	public static int compareTo(Object first, Object second) throws DBAppException
	{
		int returnValue = 0;
		if (first instanceof Integer) {
			second = parseType((String) second, "java.lang.Integer");
			returnValue = ((Integer) first).compareTo((Integer) second);
		}
		else if (first instanceof String) {
			returnValue = ((String) first).toLowerCase().compareTo(((String) second).toLowerCase());
		}
		else if (first instanceof Double) {
			second = parseType((String) second, "java.lang.Double");
			returnValue = ((Double) first).compareTo((Double) second);
		}
		else if (first instanceof Date) {
			second = parseType((String) second, "java.util.Date");
			returnValue = ((Date) first).compareTo((Date) second);
		}
		return returnValue;
	}

	// parses the value given in a string to its SPECIFIED type 
	public static Comparable parseType(String val, String dataType) throws DBAppException {
		try {
			if (dataType.equalsIgnoreCase("java.lang.Integer")) {
				return Integer.parseInt(val);
			}
			if (dataType.equalsIgnoreCase("java.lang.Double")) {
				return Double.parseDouble(val);
			}
			if (dataType.equalsIgnoreCase("java.util.Date")) {
				return new SimpleDateFormat("yyyy-MM-dd").parse(val);
			}
			return val.toLowerCase();
		} catch (ParseException i) {
			i.printStackTrace();
			throw new DBAppException("Cannot parse value to passed type");
		}
	}

	// check the boundaries of the inserted value that it is in the specified boundaries
	public boolean checkBoundaries(Object value, Object min, Object max) throws DBAppException {
		return compareTo(value,min) >= 0 && compareTo(value, max) <= 0;
	}

	// a bundle of the required validations for the insert of a new tuple
	public void validate(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
		// check table exist **
		if (metadata.size() == 0) {
			throw new DBAppException("Table not found");
		}
		for (Map.Entry<String, Object> col : htblColNameValue.entrySet()) {
			boolean columnFound = false;
			for (int i = 0; i < metadata.size(); i++) {
				if (metadata.get(i)[1].equals(col.getKey())) {
					columnFound = true;
					// check data type of column is correct and from allowed data types **
					checkColumnType(col.getValue(), metadata.get(i)[2], metadata.get(i)[1]);
					// check check max and minimum values **
					Object min = metadata.get(i)[6];
					Object max = metadata.get(i)[7];
					if (!checkBoundaries(col.getValue(), min, max)) {
						throw new DBAppException("Value is out of maximum and minimum bound");
					}
				}
			}
			// check column exists **
			if (!columnFound) {
				throw new DBAppException("Column not found in table");
			}
		}
	}
	
	public Hashtable<String, Object> addNullValues(String strTableName, Hashtable<String, Object> htblColNameValue) {
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
		Hashtable<String, Object> result = new Hashtable<>();
		for (String[] s : metadata) {
			if (!htblColNameValue.contains(s[1])) {
				result.put(s[1], new Null());
			}
		}
		result.putAll(htblColNameValue);
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
		for (Map.Entry<String, Object> col : htblColNameValue.entrySet()) {
			if (col.getValue() instanceof String) {
				htblColNameValue.replace(col.getKey(), ((String) col.getValue()).toLowerCase());
			}
		}
		validate(strTableName, htblColNameValue);
		htblColNameValue = addNullValues(strTableName, htblColNameValue);
		String path = generatePath(strTableName);
			Table table = (Table) deserilize(path);
			table.insert(htblColNameValue);
			for(int i=0;i<table.indicesRef.size();i++) {
				Octree tree = (Octree) deserilize(table.indicesRef.get(i));
				for(int j=0;j<table.shiftedRows.size();j++) {
					if(j>0) {
						tree.delete(table.shiftedRows.get(j).htbl,"fofa");
					}
					tree.insert(table.shiftedRows.get(j).htbl, table.shiftedRows.get(j).newPath);
				}
				serialize(table.indicesRef.get(i), tree);
			}
			table.shiftedRows.clear();
			serialize(path, table);
		}catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException("Error in inserting");
		}
	}

	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
		for (Map.Entry<String, Object> col : htblColNameValue.entrySet()) {
			if (col.getValue() instanceof String) {
				htblColNameValue.replace(col.getKey(), ((String) col.getValue()).toLowerCase());
			}
		}
		validate(strTableName, htblColNameValue);
		String path = generatePath(strTableName);
			Table table = (Table) deserilize(path);
			ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
			String type = "";
			for (int i = 0; i < metadata.size(); i++) {
				if (metadata.get(i)[1].equals(table.clusterKey)) {
					type = metadata.get(i)[2];
					break;
				}
			}
			Object strClusteringKeyValueObject = parseType(strClusteringKeyValue, type);
			if (! table.keyFound(strClusteringKeyValueObject)) {
				throw new DBAppException("This clustering key value is not found");
			}
			table.update(strClusteringKeyValueObject, htblColNameValue);
			serialize(path, table);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException("Error in updating table");
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
		for (Map.Entry<String, Object> col : htblColNameValue.entrySet()) {
			if (col.getValue() instanceof String) {
				htblColNameValue.replace(col.getKey(), ((String) col.getValue()).toLowerCase());
			}
		}
		validate(strTableName,htblColNameValue);
		String path = generatePath(strTableName);
			Table table = (Table) deserilize(path);
			table.delete(htblColNameValue);
			serialize(path, table);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException("error while deleting");
		}
	}

	public void readConfig() throws IOException, DBAppException{
		try {
		String filepath = "./src/resources/DBApp.config";
		File f= new File("resources");
		f.mkdir();
			FileInputStream reader = new FileInputStream(filepath);
			Properties properties = new Properties();
			properties.load(reader);
			String maxString = properties.getProperty("MaximumRowsCountinTablePage");
			MaximumRowsCountinTablePage = Integer.parseInt(maxString);
		} catch (Exception e) {
			throw new DBAppException(e.getMessage()); 
		}
	}

	public static Object deserilize(String path) throws DBAppException {
		Object obj = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			obj = in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
		    i.printStackTrace();
			throw new DBAppException(path + " not found");
		}
		return obj;
	}

	public static void serialize(String path, Serializable obj) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(obj);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public static void displayTableContent(String strTableName) throws DBAppException {
		Table table = (Table) deserilize("./src/tables/" + strTableName + "/" + strTableName + ".class");
		for (int i = 0; i < table.pagesCount; i++) {
			String pagePath = table.PagesRef.get(i);
			System.out.println(table.PagesRef);
			Page currentPage = (Page) DBApp.deserilize(pagePath);
			System.out.println("Page number: " + currentPage.pageNumber);
			System.out.println("Min Value: " + currentPage.minValue);
			System.out.println("Max Value: " + currentPage.maxValue);
			System.out.println("Page rows:");
			for (int j = 0; j < currentPage.tuples.size(); j++) {
				System.out.println(currentPage.tuples.get(j));
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		DBApp dbApp = new DBApp();
		
		String strTableName = "Human";
		DateFormat df = new SimpleDateFormat("yyyy-mm-dd");
		
//		// creating a new table
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.double");
//		htblColNameType.put("date" , "java.util.date");
//		
//
//		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
//		htblColNameMin.put("id", "1");
//		htblColNameMin.put("name", "A");
//		htblColNameMin.put("gpa", "0.7");
//		htblColNameMin.put("date" , "1970-1-1");
//
//		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
//		htblColNameMax.put("id", "1000000");
//		htblColNameMax.put("name", "ZZZZZZZZZZZZ");
//		htblColNameMax.put("gpa", "5");
//		htblColNameMax.put("date" , "2023-1-1");
//		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		
//		// creating index for the table
//		String[] columns = new String[] {"date", "name", "gpa"};
//        dbApp.createIndex(strTableName,  columns);
//			
//        // inserting values into the table
//		Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
//		htblColNameValue1.put("id", new Integer(30));
//		htblColNameValue1.put("name", new String("youssef"));
//		htblColNameValue1.put("gpa", new Double(0.81));
//		htblColNameValue1.put("date", df.parse("2002-1-1"));
//		dbApp.insertIntoTable(strTableName, htblColNameValue1);	
//		
//		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
//		htblColNameValue2.put("id", new Integer(78));
//		htblColNameValue2.put("name", new String("hommer"));
//		htblColNameValue2.put("gpa", new Double(0.9));
//		htblColNameValue2.put("date", df.parse("1999-08-01"));
//		dbApp.insertIntoTable(strTableName, htblColNameValue2);
//		
//		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
//		htblColNameValue3.put("id", new Integer(45));
//		htblColNameValue3.put("name", new String("marwan"));
//		htblColNameValue3.put("gpa", new Double(2.9));
//		htblColNameValue3.put("date", df.parse("2011-1-1"));
//		dbApp.insertIntoTable(strTableName, htblColNameValue3);
//		
//		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
//		htblColNameValue4.put("id", new Integer(26));
//		htblColNameValue4.put("name", new String("ziko"));
//		htblColNameValue4.put("gpa", new Double(1.99));
//		htblColNameValue4.put("date", df.parse("1977-1-1"));
//		dbApp.insertIntoTable(strTableName, htblColNameValue4);
//		
//		// update table
//		Hashtable<String, Object> update = new Hashtable<String, Object>();
//		update.put("gpa", new Double(0.9));
//		dbApp.updateTable(strTableName, "26" , update);
//	
//		// delete from table
//		Hashtable<String, Object> delete = new Hashtable<String, Object>();
//		delete.put("id", new Integer(295));
//		delete.put("name", new String("curry"));
//		delete.put("gpa", new Double(0.81));
//		dbApp.deleteFromTable(strTableName, delete);	
//		
//		// select from table
//		SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        arrSQLTerms[0] = new SQLTerm("Human", "date", "<", df.parse("2000-1-1"));
//        arrSQLTerms[1] = new SQLTerm("Human", "name", "<", "d");
//        arrSQLTerms[2] = new SQLTerm("Human", "gpa", "<", new Double(1.0));
//
//        String[]strarrOperators = new String[2];
//        strarrOperators[0] = "XOR";
//        strarrOperators[1] = "AND";
//
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//        while(resultSet.hasNext())
//        {
//                System.out.print(resultSet.next());
//        }
//      
//        // display table content
//		displayTableContent(strTableName);
//		
//		// display octree index
//		String path = dbApp.generatePath(strTableName);
//		Table table = (Table) deserilize(path);
//		Octree tree = (Octree) deserilize(table.indicesRef.get(0));
//		tree.printTree(tree.root, 0);
	}
}
