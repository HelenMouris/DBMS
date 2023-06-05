package engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import exceptions.DBAppException;

public class Table implements Serializable {

	private static final long serialVersionUID = 2746483527410304245L;
	public String tableName;
	public String clusterKey;
	public int pagesCount;
	public Vector<String> PagesRef; // path of pages
	public int maxRowsPerPage;
	public Vector<String> indicesRef; // path of index
	public Vector<Bucket> shiftedRows; // the rows that changed their page and at the first bucket the newly inserted row
	public int lastPageId = 0;

	public Table(String tableName, String clusterKey, int maximumRowsCountinTablePage) {
		this.tableName = tableName;
		this.clusterKey = clusterKey;
		this.maxRowsPerPage = maximumRowsCountinTablePage;
		this.pagesCount = 0;
		this.PagesRef = new Vector();
		this.indicesRef = new Vector();
		this.shiftedRows = new Vector();
	}

	public String generatePagePath(int count) {
		return "./src/tables/" + tableName + "/" + count + ".class";
	}

	private String generateOctreePath(String indexName) {
		return "./src/tables/" + tableName + "/" + indexName + ".class";
	}

	public void insert(Hashtable<String, Object> values)
			throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {
		boolean inserted = false;
		// case1: No pages inserted in table yet
		if (pagesCount == 0) {
			Page newPage = new Page(tableName, clusterKey);
			newPage.key = this.clusterKey;
			if (newPage.keyFound(values.get(clusterKey))) {
				throw new DBAppException("This clustering key is already found!");
			}
			newPage.insert(values);
			newPage.pageNumber = lastPageId;
			Bucket b = new Bucket(values, newPage.generatePath());
			shiftedRows.add(b);
			inserted = true;
			String path = generatePagePath(pagesCount);
			DBApp.serialize(path, newPage);
			PagesRef.add(path);
			pagesCount++;
		} else {
			for (int i = 0; i < pagesCount && inserted == false; i++) {

				Object NewPrimaryKey = values.get(this.clusterKey);
				String pagePath = PagesRef.get(i);
				Page currentPage = (Page) DBApp.deserilize(pagePath);
				// if primary key is between max and min OR less than min
				if ((((Comparable) currentPage.maxValue).compareTo((Comparable) NewPrimaryKey) > 0
						&& ((Comparable) currentPage.minValue).compareTo((Comparable) NewPrimaryKey) < 0)
						|| ((Comparable) currentPage.minValue).compareTo((Comparable) NewPrimaryKey) >= 0) {
					// case2: there is space
					if (maxRowsPerPage != currentPage.tuples.size()) {
						if (currentPage.keyFound(values.get(clusterKey))) {
							throw new DBAppException("This clustering key is already found!");
						}
						currentPage.insert(values);
						DBApp.serialize(pagePath, currentPage);
						Bucket b = new Bucket(values, currentPage.generatePath());
						shiftedRows.add(b);
						inserted = true;
						break;
						// case3: there is no space
						// will not break or set inserted to true to keep looping to insert the last
						// element to next page
					} else {
						Hashtable<String, Object> LastElement = currentPage.tuples.remove(maxRowsPerPage - 1);
						if (currentPage.keyFound(values.get(clusterKey))) {
							throw new DBAppException("This clustering key is already found!");
						}
						currentPage.insert(values);
						DBApp.serialize(pagePath, currentPage);
						Bucket b = new Bucket(values, currentPage.generatePath());
						shiftedRows.add(b);
						values = LastElement;
					}
				}
				// if primary key is greater than max
				else if (((Comparable) currentPage.maxValue).compareTo((Comparable) NewPrimaryKey) < 0) {
					// case4: there is space AND it is less than minimum of the next page
					if (maxRowsPerPage != currentPage.tuples.size()) {
						if (i < pagesCount - 1) {
							Page nextPage = (Page) DBApp.deserilize(PagesRef.get(i + 1));
							if (((Comparable) nextPage.minValue).compareTo((Comparable) NewPrimaryKey) > 0) {
								if (currentPage.keyFound(values.get(clusterKey))) {
									throw new DBAppException("This clustering key is already found!");
								}
								currentPage.insert(values);
								DBApp.serialize(pagePath, currentPage);
								Bucket b = new Bucket(values, currentPage.generatePath());
								shiftedRows.add(b);
								inserted = true;
								break;
							}
							DBApp.serialize(pagePath, nextPage);
						}
					}
				}
			}
		}
		// no place for this element OR primary key is greater then the maximum in the
		// last page
		if (inserted == false) {
			String pagePath = PagesRef.get(pagesCount - 1);
			Page lastPage = (Page) DBApp.deserilize(pagePath);
			if (maxRowsPerPage != lastPage.tuples.size()) {
				// case5: primary key is greater then the maximum in the last page
				if (lastPage.keyFound(values.get(clusterKey))) {
					throw new DBAppException("This clustering key is already found!");
				}
				lastPage.insert(values);
				DBApp.serialize(pagePath, lastPage);
				Bucket b = new Bucket(values, lastPage.generatePath());
				shiftedRows.add(b);
				inserted = true;
			} else {
				// case6: no place for this element so create a new page
				Page newPage = new Page(tableName, clusterKey);
				newPage.key = this.clusterKey;
				lastPageId++;
				newPage.pageNumber = lastPageId;
				if (newPage.keyFound(values.get(clusterKey))) {
					throw new DBAppException("This clustering key is already found!");
				}
				newPage.insert(values);
				Bucket b = new Bucket(values, newPage.generatePath());
				shiftedRows.add(b);
				inserted = true;
				String path = generatePagePath(lastPageId);
				DBApp.serialize(path, newPage);
				PagesRef.add(path);
				pagesCount++;
			}
		}
	}

	public boolean keyFound(Object strClusteringKeyValue) throws DBAppException {
		for (int i = 0; i < pagesCount; i++) {
			String pagePath = PagesRef.get(i);
			Page currentPage = (Page) DBApp.deserilize(pagePath);
			if (currentPage.keyFound(strClusteringKeyValue)) {
				return true;
			}
		}
		return false;
	}

	public Vector<Hashtable<String, Object>> select(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException, IOException {
		Vector<Hashtable<String, Object>> result = new Vector<>();
		boolean useIndex = false;
		Vector<String> pagesPath = new Vector<String>();
		// 1. loop on the list of operators and check if all are AND
		if (isAllAnd(strarrOperators)) {
			// 2. if yes, get Indexed keys from metadata
			ArrayList<String> indexedColumns = getIndexedColumns();
			// 3. loop on each term and check if column is in available indexes
			String suitableIndex = getSuitableIndex(indexedColumns, arrSQLTerms);
			if (!suitableIndex.equals("null")) {
				useIndex = true;
				System.out.println(useIndex);
				// 6. deserialize the octree chosen
				String octreePath = generateOctreePath(suitableIndex);
				Octree octreeIndex = (Octree) DBApp.deserilize(octreePath);
				// 7. for each column get the sqlterm and convert it to min and max
				Object xMin = null, xMax = null, yMin = null, yMax = null, zMin = null, zMax = null;
				boolean isXminEqual = true, isXmaxEqual = true, isYminEqual = true, isYmaxEqual = true,
						isZminEqual = true, isZmaxEqual = true;
				for (SQLTerm s : arrSQLTerms) {
					if (s._strColumnName.equals(octreeIndex.columnName[0])) {
						xMin = convertSQLTermToRange(s, octreeIndex.root.xMin, octreeIndex.root.xMax).get(0);
						xMax = convertSQLTermToRange(s, octreeIndex.root.xMin, octreeIndex.root.xMax).get(1);
						isXminEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.xMin, octreeIndex.root.xMax).get(2);
						isXmaxEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.xMin, octreeIndex.root.xMax).get(3);
					}
					if (s._strColumnName.equals(octreeIndex.columnName[1])) {
						yMin = convertSQLTermToRange(s, octreeIndex.root.yMin, octreeIndex.root.yMax).get(0);
						yMax = convertSQLTermToRange(s, octreeIndex.root.yMin, octreeIndex.root.yMax).get(1);
						isYminEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.yMin, octreeIndex.root.yMax).get(2);
						isYmaxEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.yMin, octreeIndex.root.yMax).get(3);
					}
					if (s._strColumnName.equals(octreeIndex.columnName[2])) {
						zMin = convertSQLTermToRange(s, octreeIndex.root.zMin, octreeIndex.root.zMax).get(0);
						zMax = convertSQLTermToRange(s, octreeIndex.root.zMin, octreeIndex.root.zMax).get(1);
						isZminEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.zMin, octreeIndex.root.zMax).get(2);
						isZmaxEqual = (boolean) convertSQLTermToRange(s, octreeIndex.root.zMin, octreeIndex.root.zMax).get(3);
					}
				}
				pagesPath = octreeIndex.search(xMin, yMin, zMin, xMax, yMax, zMax, isXminEqual, isXmaxEqual, isYminEqual, isYmaxEqual, isZminEqual, isZmaxEqual);
			}
		}
		if (!useIndex) {
			System.out.println("index not used");
			pagesPath = PagesRef;
		}
		// loop on the returned paths and evaluate select on each tuple
		for (String page : pagesPath) {
			Page currentPage = (Page) DBApp.deserilize(page);
			result.addAll(currentPage.select(arrSQLTerms, strarrOperators));
		}
		return result;
	}

	public boolean isAllAnd(String[] strarrOperators) {
		for (String s : strarrOperators) {
			if (!s.equals("AND"))
				return false;
		}
		return true;
	}

	public ArrayList<String> getIndexedColumns() throws IOException {
		ArrayList<String> result = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("./src/data/metaData.csv"));
		String line;
		while ((line = br.readLine()) != null) {
			String[] lineArray = line.split(",");
			if (lineArray[0].equals(tableName) && !lineArray[4].equals("null")) {
				result.add(lineArray[1]);
			}
		}
		br.close();
		return result;
	}

	public String getSuitableIndex(ArrayList<String> indexedColumns, SQLTerm[] arrSQLTerms) {
		Hashtable<String, Integer> termsInIndex = new Hashtable<String, Integer>();
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(tableName);
		Vector<String> namesOfIndex = new Vector<String>();
		for (String[] s : metadata) {
			if (!namesOfIndex.contains(s[4])) {
				namesOfIndex.add(s[4]);
			}
		}
		for (String s : namesOfIndex) {
			int counter = 0;
			for (SQLTerm term : arrSQLTerms) {
				if (s.contains(term._strColumnName)) {
					counter++;
				}
				if (counter == 3) {
					return s;
				}
			}
		}
		return "null";
	}

	public ArrayList<Object> convertSQLTermToRange(SQLTerm sqlTerm, Object metaMin, Object metaMax)
			throws DBAppException {
		ArrayList<Object> result = new ArrayList<>();
		switch (sqlTerm._strOperator) {
		case ">":
			result.add(sqlTerm._objValue);
			result.add(metaMax);
			result.add(false);
			result.add(true);
			break; // min is sqlvalue and max in meta
		case ">=":
			result.add(sqlTerm._objValue);
			result.add(metaMax);
			result.add(true);
			result.add(true);
			break; // min is sqlvalue and max is meta
		case "<":
			result.add(metaMin);
			result.add(sqlTerm._objValue);
			result.add(true);
			result.add(false);
			break; // min is meta and max in sqlvalue
		case "<=":
			result.add(metaMin);
			result.add(sqlTerm._objValue);
			result.add(true);
			result.add(true);
			break; // min is meta and max is sqlvalue
		case "=":
			result.add(sqlTerm._objValue);
			result.add(sqlTerm._objValue);
			result.add(true);
			result.add(true);
			break; // min and max in sqlvalue
		case "!=":
			result.add(sqlTerm._objValue);
			result.add(sqlTerm._objValue);
			result.add(false);
			result.add(false);
			break; // ????
		default:
			throw new DBAppException("wrong passed sql term");
		}
		return result;
	}

	public void update(Object strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		Object minX = null;
		Object minY = null;
		Object minZ = null;
		Object maxX = null;
		Object maxY = null;
		Object maxZ = null;
		String fofa = "";

		int found = foundKey(strClusteringKeyValue, this.indicesRef);
		Vector<Hashtable<String, Object>> oldAndNew = new Vector<Hashtable<String, Object>>();
		if (found == -1) {// fadel delete from All octrees fe el case deh bas
			for (int i = 0; i < pagesCount; i++) {
				String path = PagesRef.get(i);
				Page currentPage = (Page) DBApp.deserilize(path);
				if (((Comparable) currentPage.maxValue).compareTo((Comparable) strClusteringKeyValue) >= 0) {
					oldAndNew = currentPage.update(htblColNameValue, strClusteringKeyValue);
					DBApp.serialize(path, currentPage);
					Hashtable<String, Object> oldValue = oldAndNew.get(0);
					Hashtable<String, Object> newValue = oldAndNew.get(1);
					for (int j = 0; j < indicesRef.size(); j++) {
						Octree oct = (Octree) DBApp.deserilize(indicesRef.get(j));
						oct.update(oldValue, newValue, fofa);
						DBApp.serialize(indicesRef.get(j), oct);
					}
					break;
				}
			}
		} else {
			Octree temp = (Octree) DBApp.deserilize(indicesRef.get(found));
			if (this.clusterKey.equals(temp.columnName[0])) {
				minX = strClusteringKeyValue;
				maxX = strClusteringKeyValue;
			} else {
				minX = temp.root.xMin;
				maxX = temp.root.xMax;
			}
			if (this.clusterKey.equals(temp.columnName[1])) {
				minY = strClusteringKeyValue;
				maxY = strClusteringKeyValue;
			} else {
				minY = temp.root.yMin;
				maxY = temp.root.yMax;
			}

			if (this.clusterKey.equals(temp.columnName[2])) {
				minZ = strClusteringKeyValue;
				maxZ = strClusteringKeyValue;
			} else {
				minZ = temp.root.zMin;
				maxZ = temp.root.zMax;
			}
			Vector<String> pagesUpdate = temp.search(minX, minY, minZ, maxX, maxY, maxZ, true, true, true, true, true, true);
			for (int n = 0; n < pagesUpdate.size(); n++) {
				Page num = (Page) DBApp.deserilize(pagesUpdate.get(n));
				oldAndNew = num.update(htblColNameValue, strClusteringKeyValue);
				DBApp.serialize(pagesUpdate.get(n), num);

			}
			Hashtable<String, Object> oldValue = oldAndNew.get(0);
			Hashtable<String, Object> newValue = oldAndNew.get(1);
			for (int j = 0; j < indicesRef.size(); j++) {
				Octree oct = (Octree) DBApp.deserilize(indicesRef.get(j));
				oct.update(oldValue, newValue, fofa);
				DBApp.serialize(indicesRef.get(j), oct);
			}
		}
	}

	public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		Object strClusteringKeyValue = htblColNameValue.get(clusterKey);
		Object minX = null;
		Object minY = null;
		Object minZ = null;
		Object maxX = null;
		Object maxY = null;
		Object maxZ = null;
		String fofa = "";

		int found = indexFound(htblColNameValue, this.indicesRef);
		if (found == -1) {
			for (int i = 0; i < pagesCount; i++) { // deleting without index
				String path = PagesRef.get(i);
				Page currentPage = (Page) DBApp.deserilize(path);
				Vector<Hashtable<String, Object>> rowsDelete = currentPage.delete(htblColNameValue);
				for (int j = 0; j < indicesRef.size(); j++) { // Deleting row from index
					Octree oct = (Octree) DBApp.deserilize(indicesRef.get(j));
					for (int o = 0; o < rowsDelete.size(); o++) {
						oct.delete(rowsDelete.get(o), fofa);
					}
					DBApp.serialize(indicesRef.get(j), oct);
				}
				if (currentPage.tuples.isEmpty()) {
					File f = new File(PagesRef.get(i));
					System.out.println(f.delete());
					PagesRef.remove(i);
					pagesCount--;
				} else {
					DBApp.serialize(path, currentPage);
				}
			}
		} else { // deleting row using index
			Octree temp = (Octree) DBApp.deserilize(indicesRef.get(found));

			if (htblColNameValue.containsKey(temp.columnName[0])) {
				minX = htblColNameValue.get(temp.columnName[0]);
				maxX = htblColNameValue.get(temp.columnName[0]);
			} else {
				minX = temp.root.xMin;
				maxX = temp.root.xMax;
			}
			if (htblColNameValue.containsKey(temp.columnName[1])) {
				minY = htblColNameValue.get(temp.columnName[1]);
				maxY = htblColNameValue.get(temp.columnName[1]);
			} else {
				minY = temp.root.yMin;
				maxY = temp.root.yMax;
			}

			if (htblColNameValue.containsKey(temp.columnName[2])) {
				minZ = htblColNameValue.get(temp.columnName[2]);
				maxZ = htblColNameValue.get(temp.columnName[2]);
			} else {
				minZ = temp.root.zMin;
				maxZ = temp.root.zMax;
			}

			Vector<String> pagesDelete = temp.search(minX, minY, minZ, maxX, maxY, maxZ, true, true, true, true, true, true);
			for (int n = 0; n < pagesDelete.size(); n++) {
				Page num = (Page) DBApp.deserilize(pagesDelete.get(n));
				Vector<Hashtable<String, Object>> rowsDelete = num.delete(htblColNameValue);
				for (int j = 0; j < indicesRef.size(); j++) { // Deleting row from index
					Octree oct = (Octree) DBApp.deserilize(indicesRef.get(j));
					for (int o = 0; o < rowsDelete.size(); o++) {
						oct.delete(rowsDelete.get(o), fofa);
					}
					DBApp.serialize(indicesRef.get(j), oct);
				}
				if (num.tuples.isEmpty()) {
					File f = new File(PagesRef.get(n));
					System.out.println(f.delete());
					PagesRef.remove(num.generatePath());
					pagesCount--;
				} else {
					DBApp.serialize(pagesDelete.get(n), num);
				}
			}
		}
	}

	public int indexFound(Hashtable<String, Object> htbl, Vector<String> ind) throws DBAppException {																				
		int count = 0;
		int max = 0;
		int index = -1;
		int j;
		for (j = 0; j < indicesRef.size(); j++) {
			Octree temp = (Octree) DBApp.deserilize(indicesRef.get(j));
			for (int i = 0; i < temp.columnName.length; i++) {
				if (htbl.containsKey(temp.columnName[i])) {
					count++;
				}
			}
			if (count > max) {
				max = count;
				index = j;
			}
			count = 0;

			DBApp.serialize(indicesRef.get(j), temp);
		}
		return index;
	}

	public int foundKey(Object key, Vector<String> ind) throws DBAppException {
		int index = -1;
		int j;
		for (j = 0; j < indicesRef.size(); j++) {
			Octree temp = (Octree) DBApp.deserilize(indicesRef.get(j));
			for (int i = 0; i < temp.columnName.length; i++) {
				if (key == temp.columnName[i]) {
					index = j;
				}
			}
			DBApp.serialize(indicesRef.get(j), temp);
		}
		return index;
	}

}