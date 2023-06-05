package engine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import exceptions.DBAppException;

public class Page implements Serializable {

	private static final long serialVersionUID = 2746483527410304245L;
	public Vector<Hashtable<String, Object>> tuples;
	public int pageNumber;
	public String tableName;
	public Object key;
	public Object minValue;
	public Object maxValue;

	public Page(String tableName, String clusterKey) {
		this.tableName = tableName;
		this.key = clusterKey;
		this.tuples = new Vector<Hashtable<String, Object>>();
	}

	public String generatePath() {
		return "./src/tables/" + tableName + "/" + pageNumber + ".class";
	}

	public String insert(Hashtable<String, Object> row) {
		if (tuples.size() == 0) {
			tuples.add(row);
		} else {
			int index = binarySearch(tuples, 0, this.tuples.size() - 1, row.get(key));
			tuples.add(index, row);
		}
		updateMinAndMax();
		return generatePath();
	}

	public void updateMinAndMax() {
		if (tuples.size() != 0) {
			minValue = tuples.get(0).get(key);
			maxValue = tuples.get(tuples.size() - 1).get(key);
		}
	}

	@SuppressWarnings("unchecked")
	public int binarySearch(Vector<Hashtable<String, Object>> page, int left, int right, Object strClusteringKeyValue) {
		int middle = left + (right - left) / 2;
		while (left <= right) {
			if (((Comparable) page.get(middle).get(key)).compareTo(strClusteringKeyValue) == 0) {
				return middle;
			} else if (((Comparable) page.get(middle).get(key)).compareTo(strClusteringKeyValue) > 0) {
				right = middle - 1;
			} else {
				left = middle + 1;
			}
			middle = left + (right - left) / 2;
		}
		if (left > right) {
			System.out.println("Not found");
		}
		return middle;
	}

	public boolean keyFound(Object strClusteringKeyValue) {
		for (int i = 0; i < tuples.size(); i++) {
			if (((Comparable) tuples.get(i).get(key)).compareTo((Comparable) strClusteringKeyValue) == 0) {
				return true;
			}
		}
		return false;
	}

	public Vector<Hashtable<String, Object>> update(Hashtable<String, Object> row, Object strClusteringKeyValue) {
		int index = binarySearch(tuples, 0, this.tuples.size() - 1, strClusteringKeyValue);
		Vector<Hashtable<String, Object>> oldAndNew = new Vector<Hashtable<String, Object>>();
		for (Map.Entry<String, Object> col : row.entrySet()) {
			Hashtable<String, Object> old = new Hashtable<String, Object>();
			old = (Hashtable<String, Object>) tuples.get(index).clone();
			oldAndNew.add(old);
			tuples.get(index).replace(col.getKey(), col.getValue());
			oldAndNew.add(tuples.get(index));
			System.out.println(oldAndNew);
		}
		return oldAndNew;

	}

	public Vector<Hashtable<String, Object>> delete(Hashtable<String, Object> row) {
		int x = tuples.size();
		Vector<Hashtable<String, Object>> res = new Vector<Hashtable<String, Object>>();
		int index = 0;
		for (int count = 0; count < x; count++) {
			Hashtable<String, Object> record = tuples.get(index);
			boolean flag = true;
			for (Map.Entry<String, Object> col : row.entrySet()) {
				if (!(((Comparable) record.get(col.getKey())).compareTo((Comparable) col.getValue()) == 0)) {
					flag = false;
					break;
				}
			}
			if (flag) {
				res.add(tuples.get(index));
				tuples.remove(index--);

			}
			index++;
		}
		updateMinAndMax();
		return res;
	}

	public Vector<Hashtable<String, Object>> select(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException {
		Vector<Hashtable<String, Object>> result = new Vector<>();
		for (Hashtable<String, Object> t : tuples) {
			if (evaluateSelect(t, arrSQLTerms, strarrOperators, 0))
				result.add(t);
		}
		return result;
	}

	private boolean evaluateSelect(Hashtable<String, Object> t, SQLTerm[] arrSQLTerms, String[] strarrOperators, int i)
			throws DBAppException {
		if (i == strarrOperators.length) {
			return checkRange(t, arrSQLTerms[i]);
		} else {
			switch (strarrOperators[i]) {
			case "AND":
				return checkRange(t, arrSQLTerms[i]) && evaluateSelect(t, arrSQLTerms, strarrOperators, i + 1);
			case "OR":
				return checkRange(t, arrSQLTerms[i]) || evaluateSelect(t, arrSQLTerms, strarrOperators, i + 1);
			case "XOR":
				return checkRange(t, arrSQLTerms[i]) ^ evaluateSelect(t, arrSQLTerms, strarrOperators, i + 1);
			default:
				throw new DBAppException("wrong passed sql operator");
			}
		}
	}

	public boolean checkRange(Hashtable<String, Object> t, SQLTerm sqlTerm) throws DBAppException {
		Comparable comparedValue = (Comparable) t.get(sqlTerm._strColumnName);
		Comparable termValue = (Comparable) sqlTerm._objValue;
		switch (sqlTerm._strOperator) {
		case ">":
			return (comparedValue.compareTo(termValue) > 0);
		case ">=":
			return (comparedValue.compareTo(termValue) >= 0);
		case "<":
			return (comparedValue.compareTo(termValue) < 0);
		case "<=":
			return (comparedValue.compareTo(termValue) <= 0);
		case "=":
			return (comparedValue.compareTo(termValue) == 0);
		case "!=":
			return (comparedValue.compareTo(termValue) != 0);
		default:
			throw new DBAppException("wrong passed sql term");
		}
	}

}