package engine;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import exceptions.DBAppException;

public class Octree<T> implements Serializable {
	private static final long serialVersionUID = 2746483527410304245L;
	public OctreeNode root;
	public String strTableName;
	public String[] columnName;
	public Object key;

	public Octree(String strTableName, String[] columnName) throws IOException, DBAppException {
		ArrayList<String[]> metadata = DBApp.getTableFromMetadata(strTableName);
		boolean keyFound = false;
		Object xMin = null, xMax = null, yMin = null, yMax = null, zMin = null, zMax = null;
		for (int i = 0; i < columnName.length; i++) {
			for (int j = 0; j < metadata.size(); j++) {
				if (metadata.get(j)[1].equals(columnName[i])) {
					if (i == 0) {
						xMin = parseType(metadata.get(j)[6], metadata.get(j)[2]);
						xMax = parseType(metadata.get(j)[7], metadata.get(j)[2]);
					}
					if (i == 1) {
						yMin = parseType(metadata.get(j)[6], metadata.get(j)[2]);
						yMax = parseType(metadata.get(j)[7], metadata.get(j)[2]);
					}
					if (i == 2) {
						zMin = parseType(metadata.get(j)[6], metadata.get(j)[2]);
						zMax = parseType(metadata.get(j)[7], metadata.get(j)[2]);
					}
				}
				if (!keyFound) {
					if (metadata.get(j)[3].equalsIgnoreCase("true")) {
						key = metadata.get(j)[1];
						keyFound = true;
					}
				}
			}
		}
		this.columnName = columnName;
		root = new OctreeNode(xMin, xMax, yMin, yMax, zMin, zMax);
	}

	public void insert(Hashtable<String, Object> htbl, String path) throws IOException {
		Point p = extract(htbl, path);
		OctreeNode targetNode = searchNode(p);
		targetNode.insert(p);
	}

	public OctreeNode searchNode(Point p) {
		OctreeNode current = this.root;
		while (true) {
			if (current.isLeaf) {
				return current;
			} else {
				int childIndex = getChildIndex(current, p);
				current = current.children[childIndex];
			}
		}
	}

	public OctreeNode searchParentNode(Point p) {
		OctreeNode current = this.root;
		OctreeNode parent = null;
		while (true) {
			if (current.isLeaf) {
				return parent;
			} else {
				int childIndex = getChildIndex(current, p);
				parent = current;
				current = current.children[childIndex];
			}
		}
	}

	public void delete(Hashtable<String, Object> htbl, String path) {
		Point p = extract(htbl, path);
		OctreeNode targetNode = searchNode(p);
		OctreeNode ParentNode = searchParentNode(p);
		boolean merge = true;
		Vector<Point> Newdata = new Vector<Point>();
		int x = targetNode.data.size();
		for (int i = 0; i < targetNode.data.size(); i++) {
			if (p.x.equals(targetNode.data.get(i).x) && p.y.equals(targetNode.data.get(i).y)
					&& p.z.equals(targetNode.data.get(i).z) /* && p.key==targetNode.data.get(i).key */) {
				targetNode.data.remove(i);
				i--;
			}
		}
		// check to merge or not go down level
		if (ParentNode != null) {
			for (int i = 0; i < 8; i++) {
				if (!ParentNode.children[i].isLeaf) {
					merge = false;
					break;
				} else {
					for (int j = 0; j < ParentNode.children[i].data.size(); j++) {
						Newdata.add(ParentNode.children[i].data.get(j));
					}
				}
			}
			// go down level
			int temp = targetNode.maxNumberOfRows;
			System.out.println("foaaaaa");
			if (Newdata.size() <= temp) {
				ParentNode.isLeaf = true;
				ParentNode.data = Newdata;
			}
		}
	}

	public OctreeNode searchDelete(Point p) {
		OctreeNode current = this.root;
		while (true) {
			if (current.isLeaf) {
				return current;
			} else {
				int childIndex = getChildIndex(current, p);
				current = current.children[childIndex];
			}
		}
	}

	public void update(Hashtable<String, Object> htbl, Hashtable<String, Object> newHtbl, String path) throws IOException {
		Point p = extract(htbl, path);
		OctreeNode targetNode = searchNode(p);
		OctreeNode ParentNode = searchParentNode(p);
		Vector<Point> newDate = new Vector<Point>();
		for (int i = 0; i < targetNode.data.size(); i++) {
			if (p.x.equals(targetNode.data.get(i).x) && p.y.equals(targetNode.data.get(i).y) && p.z.equals(targetNode.data.get(i).z)) {
				targetNode.data.remove(i);
			}
		}
		if (ParentNode != null) {
			for (int i = 0; i < 8; i++) {
				if (!ParentNode.children[i].isLeaf) {
					break;
				} else {
					for (int j = 0; j < ParentNode.children[i].data.size(); j++) {
						newDate.add(ParentNode.children[i].data.get(j)); // add all of the data to check that it's less than maxNumber of rows
					}
				}
			}
			// go down level
			int temp = targetNode.maxNumberOfRows - 1;
			if (newDate.size() < temp) {
				ParentNode.isLeaf = true;
				ParentNode.data = newDate;
			}
		}
		insert(newHtbl, path);
	}

	public int getChildIndex(OctreeNode current, Point point) {
		int index = 0;
		Object xMid = getMiddle(current.xMin, current.xMax);
		Object yMid = getMiddle(current.yMin, current.yMax);
		Object zMid = getMiddle(current.zMin, current.zMax);
		if (((Comparable) point.x).compareTo((Comparable) xMid) > 0)
			index |= 1;
		if (((Comparable) point.y).compareTo((Comparable) yMid) > 0)
			index |= 2;
		if (((Comparable) point.z).compareTo((Comparable) zMid) > 0)
			index |= 4;
		return index;
	}

	public Object getMiddle(Object min, Object max) {
		Object mid = null;
		if (min instanceof Integer) {
			mid = ((Integer) min + (Integer) max) / 2;
		}
		if (min instanceof Double) {
			mid = ((Double) min + (Double) max) / 2;
		}
		if (min instanceof String) {
			for (int i = 0; i < (((String) max).length()); i++) {
				if (((String) min).length() == ((String) max).length()) {
					break;
				} else {
					min = ((String) min).toLowerCase() + "z";
				}

			}
			mid = MiddleString((String) min, (String) max, Math.min(((String) min).length(), ((String) max).length()));
		}
		if (min instanceof Date) {
			System.out.println("4");
			mid = MiddleDate((Date) min, (Date) max);
		}
		return mid;
	}

	public static Date MiddleDate(Date first, Date second) {
		long f = first.getTime();
		long s = second.getTime();
		long diff = (f + s) / 2;
		return new Date(diff);
	}

	public static String MiddleString(String S, String T, int N) {
		int diff = Math.abs(S.length() - T.length());
		if (S.length() > T.length()) {
			T = T.concat(S.substring(diff));
		} else if (S.length() < T.length()) {
			S = S.concat(T.substring(diff));
		}
		S = S.toLowerCase();
		T = T.toLowerCase();
		String res = "";
		// N=10;
		// Stores the base 26 digits after addition
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}

		// Iterate from right to left
		// and add carry to next position
		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}
		// Reduce the number to find the middle
		// string by dividing each position by 2
		for (int i = 0; i <= N; i++) {

			// If current value is odd,
			// carry 26 to the next index value
			if ((a1[i] & 1) != 0) {

				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}
			a1[i] = (int) a1[i] / 2;
		}
		for (int i = 1; i <= N; i++) {
			res = res + ((char) (a1[i] + 97));
		}
		res = res.toLowerCase();
		return res;
	}

	public static int compareTo(Object first, Object second) throws DBAppException {
		int returnValue = 0;
		if (first instanceof Integer) {
			second = parseType((String) second, "java.lang.Integer");
			returnValue = ((Integer) first).compareTo((Integer) second);
		} else if (first instanceof String) {
			returnValue = ((String) first).toLowerCase().compareTo(((String) second).toLowerCase());
		} else if (first instanceof Double) {
			second = parseType((String) second, "java.lang.Double");
			returnValue = ((Double) first).compareTo((Double) second);
		} else if (first instanceof Date) {
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
			throw new DBAppException("Cannot parse value to passed type");
		}
	}

	public Point extract(Hashtable<String, Object> htbl, String path) {
		Object x, y, z;
		if (htbl.get(columnName[0]) instanceof String) {
			x = ((String) htbl.get(columnName[0])).toLowerCase();
		} else {
			x = htbl.get(columnName[0]);
		}
		if (htbl.get(columnName[1]) instanceof String) {
			y = ((String) htbl.get(columnName[1])).toLowerCase();
		} else {
			y = htbl.get(columnName[1]);
		}
		if (htbl.get(columnName[2]) instanceof String) {
			z = ((String) htbl.get(columnName[2])).toLowerCase();
		} else {
			z = htbl.get(columnName[2]);
		}
		// Object y = htbl.get(columnName[1]);
		// Object z = htbl.get(columnName[2]);
		Point p = new Point(x, y, z, key);
		p.paths.add(path);
		return p;
	}

	public Vector<String> search(Object xMin, Object yMin, Object zMin, Object xMax, Object yMax, Object zMax, boolean isXminEqual, boolean isXmaxEqual, boolean isYminEqual, boolean isYmaxEqual, boolean isZminEqual, boolean isZmaxEqual) throws DBAppException {
		Vector<String> result = new Vector<String>();
		Vector<Point> pointsInRange = root.search(xMin, yMin, zMin, xMax, yMax, zMax, isXminEqual, isXmaxEqual, isYminEqual, isYmaxEqual, isZminEqual, isZmaxEqual);
		for (Point p : pointsInRange) {
			for (int i = 0; i < p.paths.size(); i++) {
				if (!result.contains(p.paths.get(i))) {
					result.add(p.paths.get(i));
				}
			}
		}
		return result;
	}

	public void printTree(OctreeNode root, int childNumber) {
		if (root == null) {
			return;
		}
		System.out.println("child number" + childNumber);
		if (root.isLeaf) {
			System.out.println("leaf Node: (" + root.xMin + ", " + root.yMin + ", " + root.zMin + ") to (" + root.xMax + ", " + root.yMax + ", " + root.zMax + ")");
			for (Point dataPoint : root.data) {
				System.out.println("DataPoint: (" + dataPoint.x + ", " + dataPoint.y + ", " + dataPoint.z + ")");
			}
		} else {
			System.out.println("Non leaf Node: (" + root.xMin + ", " + root.yMin + ", " + root.zMin + ") to (" + root.xMax + ", " + root.yMax + ", " + root.zMax + ")");
			for (int i = 0; i < 8; i++) {
				printTree(root.children[i], i);
			}
		}
	}

}
