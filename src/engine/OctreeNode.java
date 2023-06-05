package engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import exceptions.DBAppException;

public class OctreeNode implements Serializable {
	private static final long serialVersionUID = 1L;
	public Vector<Point> data;
	public int maxNumberOfRows=0;
	public Object xMin, xMax, yMin, yMax, zMin, zMax;
	public OctreeNode[] children;
	public boolean isLeaf = true;

	public OctreeNode(Object xMin, Object xMax, Object yMin, Object yMax, Object zMin, Object zMax) throws IOException {
		data = new Vector<Point>();
		readConfig();
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
		this.zMin = zMin;
		this.zMax = zMax;
	}

	public void insert(Point p) throws IOException {
		// Checks first for the values of x,y,z it they are already found, if found --> put the path in the vector of 
		// path , if not found --> continue our execution as before .
		for(int i=0;i<data.size();i++) {
			if(p.x.equals(data.get(i).x)&&p.y.equals(data.get(i).y)&& p.z.equals(data.get(i).z)) {
				if(!data.get(i).paths.contains(p.paths.get(0))) {
					data.get(i).paths.add(p.paths.get(0));
				}
				return;
			}
		}
		if (data.size() == maxNumberOfRows) {
			this.split();
			for (int i = 0; i < data.size(); i++) {
				int index = getChildIndex(data.get(i));
				children[index].insert(data.get(i));
			}
			int index = getChildIndex(p);
			children[index].insert(p);
			data.clear();
			isLeaf = false;
		} else {
			Point point = new Point(p.x,p.y,p.z,p.key);
			point.paths.add(p.paths.get(0));
			data.add(point);
		}
	}

	// This has to adapt with our four types: int , double, String, Date.
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int getChildIndex(Point point) {
		int index = 0;
		Object xMid = getMiddle(xMin, xMax);
		Object yMid = getMiddle(yMin, yMax);
		Object zMid = getMiddle(zMin, zMax);
		System.out.println(xMid);
		if (((Comparable) point.x).compareTo((Comparable) xMid) > 0)
			index |= 1;
		if (((Comparable) point.y).compareTo((Comparable) yMid) > 0)
			index |= 2;
		if (((Comparable) point.z).compareTo((Comparable) zMid) > 0)
			index |= 4;
		return index;
	}

	// TODO : getting the middle of two strings,integers, doubles, dates
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
			mid = MiddleDate((Date)min, (Date) max);
		}
		return mid;
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
	
	// this method gets the middle date by getting the median of the number of seconds of each date from the epoch 
	public static Date MiddleDate(Date first, Date second) {
		long f =first.getTime();
		long s =second.getTime();
		long diff = (f+s)/2;
		return new Date(diff);
	}

	// This has to adapt with our four types: int , double, String, Date.
	private void split() throws IOException {
		children = new OctreeNode[8];
		Object xMid = getMiddle(xMin, xMax);
		Object yMid = getMiddle(yMin, yMax);
		Object zMid = getMiddle(zMin, zMax);
		children[0] = new OctreeNode(xMin, xMid, yMin, yMid, zMin, zMid);
		children[1] = new OctreeNode(xMid, xMax, yMin, yMid, zMin, zMid);
		children[2] = new OctreeNode(xMin, xMid, yMid, yMax, zMin, zMid);
		children[3] = new OctreeNode(xMid, xMax, yMid, yMax, zMin, zMid);
		children[4] = new OctreeNode(xMin, xMid, yMin, yMid, zMid, zMax);
		children[5] = new OctreeNode(xMid, xMax, yMin, yMid, zMid, zMax);
		children[6] = new OctreeNode(xMin, xMid, yMid, yMax, zMid, zMax);
		children[7] = new OctreeNode(xMid, xMax, yMid, yMax, zMid, zMax);
	}

	public Vector<Point> search(Object xMin, Object yMin, Object zMin, Object xMax, Object yMax, Object zMax,
                             boolean isXminEqual, boolean isXmaxEqual, boolean isYminEqual, boolean isYmaxEqual,
                             boolean isZminEqual, boolean isZmaxEqual) throws DBAppException {
		Vector<Point> result = new Vector<Point>();
		if (!isLeaf) {
			for (OctreeNode child : children) {
				if (child.intersectsNonleaf(xMin, yMin, zMin, xMax, yMax, zMax, isXminEqual, isXmaxEqual,
						isYminEqual, isYmaxEqual, isZminEqual, isZmaxEqual)) {
					result.addAll(child.search(xMin, yMin, zMin, xMax, yMax, zMax, isXminEqual, isXmaxEqual,
							isYminEqual, isYmaxEqual, isZminEqual, isZmaxEqual));
				}
			}
		} else {
			for (Point d : data) {
				if (inRangeOfLeaf(d, xMin, yMin, zMin, xMax, yMax, zMax, isXminEqual, isXmaxEqual,
						isYminEqual, isYmaxEqual, isZminEqual, isZmaxEqual)) {
					result.add(d);
				}
			}
		}
		return result;
	}

	private boolean intersectsNonleaf(Object xMin, Object yMin, Object zMin, Object xMax, Object yMax, Object zMax,
									boolean isXminEqual, boolean isXmaxEqual, boolean isYminEqual, boolean isYmaxEqual,
									boolean isZminEqual, boolean isZmaxEqual) throws DBAppException {
		boolean intersects = true;
		if (isXminEqual) {
			intersects &= compareTo(xMax, this.xMin) >= 0;
		} else {
			intersects &= compareTo(xMax, this.xMin) > 0;
		}
		if (isYminEqual) {
			intersects &= compareTo(yMax, this.yMin) >= 0;
		} else {
			intersects &= compareTo(yMax, this.yMin) > 0;
		}
		if (isZminEqual) {
			intersects &= compareTo(zMax, this.zMin) >= 0;
		} else {
			intersects &= compareTo(zMax, this.zMin) > 0;
		}
		if (isXmaxEqual) {
			intersects &= compareTo(xMin, this.xMax) <= 0;
		} else {
			intersects &= compareTo(xMin, this.xMax) < 0;
		}
		if (isYmaxEqual) {
			intersects &= compareTo(yMin, this.yMax) <= 0;
		} else {
			intersects &= compareTo(yMin, this.yMax) < 0;
		}
		if (isZmaxEqual) {
			intersects &= compareTo(zMin, this.zMax) <= 0;
		} else {
			intersects &= compareTo(zMin, this.zMax) < 0;
		}
		return intersects;
	}

	private boolean inRangeOfLeaf(Point data, Object xMin, Object yMin, Object zMin, Object xMax, Object yMax, Object zMax,
									boolean isXminEqual, boolean isXmaxEqual, boolean isYminEqual, boolean isYmaxEqual,
									boolean isZminEqual, boolean isZmaxEqual) throws DBAppException {
		Object x = data.x;
		Object y = data.y;
		Object z = data.z;

		boolean inRange = true;
		if (isXminEqual) {
			inRange &= compareTo(x, xMin) >= 0;
		} else {
			inRange &= compareTo(x, xMin) > 0;
		}
		if (isYminEqual) {
			inRange &= compareTo(y, yMin) >= 0;
		} else {
			inRange &= compareTo(y, yMin) > 0;
		}
		if (isZminEqual) {
			inRange &= compareTo(z, zMin) >= 0;
		} else {
			inRange &= compareTo(z, zMin) > 0;
		}
		if (isXmaxEqual) {
			inRange &= compareTo(x, xMax) <= 0;
		} else {
			inRange &= compareTo(x, xMax) < 0;
		}
		if (isYmaxEqual) {
			inRange &= compareTo(y, yMax) <= 0;
		} else {
			inRange &= compareTo(y, yMax) < 0;
		}
		if (isZmaxEqual) {
			inRange &= compareTo(z, zMax) <= 0;
		} else {
			inRange &= compareTo(z, zMax) < 0;
		}
		return inRange;
	}

	public static int compareTo(Object first, Object second) throws DBAppException {
		int returnValue = 0;
		if (first instanceof Integer) {
			returnValue = ((Integer) first).compareTo((Integer) second);
		}
		else if (first instanceof String) {
			returnValue = ((String) first).toLowerCase().compareTo(((String) second).toLowerCase());
		}
		else if (first instanceof Double) {
			returnValue = ((Double) first).compareTo(((Double) second));
		}
		else if (first instanceof Date) {
			returnValue = ((Date) first).compareTo((Date) second);
		}
		return returnValue;
	}
	
	public void readConfig() throws IOException{
		String filepath = "./src/resources/DBApp.config";
		File f = new File("resources");
		f.mkdir();
		try {
			FileInputStream reader = new FileInputStream(filepath);
			Properties properties = new Properties();
			properties.load(reader);
			String maxString = properties.getProperty("MaximumEntriesinOctreeNode");
			maxNumberOfRows = Integer.parseInt(maxString);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.toString());
		}
	}
}
