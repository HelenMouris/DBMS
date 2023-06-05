package engine;

import java.io.Serializable;
import java.util.Vector;

public class Point implements Serializable {
	public Object x, y, z;
	public Object key;
	public Vector<String> paths; // Contains the path of pages of all rows that have these x,y,z values (duplicates)

	public Point(Object x, Object y, Object z, Object key) {
		this.x = x;
		this.y = y;
		this.z = z;
		this.key = key;
		this.paths = new Vector();
	}

}
