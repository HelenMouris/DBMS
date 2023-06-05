package engine;

import java.io.Serializable;
import java.util.Hashtable;

public class Bucket implements Serializable {
	public Hashtable<String, Object> htbl;
	public String newPath;
	
	public Bucket(Hashtable<String, Object> htbl, String newPath) {
		this.htbl= htbl;
		this.newPath= newPath;
	}

}
