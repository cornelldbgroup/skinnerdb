package diskio;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * Implements utility methods for loading
 * or storing data from/to disk.
 * 
 * @author immanueltrummer
 *
 */
public class DiskUtil {
	/**
	 * Loads an object from specified path on hard disk.
	 * 
	 * @param path	path on hard disk
	 * @return		object loaded from disk
	 * @throws Exception
	 */
	public static Object loadObject(String path) throws Exception {
		// Read generic object from file
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream objIn = new ObjectInputStream(fileIn);
			Object object = objIn.readObject();
			objIn.close();
			fileIn.close();
			return object;
		} catch (Exception e) {
			throw new Exception("Error loading object at path '" + path + "'");
		}
	}
}
