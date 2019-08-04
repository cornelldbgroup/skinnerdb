package diskio;

import data.*;
import types.JavaType;

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

	public static ColumnData loadObject(String path, JavaType javaType) throws Exception {
		ColumnData columnData;
		// Read generic object from file
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream objIn = new ObjectInputStream(fileIn);
			Object object = objIn.readObject();
			// Cast object according to column type
			switch (javaType) {
				case INT:
					columnData = (IntData)object;
					break;
				case LONG:
					columnData = (LongData)object;
					break;
				case DOUBLE:
					columnData = (DoubleData)object;
					break;
				case STRING:
					columnData = (StringData)object;
					break;
				default:
					throw new IllegalStateException("Unexpected value: " + javaType);
			}
			objIn.close();
			fileIn.close();
			return columnData;
		} catch (Exception e) {
			throw new Exception("Error loading object at path '" + path + "'");
		}
	}
}
