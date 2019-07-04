package data;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.TreeSet;

/**
 * Maps string values to codes (and back).
 * 
 * @author immanueltrummer
 *
 */
public class Dictionary implements Serializable {
	/**
	 * Stores strings in order of code values.
	 */
	public final String[] strings;
	/**
	 * Number of strings in dictionary.
	 */
	public final int nrStrings;
	/**
	 * Initializes dictionary, given sorted strings.
	 * 
	 * @param stringSet	sorted strings to encode
	 */
	public Dictionary(TreeSet<String> stringSet) {
		// Create space for storing strings
		nrStrings = stringSet.size();
		strings = new String[nrStrings];
		// Assign codes
		int code = 0;
		for (String string : stringSet) {
			strings[code] = string;
			++code;
		}
	}
	/**
	 * Returns code value for given string or
	 * a value below zero if key cannot be found.
	 * 
	 * @param string	string to search
	 * @return			string code or -1
	 */
	public int getCode(String string) {
		return Arrays.binarySearch(strings, string);
	}
	/**
	 * Return string for given code value.
	 * 
	 * @param code	searching string for this code
	 * @return		string associated with code value
	 */
	public String getString(int code) {
		return strings[code];
	}
	/**
	 * Stores dictionary at given path.
	 * 
	 * @param path	path to store dictionary at
	 * @throws Exception
	 */
	public void store(String path) throws Exception {
		Files.createDirectories(Paths.get(path).getParent());
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(this);
		objOut.close();
		fileOut.close();
	}
}
