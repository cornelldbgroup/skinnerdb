package tools;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
/**
 * Configuration tools:
 * Read configuration parameters from the local file.
 *
 * @author Anonymous
 */
public class Configuration {
    /**
     * The location of configuration file.
     */
    private static String configFile = "config.properties";
    /**
     * Creates an empty property list with no default values.
     */
    private final static Properties props = new Properties();
    /**
     * Initialize the location of configuration file
     * and load parameters and values to property list.
     *
     * @param dbDir     main database directory
     */
    public static void initConfiguration(String dbDir) throws IOException {
        Configuration.configFile = dbDir + "/config.sdb";
        FileReader reader = new FileReader(configFile);
        props.load(reader);
        reader.close();
    }
    /**
     * Given the name of parameter,
     * return the value from the configuration file.
     *
     * @param key   name of parameter
     * @return      the value of given parameter
     */
    public static String getProperty(String key) {
        if(props.containsKey(key)) {
            return props.getProperty(key);
        } else {
//            System.err.println("Warning: Not found the corresponding value for key " + key
//                    +" in the configuration file! Try with the default.");
            return null;
        }
    }
    /**
     * Given the name of parameter,
     * return the value from the configuration file.
     * If the parameter is not in the file, return the
     * default value
     *
     * @param key           name of parameter
     * @param defaultVal    default value of parameter
     * @return      the value of given parameter
     */
    public static String getProperty(String key, String defaultVal) {
        return props.containsKey(key) ? getProperty(key) : defaultVal;
    }

    /**
     *
     * @param key
     * @param value
     */
    public static void setProperty(String key, String value) {
        if(value != null)
            props.setProperty(key, value);
    }
}