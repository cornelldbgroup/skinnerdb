package tools;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
/**
 * Configuration tools:
 * Read configuration parameters from the local file.
 *
 * @author Ziyun Wei
 */
public class Configuration {

    private final static String configFile = "config.properties";
    private final static Properties props = new Properties();

    static {
        try {
            FileReader reader = new FileReader(configFile);
            props.load(reader);
            reader.close();
        } catch (FileNotFoundException ex) {
            // file does not exist
            System.err.println("Configuration File Not Found!");
            System.exit(0);
        } catch (IOException ex) {
            // I/O error
            System.err.println("Configuration File Reading Error!");
            System.exit(0);
        }
    }

    public static String getProperty(String key) {
        if(props.containsKey(key)) {
            return props.getProperty(key);
        } else {
//            System.err.println("Warning: Not found the corresponding value for key " + key
//                    +" in the configuration file! Try with the default.");
            return null;
        }
    }

    public static String getProperty(String key, String defaultVal) {
        return props.containsKey(key) ? getProperty(key) : defaultVal;
    }

    public static void setProperty(String key, String value) {
        if(value != null) props.setProperty(key, value);
    }
}