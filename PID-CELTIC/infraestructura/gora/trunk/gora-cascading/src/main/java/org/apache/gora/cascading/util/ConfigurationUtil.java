package org.apache.gora.cascading.util;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationUtil {

    /**
     * Declares de modes to convert Configuration to Properties, where the value can be
     * the raw configured, or interpreted variables in values.
     */
    public enum ToPropertiesMode { RAW, INTERPRETED } ;
    
    public static Configuration toConfiguration(Properties properties) {
        Configuration configuration = new Configuration(false) ;
        if (properties != null) {
            for (Entry<Object,Object> entry : properties.entrySet()) {
                configuration.set((String) entry.getKey(), String.valueOf(entry.getValue())) ;
            }
        }
        return configuration ;
    }

    public static Properties toProperties(Configuration configuration, ToPropertiesMode mode) {
        Properties properties = new Properties() ;
        if (configuration != null) {
            Iterator<Entry<String,String>> configurationIterator = configuration.iterator() ;
            while (configurationIterator.hasNext()) {
                Entry<String,String> entry = configurationIterator.next() ;
                switch (mode) {
                    case RAW :
                        properties.setProperty(entry.getKey(), configuration.getRaw(entry.getKey())) ;
                        break ;
                        
                    case INTERPRETED :
                        properties.setProperty(entry.getKey(), configuration.get(entry.getKey())) ;
                        break ;
                }
            }
        }
        return properties ;
    }
    
    /**
     * Creates a Properties with raw values (variables not substituted) form Configuration
     * @param configuration
     * @return
     */
    public static Properties toRawProperties(Configuration configuration) {
        return toProperties(configuration, ToPropertiesMode.RAW) ;
    }

    /**
     * Creates a Properties with values (variables not substituted) form Configuration
     * @param configuration
     * @return
     */
    public static Properties toInterpretedProperties(Configuration configuration) {
        return toProperties(configuration, ToPropertiesMode.INTERPRETED) ;
    }
    
}
