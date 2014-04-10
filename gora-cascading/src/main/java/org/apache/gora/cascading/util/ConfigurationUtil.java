package org.apache.gora.cascading.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * Class extracted from org.apache.pig.backend.hadoop.datastorage (copied partially)
 * @author indra
 *
 */
public class ConfigurationUtil {

    public static Configuration toConfiguration(Properties properties) {
        assert properties != null;
        final Configuration config = new Configuration(false);
        @SuppressWarnings("unchecked")
        final Enumeration<String> iter = (Enumeration<String>) properties.propertyNames();
        while (iter.hasMoreElements()) {
            final String key = (String) iter.nextElement();
            final String val = properties.getProperty(key);
            config.set(key, val);
        }
        return config;
    }

    public static Properties toProperties(Configuration configuration) {
        Properties properties = new Properties();
        assert configuration != null;
        Iterator<Map.Entry<String, String>> iter = configuration.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static Configuration toConfiguration(Map<Object,Object> map) {
        Configuration configuration = new Configuration(false) ;
        if (map != null) {
            for (Entry<Object,Object> entry : map.entrySet()) {
                configuration.set((String) entry.getKey(), String.valueOf(entry.getValue())) ;
            }
        }
        return configuration ;
    }
    

    public static Map<Object,Object> toRawMap(Configuration configuration) {
        Map<Object,Object> hashMap = new HashMap<Object,Object>(configuration.size());
        if (configuration != null) {
            Iterator<Entry<String,String>> configurationIterator = configuration.iterator() ;
            while (configurationIterator.hasNext()) {
                Entry<String,String> entry = configurationIterator.next() ;
                hashMap.put(entry.getKey(), entry.getValue()) ;
            }
        }
        return hashMap ;
    }
    
    /**
     * Merges configuration from the 'from' into the 'to'.
     * If a 'from' key exists in 'to', it is ignored.
     * If a 'from' key does not exists in 'to', it is added to 'to'.
     * 
     * Workaround used to merge Job configuration into JobConf
     * @param from
     * @param to
     */
    public static void mergeConfigurationFromTo(Configuration from, Configuration to) {
        for(Entry<String,String> fromEntry: from) {
            if (to.get(fromEntry.getKey()) == null) {
                to.set(fromEntry.getKey(), fromEntry.getValue()) ;
            }
        }
    }
    
}
