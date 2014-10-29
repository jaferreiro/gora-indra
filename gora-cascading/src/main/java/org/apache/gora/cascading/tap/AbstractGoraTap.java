package org.apache.gora.cascading.tap;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.gora.cascading.util.ConfigurationUtil;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

@SuppressWarnings("rawtypes")
public abstract class AbstractGoraTap<CONFIG, INPUT, OUTPUT, SCHEME extends Scheme> extends Tap<CONFIG, INPUT, OUTPUT> {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractGoraTap.class);

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;

    private Class<?> keyClass;
    private Class<? extends Persistent> persistentClass;
    
    private transient DataStore<?, ? extends Persistent> dataStore ;
    
    @SuppressWarnings("unchecked")
    public AbstractGoraTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, SCHEME scheme, SinkMode sinkMode, Configuration configuration) {
        super(scheme, sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
    }
    
    /**
     * Retrieves the datastore
     * @param conf (Optional) Needed the first time the datastore is retrieved for this scheme.
     *             In subsequent calls is ignored since the datastore is taken from cache.
     * @return
     * @throws GoraException
     */
    public DataStore<?, ? extends Persistent> getDataStore(CONFIG cascadingConf) throws GoraException {
        if (this.dataStore == null) {
            Object configuration = cascadingConf ;
            if (cascadingConf instanceof Properties) {
                configuration = ConfigurationUtil.toConfiguration((Properties)cascadingConf) ;
            }
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, (Configuration)configuration) ;
        }
        return this.dataStore ;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public SCHEME getScheme()
    {
        return (SCHEME) super.getScheme() ;
    }

    @Override
    public String getIdentifier() {
        // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
        return this.tapId ;
    }

    @Override
    public boolean createResource(CONFIG conf) throws IOException {
        this.getDataStore(conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(CONFIG conf) throws IOException {
        this.getDataStore(conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(CONFIG conf) throws IOException {
        return this.getDataStore(conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(CONFIG conf) throws IOException {
        return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

    @Override
    public void sourceConfInit(FlowProcess<CONFIG> flowProcess, CONFIG conf) {
        LOG.debug("sourcing from k-v: {}-{}", this.keyClass.getName(), this.persistentClass.getName()) ;
        super.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<CONFIG> flowProcess, CONFIG conf) {
        LOG.debug("sinking to k-v: {}-{}", this.keyClass.getName(), this.persistentClass.getName()) ;
        super.sinkConfInit(flowProcess, conf);
    }

    
    
}
