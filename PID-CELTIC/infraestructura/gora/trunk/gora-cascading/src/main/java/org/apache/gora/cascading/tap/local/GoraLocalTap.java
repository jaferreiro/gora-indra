package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraLocalTap extends Tap<Properties, ResultBase, DataStore> {

    /** Field LOG */
    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalTap.class);

    // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
    private final String tapId = UUID.randomUUID().toString() ;

    private Class<?> keyClass;
    private Class<? extends Persistent> persistentClass;

    // TODO transient?
    private DataStore<?, ? extends Persistent> dataStore ;
    
    // HBase/HDFS configuration
    private JobConf jobConfiguration ;
    
    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP) ;
    }

    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, Configuration configuration) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP, configuration) ;
    }

    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, SinkMode sinkMode, Configuration configuration) {
        super(scheme, sinkMode) ;
        this.keyClass = keyClass ;
        this.persistentClass = persistentClass ;
        this.jobConfiguration = new JobConf(configuration) ;
    }

    public GoraLocalTap (Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, SinkMode sinkMode) {
        this(keyClass, persistentClass, scheme, sinkMode, new Configuration()) ;
    }
    
    /**
     * Retrieves the datastore
     * @param _conf Ignored properties configuration, since the configuration needed for the dataStore is taken from gora.properties and from *-site.xml(Hadoop)
     * @return
     * @throws GoraException
     */
    public DataStore<?, ? extends Persistent> getDataStore(Properties _conf) throws GoraException {
        if (this.dataStore == null) {
            this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, this.jobConfiguration) ;
        }
        return this.dataStore ;
    }
    
    @Override
    public GoraLocalScheme getScheme()
    {
        return (GoraLocalScheme) super.getScheme() ;
    }
    
    @Override
    public String getIdentifier() {
        // TODO Change this to something decent. Does this identifier affects optimizations of Taps usage?
        return this.tapId ;
    }

    @Override
    public boolean createResource(Properties _conf) throws IOException {
        this.getDataStore(_conf).createSchema() ;
        return true ; 
    }

    @Override
    public boolean deleteResource(Properties _conf) throws IOException {
        this.getDataStore(_conf).deleteSchema() ;
        return true ;
    }

    @Override
    public boolean resourceExists(Properties _conf) throws IOException {
        return this.getDataStore(_conf).schemaExists() ;
    }

    @Override
    public long getModifiedTime(Properties _conf) throws IOException {
        return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, ResultBase input) throws IOException {
        if (input != null) {
            return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), input) ;
        }
        Query query = this.getScheme().createQuery(this) ;
//        return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), new CloseableResult(query.execute())) ;
        return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), new CloseableResultIterator(query.execute())) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, DataStore output) throws IOException {
        // Devolver un TupleEntryCollector que se recibirá instancias TupleEntry/Tuple para ir grabando.
        // @param output puede ser null y habrá que crear una instancia de GoraRecordWriter

        return new TupleEntrySchemeCollector(flowProcess, this.getScheme(), this.getDataStore(null)) ;
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Properties propsConf) {
        LOG.debug("sourcing from k-v: {}-{}", this.keyClass.getName(), this.persistentClass.getName()) ;
        super.sourceConfInit(flowProcess, propsConf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Properties conf) {
        super.sinkConfInit(flowProcess, conf);
    }

}
