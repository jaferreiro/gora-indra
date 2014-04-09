package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Properties;

import org.apache.gora.cascading.tap.AbstractGoraTap;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

@SuppressWarnings({ "rawtypes" })
public class GoraLocalTap extends AbstractGoraTap<Properties, ResultBase, DataStore, GoraLocalScheme> {

    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalTap.class);

    public GoraLocalTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP) ;
    }

    public GoraLocalTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, SinkMode sinkMode) {
        this(keyClass, persistentClass, scheme, sinkMode, new Configuration()) ;
    }
    
    public GoraLocalTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, Configuration configuration) {
        this(keyClass, persistentClass, scheme, SinkMode.KEEP, configuration) ;
    }

    public GoraLocalTap(Class<?> keyClass, Class<? extends Persistent> persistentClass, GoraLocalScheme scheme, SinkMode sinkMode, Configuration configuration) {
        super(keyClass, persistentClass, scheme, sinkMode, configuration) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, ResultBase input) throws IOException {
        if (input != null) {
            return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), input) ;
        }
        Query query = this.getScheme().createQuery(flowProcess, this) ;
        return new TupleEntrySchemeIterator(flowProcess, this.getScheme(), new CloseableResultIterator(query.execute())) ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, DataStore output) throws IOException {
        return new TupleEntrySchemeCollector(flowProcess, this.getScheme(), this.getDataStore(flowProcess.getConfigCopy())) ;
    }

}
