package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.collect.Iterables;

@SuppressWarnings("rawtypes")
public class GoraLocalScheme extends Scheme<Properties,  // Config
                                       ResultBase,       // Input
                                       DataStore,        // Output
                                       Object[],         // SourceContext
                                       Object[]>         // SinkContext
{
    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalScheme.class);
                                           
    Object queryStartKey ;
    Object queryEndKey ;
    Long queryLimit ;

    /** Special Fields for retrieving all the instance */
    public static final Fields keyAndPersistent = new Fields ("key", "persistent") ;
    
    private static final long serialVersionUID = 1L;

    public GoraLocalScheme() {
        this(/*source fields*/ Fields.ALL, /*sink fields*/ Fields.ALL, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceSinkFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraLocalScheme(Fields sourceSinkFields) {
        this(sourceSinkFields, sourceSinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load
     * @param sourceFields {@link Fields#ALL} | Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraLocalScheme(Fields sourceFields, int numSinkParts) {
        this(sourceFields, /*sink fields*/ null, numSinkParts) ;
    }

    /**
     * Constructor with the fields to load and fields to save
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     */
    public GoraLocalScheme(Fields sourceFields, Fields sinkFields) {
        this(sourceFields, sinkFields, /*numSinkParts*/ 0) ;
    }

    /**
     * Constructor with the fields to load, fields to save to and suggestion of number of sink parts
     * @param sourceFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sinkFields Strings with fields names from defined in .avsc model (only from 1st level).
     * @param sumSinkParts {@link Scheme}
     */
    public GoraLocalScheme(Fields sourceFields, Fields sinkFields, int numSinkParts) {
        super(sourceFields, sinkFields, numSinkParts);
        if (sourceFields.isAll()) {
            this.setSourceFields(GoraLocalScheme.keyAndPersistent) ;
        }
    }

    public Object getQueryStartKey() {
        return queryStartKey;
    }

    public void setQueryStartKey(Object queryStartKey) {
        this.queryStartKey = queryStartKey;
    }

    public Object getQueryEndKey() {
        return queryEndKey;
    }

    public void setQueryEndKey(Object queryEndKey) {
        this.queryEndKey = queryEndKey;
    }

    public Long getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(Long queryLimit) {
        this.queryLimit = queryLimit;
    }

    @SuppressWarnings("unchecked")
    public Query<?,? extends Persistent> createQuery(GoraLocalTap tap) throws GoraException {
        
        Query query = tap.getDataStore(null).newQuery() ;
        
        if (this.queryStartKey != null)
            query.setStartKey(this.queryStartKey) ;
        if (this.queryEndKey != null)
            query.setEndKey(this.queryEndKey) ;
        if (this.queryLimit != null)
            query.setLimit(this.queryLimit) ;
        
        if (this.getSourceFields().isDefined() && !this.getSourceFields().equalsFields(GoraLocalScheme.keyAndPersistent)) {
            String[] fields = (String[]) Iterables.toArray(this.getSourceFields(), Object.class) ;
            query.setFields(fields) ;
        }
        return query ;
    }
    
    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ResultBase, DataStore> tap, Properties propsConf) {
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ResultBase, DataStore> tap, Properties propsConf) {
    }
    
    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Object[], ResultBase> sourceCall) throws IOException {
        try {
            if (!sourceCall.getInput().next()) {
                return false ;
            }
        } catch (Exception e) {
            // TODO: spit to log
            throw new IOException(e) ;
        }
        
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();

        Fields fields = sourceCall.getIncomingEntry().getFields() ;
        if (fields.equalsFields(GoraLocalScheme.keyAndPersistent)) {
            tuple.clear();
            tuple.addString((String) sourceCall.getInput().getKey()) ;
            tuple.add(sourceCall.getInput().get()) ;
        }
        
        return true;
    }

    /**
     * Saves the tuple in sinbkCall.getOutgoingEntry()
     * 
     * The fields to save are indicated in the constructor as sinkFields, and can be Fields.ALL or
     * specific fields of the Persistent object. 
     * 
     * If the tuple received has fields: ("key", "persistent"), the sencond field must be of type 
     * PersistentBase. Will only save the fields set in sinkFields.
     * 
     * If the tuple has not exactly ("key","persistent") fields, a PersistentBase object will
     * be created, populated with sinkFields values and persisted.
     * 
     */
    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], DataStore> sinkCall) throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry() ;
        Fields fieldsInTuple = tupleEntry.getFields() ; // Received fields to save

        String key = tupleEntry.getString("key") ;

        // If tuple is (key, Persistent) as "key","persistent"
        if (fieldsInTuple.equalsFields(GoraLocalScheme.keyAndPersistent)) {

            PersistentBase persistent = (PersistentBase) tupleEntry.getObject("persistent") ;

            if (this.getSinkFields().equals(Fields.ALL)) {
                persistent.setDirty() ;
            } else {
                persistent.clearDirty() ;
                Iterator sinkFieldsIterator = this.getSinkFields().iterator() ;
                while (sinkFieldsIterator.hasNext()) {
                    String fieldName = (String) sinkFieldsIterator.next() ;
                    // TODO: Maybe log warn when a field does not exist? (but slower)
                    persistent.setDirty(fieldName) ;
                }
            }
            sinkCall.getOutput().put(key, persistent) ;

        } else {
            
            // tuple is (key, field, field,...)
            
            PersistentBase newOutputPersistent = (PersistentBase) sinkCall.getOutput().newPersistent() ;

            Iterator fieldsNameIterator = fieldsInTuple.iterator() ;
            while (fieldsNameIterator.hasNext()) {
                String fieldName = (String) fieldsNameIterator.next() ;
                // Copy into persistent
                // Automatically sets dirty bits in Persistent for the field
                try {
                    int fieldIndex = newOutputPersistent.getFieldIndex(fieldName) ;
                    newOutputPersistent.put(fieldIndex, tupleEntry.getObject(fieldName)) ;
                } catch (NullPointerException e) {
                    LOG.warn("Field <" + fieldName + "> not found in sink class " + newOutputPersistent.getClass().getName()) ;
                }
            }
            sinkCall.getOutput().put(key, newOutputPersistent) ;
            
        }

    }
    
}
