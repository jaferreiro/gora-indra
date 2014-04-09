package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.util.Utf8;
import org.apache.gora.cascading.tap.AbstractGoraScheme;
import org.apache.gora.persistency.impl.PersistentBase;
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
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class GoraLocalScheme extends AbstractGoraScheme<Properties,  // Config
                                       ResultBase,       // Input
                                       DataStore,        // Output
                                       Object[],         // SourceContext
                                       Object[],         // SinkContext
                                       GoraLocalTap>     // Tap class
{
    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalScheme.class);
                                           
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
        this(sourceFields, /*sink fields*/ Fields.ALL, numSinkParts) ;
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
        this(sourceFields, false, sinkFields, false, 0) ;
    }
    
    /**
     * Constructor with source and sink fields that enables sourcing/sinking the Persistent instance
     * as tuples with format ("key","persistent").
     * 
     * @see GoraLocalScheme#setTupleKeyName(String)
     * @see GoraLocalScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceFields
     * @param sourceAsPersistent if true, with source ("key","persistent")
     * @param sinkFields
     * @param sinkAsPersistent if true, will sink ("key","persistent")
     */
    public GoraLocalScheme(Fields sourceFields, boolean sourceAsPersistent, Fields sinkFields, boolean sinkAsPersistent) {
        this(sourceFields, sourceAsPersistent, sinkFields, sinkAsPersistent, 0) ;
    }

    /**
     * Constructor with source and sink fields that enables sourcing/sinking the Persistent instance
     * as tuples with format ("key","persistent").
     * 
     * @see GoraLocalScheme#setTupleKeyName(String)
     * @see GoraLocalScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceFields
     * @param sourceAsPersistent if true, with source ("key","persistent")
     * @param sinkFields
     * @param sinkAsPersistent if true, will sink ("key","persistent")
     */
    public GoraLocalScheme(Fields sourceFields, boolean sourceAsPersistent, Fields sinkFields, boolean sinkAsPersistent, int numSinkParts) {
        super(sourceFields, sourceAsPersistent, sinkFields, sinkAsPersistent, numSinkParts) ;
    }
    
    @Override
    protected String[] getPersistentFields(FlowProcess<Properties> flowProcess, Tap tap) throws GoraException {
        return ((PersistentBase) ((GoraLocalTap)tap).getDataStore(flowProcess.getConfigCopy()).newPersistent()).getFields() ;
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
            LOG.error("Error reading next input tuple", e) ;
            throw new IOException(e) ;
        }
        
        TupleEntry tupleEntry = sourceCall.getIncomingEntry() ;
        PersistentBase persistent = (PersistentBase) sourceCall.getInput().get() ;

        // Set key field
        tupleEntry.setString(this.getTupleKeyName(), (String) sourceCall.getInput().getKey()) ;
        
        if (this.isSourceAsPersistent()) {
            tupleEntry.setObject(this.getTuplePersistentFieldName(), persistent) ;
        } else {
            // scheme has to be ("key", "field", "field", ...)
            for (String fieldName: this.getSourceGoraFields()) {
                setTupleFieldFromPersistent(persistent, tupleEntry, fieldName) ;
            }
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
     * be created, populated with sinkFields values present in the TupleEntry and persisted.
     * 
     */
    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], DataStore> sinkCall) throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry() ;

        LOG.debug("(Tuple to sink) ", tupleEntry.toString()) ;

        String key = tupleEntry.getString(this.getTupleKeyName()) ;

        if (this.isSinkAsPersistent()) {
            // tuple ("key","persistent")
            PersistentBase persistent = (PersistentBase) tupleEntry.getObject(this.getTuplePersistentFieldName()) ;

            persistent.clearDirty() ;
            // Set dirty the sink fields of the Persistent instance
            for (String fieldName : this.getSinkGoraFields()) {
                // TODO: Maybe log warn when a field does not exist? (but slower)
                persistent.setDirty(fieldName) ;
            }
            sinkCall.getOutput().put(key, persistent) ;
        } else {

            // tuple (key, field, field,...)
            PersistentBase newOutputPersistent = (PersistentBase) sinkCall.getOutput().newPersistent() ;
    
            // Copy each field of the tuple to the Persistent instance
            for (String fieldName : this.getSinkGoraFields()) {
                try {
                    int indexInPersistent = newOutputPersistent.getFieldIndex(fieldName) ;
                    Object value = tupleEntry.getObject(fieldName) ;
                    
                    // convert String -> Utf8
                    if (value instanceof String){
                        value = new Utf8((String)value) ;
                    }
                    if (value instanceof Integer) {
                        value = ((Integer) value).longValue() ;
                    }
                    newOutputPersistent.put(indexInPersistent, value) ;
                } catch(NullPointerException e) {
                    LOG.warn("Field <{}> not found in sink class {}", fieldName, newOutputPersistent.getClass().getName()) ;
                }
                
            }
            sinkCall.getOutput().put(key, newOutputPersistent) ;
        }
    }

    /**
     * Flush all pending Puts. 
     */
    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<Object[], DataStore> sinkCall) throws IOException {
        super.sinkCleanup(flowProcess, sinkCall);
        sinkCall.getOutput().flush() ;
    }

    
}
