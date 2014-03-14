package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
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
import cascading.tuple.FieldsResolverException;
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
            LOG.error("Error reading next input tuple", e) ;
            throw new IOException(e) ;
        }
        
        TupleEntry tupleEntry = sourceCall.getIncomingEntry() ;
        Fields tupleFields = tupleEntry.getFields() ;
        PersistentBase persistent = (PersistentBase) sourceCall.getInput().get() ;

        tupleEntry.setString("key", (String) sourceCall.getInput().getKey()) ;
        
        // When the scheme is ("key", "persistent") the second field will be the Persistent instance 
        if (tupleFields.equalsFields(GoraLocalScheme.keyAndPersistent)) {
            tupleEntry.setObject("persistent", persistent) ;
        } else {
            // scheme = ("key", "field", "field", ...)
            Iterator sourceFields = this.getSourceFields().iterator() ;
            while (sourceFields.hasNext()) {
                String tupleFieldName = (String) sourceFields.next() ;
                if (!sourceFields.equals("key")) {
                    setTupleFieldFromPersistent(persistent, tupleEntry, tupleFieldName) ;
                }
            }

        }
        
        return true;
    }

    /**
     * Given a PersistentBase instance, takes the value from its field specified in "tupleFieldName"
     *  and saves it to the tuple with the same field name.
     *  
     * @param persistent
     * @param tupleEntry
     * @param tupleFieldName
     */
    private void setTupleFieldFromPersistent(PersistentBase persistent, TupleEntry tupleEntry, String tupleFieldName) {
        try {
            int persistentFieldIndex = persistent.getFieldIndex(tupleFieldName) ; // NPE if field name noes not exist in Persistent instance
            Object value = persistent.get(persistentFieldIndex) ;
            Schema avroSchema = persistent.getSchema().getField(tupleFieldName).schema() ; // avro schema
            this.setTupleFieldFromPersistent(avroSchema, value, tupleEntry, tupleFieldName) ;
        } catch(NullPointerException e) {
            LOG.warn("Field <" + tupleFieldName + "> not found in source class " + persistent.getClass().toString()) ;
        }
    }
        
    /**
     * Given a value with an avro schema, saves that value in the tuple field specified by tupleFieldName. 
     * @param persistent
     * @param value
     * @param tupleEntry
     * @param tupleFieldName
     */
    private void setTupleFieldFromPersistent(Schema avroSchema, Object value, TupleEntry tupleEntry, String tupleFieldName) {
        
        switch(avroSchema.getType()) {
            
            case UNION:
                // XXX Special case: When reading the top-level field of a record we must handle the
                // special case ["null","type"] definitions: this will be written as if it was ["type"]
                // if not in a special case, will execute "case RECORD".
                
                // if 'val' is empty we ignore the special case (will match Null in "case RECORD")  
                if (avroSchema.getTypes().size() == 2) {
                  
                  // schema [type0, type1]
                  Type type0 = avroSchema.getTypes().get(0).getType() ;
                  Type type1 = avroSchema.getTypes().get(1).getType() ;
                  
                  // Check if types are different and there's a "null", like ["null","type"] or ["type","null"]
                  if (!type0.equals(type1)
                      && (   type0.equals(Schema.Type.NULL)
                          || type1.equals(Schema.Type.NULL))) {

                    if (type0.equals(Schema.Type.NULL))
                        avroSchema = avroSchema.getTypes().get(1) ;
                    else 
                        avroSchema = avroSchema.getTypes().get(0) ;
                    this.setTupleFieldFromPersistentWithoutUnion(avroSchema, value, tupleEntry, tupleFieldName) ;
                  }
                }

                // else, save as object without information (duplicated sentence, but clearer)
                this.setTupleFieldFromPersistentWithoutUnion(avroSchema, value, tupleEntry, tupleFieldName) ;
                break ;
                
            default :
                this.setTupleFieldFromPersistentWithoutUnion(avroSchema, value, tupleEntry, tupleFieldName) ;
        }

    }
    
    /**
     * Copy a value given its avroSchema to the TypleEntry(tupleFieldName).
     * Unknown entries (NULL and UNION) as Object type.
     * 
     * @param avroSchema An Avro Schema of the field from the Persistent instance
     * @param value
     * @param tupleEntry
     * @param tupleFieldName
     */
    private void setTupleFieldFromPersistentWithoutUnion(Schema avroSchema, Object value, TupleEntry tupleEntry, String tupleFieldName) {
        switch (avroSchema.getType()) {
            case ARRAY:
            case RECORD:
            case MAP:
            case FIXED:
            case ENUM: 
            case BYTES: tupleEntry.setObject(tupleFieldName, value) ;
                break;
            case BOOLEAN: tupleEntry.setBoolean(tupleFieldName, (Boolean) value) ;
                break;
            case DOUBLE: tupleEntry.setDouble(tupleFieldName, (Double) value) ;
                break;
            case FLOAT: tupleEntry.setFloat(tupleFieldName, (Float) value) ;
                break;
            case INT: tupleEntry.setInteger(tupleFieldName, (Integer) value) ;
                break;
            case LONG: tupleEntry.setLong(tupleFieldName, (Long) value) ;
                break;
            case STRING: tupleEntry.setString(tupleFieldName, (String) value) ;
                break;
            case NULL: tupleEntry.setObject(tupleFieldName, value) ;
                break;
            case UNION: tupleEntry.setObject(tupleFieldName, value) ;
                break;
        }
        
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
        Fields fieldsInTuple = tupleEntry.getFields() ; // Received fields to save
        Fields sinkFields = this.getSinkFields() ;

        String key = tupleEntry.getString("key") ;

        // If tuple is (key, Persistent) as "key","persistent"
        if (fieldsInTuple.equalsFields(GoraLocalScheme.keyAndPersistent)) {

            PersistentBase persistent = (PersistentBase) tupleEntry.getObject("persistent") ;

            if (this.getSinkFields().equals(Fields.ALL)) {
                persistent.setDirty() ;
            } else {
                persistent.clearDirty() ;
                // Set dirty the sink fields of the Persistent instance
                Iterator sinkFieldsIterator = sinkFields.iterator() ;
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

            // Copy each field of the tuple to the Persistent instance
            Iterator fieldsInTupleNameIterator = fieldsInTuple.iterator() ;
            while (fieldsInTupleNameIterator.hasNext()) {
                String fieldName = (String) fieldsInTupleNameIterator.next() ;
                if (fieldName.equals("key")) continue ;

                // Check if the field of the tuple exist in the sinkFields declared in constructor
                try {
                    sinkFields.getPos(fieldName) ; // throw FieldsResolverException if not exist
                } catch (FieldsResolverException e) {
                    LOG.warn("Field <" + fieldName + "> not found in sink class " + newOutputPersistent.getClass().getName()) ;
                    continue ;
                }
                 
                // Copy into persistent
                // Automatically sets dirty bits in Persistent for the field
                try {
                    int indexInPersistent = newOutputPersistent.getFieldIndex(fieldName) ;
                    newOutputPersistent.put(indexInPersistent, tupleEntry.getObject(fieldName)) ;
                } catch(NullPointerException e) {
                    LOG.warn("Field <" + fieldName + "> not found in sink class " + newOutputPersistent.getClass().getName()) ;
                }
                
            }
            sinkCall.getOutput().put(key, newOutputPersistent) ;
            
        }

    }
    
}
