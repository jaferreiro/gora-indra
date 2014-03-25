package org.apache.gora.cascading.tap.local;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ArrayUtils;
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
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class GoraLocalScheme extends Scheme<Properties,  // Config
                                       ResultBase,       // Input
                                       DataStore,        // Output
                                       Object[],         // SourceContext
                                       Object[]>         // SinkContext
{
    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(GoraLocalScheme.class);
                                           
    Object queryStartKey ;
    Object queryEndKey ;
    Long queryLimit ;

    /** If true, source tuples will be formatted: ("key","persistent") */
    boolean sourceAsPersistent = false ;
    /** If true, sink tuples must have format: ("key","persistent") */
    boolean sinkAsPersistent = false ;
    
    /** Fields that will be read with Gora from datastore */
    String sourceGoraFields[] ;
    /** Fields to write with Gora to datastore */
    String sinkGoraFields[] ;
    
    /**Tuple field name for the key when sourcing/sinking */
    String  tupleKeyFieldName = "key" ;
    
    /**Tuple field name for the persistent when sourcing/sinkin "*AsPersistent" */
    String  tuplePersistentFieldName = "persistent" ;
    
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
        super(sourceFields, sinkFields, numSinkParts);
        this.sourceAsPersistent = sourceAsPersistent ;
        this.sinkAsPersistent = sinkAsPersistent ;
    }
    
    public Object getQueryStartKey() {
        return queryStartKey;
    }

    /** Sets the query start key. If <i>null</i> will query from the beginning of the table.
     * 
     * Recommended to set after creating a GoraLocalScheme instance.
     * 
     * @param queryStartKey
     */
    public void setQueryStartKey(Object queryStartKey) {
        this.queryStartKey = queryStartKey;
    }

    public Object getQueryEndKey() {
        return queryEndKey;
    }

    /** Sets the query end key. If <i>null</i> will query to the end of the table.
     * 
     * Recommended to set after creating a GoraLocalScheme instance.
     * 
     * @param queryStartKey
     */
    public void setQueryEndKey(Object queryEndKey) {
        this.queryEndKey = queryEndKey;
    }

    public Long getQueryLimit() {
        return queryLimit;
    }

    /** Sets the limit to the number of retrieved elements.
     * 
     * Recommended to set after creating a GoraLocalScheme instance.
     * 
     * @param queryStartKey
     */
    public void setQueryLimit(Long queryLimit) {
        this.queryLimit = queryLimit;
    }

    public boolean isSourceAsPersistent() {
        return sourceAsPersistent;
    }

    /**
     * Defines de format of the sources tuples:
     * 
     * If sourceAsPersistent==false, tuples are sourced as ("key", "field1", "field2", "field3",...), so fields declared as
     * source in the constructor will optimize the query and will set the fields of the tuple.
     * 
     * If sourceAspersistent==true, tuples are sources as ("key", "persistent") where the "persistent" field contains the
     * specific Persistent instance defined as datasource value.
     * The fields declared as source in the constructor will be only used to optimize the query.
     * 
     * @see GoraLocalScheme#setTupleKeyName(String)
     * @see GoraLocalScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceAsPersistent (default false)
     */
    public void setSourceAsPersistent(boolean sourceAsPersistent) {
        this.sourceAsPersistent = sourceAsPersistent;
    }

    public boolean isSinkAsPersistent() {
        return sinkAsPersistent;
    }


    /**
     * Defines de format of the sink tuples accepted:
     * 
     * If sinkAsPersistent==false, accepts tuples to sink with the format ("key", "field1", "field2", "field3",...).
     * If any tuple has more fields, this fields will be ignored and only saved the ones defined as fields in the constructor.
     * 
     * If sinkAspersistent==true, accepts tuples to sink with the format ("key", "persistent") where the "persistent" field contains the
     * specific Persistent instance defined as datasource value.
     * 
     * The only fields saved will be the ones defined in the constructor as "sink fields".
     * 
     * @see GoraLocalScheme#setTupleKeyName(String)
     * @see GoraLocalScheme#setTuplePersistentFieldName(String)
     * 
     * @param sourceAsPersistent (default false)
     */
    public void setSinkAsPersistent(boolean sinkAsPersistent) {
        this.sinkAsPersistent = sinkAsPersistent;
    }

    public String getTupleKeyName() {
        return tupleKeyFieldName;
    }

    /**
     * Sets the row key field name for source and sink tuples. 
     * @param tupleKeyName (default "key")
     */
    public void setTupleKeyName(String tupleKeyName) {
        this.tupleKeyFieldName = tupleKeyName;
    }
    
    public String getTuplePersistentFieldName() {
        return tuplePersistentFieldName;
    }

    /**
     * Sets the field name for source and sink tuples that will hold the Persistent (the inherited from PersistentBase) instance
     * @param tuplePersistentFieldName (default "persistent")
     */
    public void setTuplePersistentFieldName(String tuplePersistentFieldName) {
        this.tuplePersistentFieldName = tuplePersistentFieldName;
    }
    
    public String[] getSourceGoraFields() {
        return sourceGoraFields;
    }

    /**
     * Sets the source fields instead defining them in the constructor.
     * @param goraFields
     */
    public void setSourceGoraFields(String[] goraFields) {
        this.sourceGoraFields = goraFields;
    }

    public String[] getSinkGoraFields() {
        return sinkGoraFields;
    }

    /**
     * Sets the sink fields instead defining them in the constructor.
     * @param sinkGoraFields
     */
    public void setSinkGoraFields(String[] sinkGoraFields) {
        this.sinkGoraFields = sinkGoraFields;
    }

    @Override
    public Fields retrieveSourceFields(FlowProcess<Properties> flowProcess, Tap tap) {
        // Default gora fields = persistent fields
        try {
            this.setSourceGoraFields(((PersistentBase) ((GoraLocalTap)tap).getDataStore(null).newPersistent()).getFields()) ;
        } catch (GoraException e) {
            throw new RuntimeException(e) ;
        }

        Fields sourceFields = this.getSourceFields() ;

        // Source fields for Cascading
        Fields cascadingSourceFields = new Fields(this.getTupleKeyName()) ;
        if (this.isSourceAsPersistent()) { // (key, Persistent) 
            cascadingSourceFields = cascadingSourceFields.append(new Fields(this.getTuplePersistentFieldName())) ;
        } else {
            if (sourceFields.isAll() || sourceFields.isUnknown()) {
                // (key, persistent_field, persistent_field,...)
                cascadingSourceFields = cascadingSourceFields.append(new Fields(this.getSourceGoraFields())) ;
            } else {
                //(key, declared_in_constructor_field, declared_in_constructor_field,...)
                cascadingSourceFields = cascadingSourceFields.append(sourceFields) ;
            }
        }
        this.setSourceFields(cascadingSourceFields) ;
        
        // Gora fields (default above)
        if ( ! (sourceFields.isAll() || sourceFields.isUnknown())) {
            // Gora fields = sourceFields(constructor) AND gora fields
            // (intersection from Persistent and declared in constructor)
            String[] newGoraFields = (String[]) ArrayUtils.clone(this.getSourceGoraFields()) ;
            String[] deleteGoraFields = (String[]) ArrayUtils.clone(this.getSourceGoraFields()) ;
            Iterator fieldNameIterator = sourceFields.iterator() ;
            while (fieldNameIterator.hasNext()) {
                ArrayUtils.removeElement(deleteGoraFields,(String)fieldNameIterator.next()) ;
            }
            for (int i=0 ; i<deleteGoraFields.length ; i++) {
                ArrayUtils.removeElement(newGoraFields, deleteGoraFields[i]) ;
            }
            this.setSourceGoraFields(newGoraFields) ;
        }
        return this.getSourceFields() ; // == just set cascadingSourceFields 
    }

    @Override
    public Fields retrieveSinkFields(FlowProcess<Properties> flowProcess, Tap tap) {
        // Default gora fields = persistent fields
        try {
            this.setSinkGoraFields(((PersistentBase) ((GoraLocalTap)tap).getDataStore(null).newPersistent()).getFields()) ;
        } catch (GoraException e) {
            throw new RuntimeException(e) ;
        }

        Fields sinkFields = this.getSinkFields() ;

        // Sink fields for Cascading
        Fields cascadingSinkFields = new Fields(this.getTupleKeyName()) ;
        if (this.isSinkAsPersistent()) { // (key, Persistent) 
            cascadingSinkFields = cascadingSinkFields.append(new Fields(this.getTuplePersistentFieldName())) ;
        } else {
            if (sinkFields.isAll() || sinkFields.isUnknown()) {
                // (key, persistent_field, persistent_field,...)
                cascadingSinkFields = cascadingSinkFields.append(new Fields(this.getSinkGoraFields())) ;
            } else {
                //(key, declared_in_constructor_field, declared_in_constructor_field,...)
                cascadingSinkFields = cascadingSinkFields.append(sinkFields) ;
            }
        }
        this.setSinkFields(cascadingSinkFields) ;
        
        // Gora fields (default above)
        if ( ! (sinkFields.isAll() || sinkFields.isUnknown())) {
            // Gora fields = sinkFields(constructor) AND gora fields
            // (intersection from Persistent and declared in constructor)
            String[] newGoraFields = (String[]) ArrayUtils.clone(this.getSinkGoraFields()) ;
            String[] deleteGoraFields = (String[]) ArrayUtils.clone(this.getSinkGoraFields()) ;
            Iterator fieldNameIterator = sinkFields.iterator() ;
            while (fieldNameIterator.hasNext()) {
                ArrayUtils.removeElement(deleteGoraFields,(String)fieldNameIterator.next()) ;
            }
            for (int i=0 ; i<deleteGoraFields.length ; i++) {
                ArrayUtils.removeElement(newGoraFields, deleteGoraFields[i]) ;
            }
            this.setSinkGoraFields(newGoraFields) ;
        }
        return this.getSinkFields() ; // == just set cascadingSinkFields 

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

        // If in constructor source was ALL (or UNKNOWN), we retrieve all.

        if ( ! (this.getSourceFields().isAll() || this.getSourceFields().isUnknown())) {

            // otherwise, retrieve only the necessary
            if (this.getSourceGoraFields().length == 0 ) {
                this.retrieveSourceFields(null, tap) ;
            }
            query.setFields(this.getSourceGoraFields()) ;
            
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
            case STRING: tupleEntry.setString(tupleFieldName, ((Utf8)value).toString()) ;
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
                    newOutputPersistent.put(indexInPersistent, value) ;
                } catch(NullPointerException e) {
                    LOG.warn("Field <" + fieldName + "> not found in sink class " + newOutputPersistent.getClass().getName()) ;
                }
                
            }
            sinkCall.getOutput().put(key, newOutputPersistent) ;
        }
    }
    
}
