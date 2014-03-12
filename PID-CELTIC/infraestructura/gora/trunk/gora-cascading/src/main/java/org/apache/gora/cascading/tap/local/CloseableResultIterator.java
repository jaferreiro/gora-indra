package org.apache.gora.cascading.tap.local;

import java.io.IOException;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Result;

import cascading.util.SingleValueCloseableIterator;

/**
 * Decorator that allows a Result to be closeable
 * @param <K>
 * @param <T>
 */
public class CloseableResultIterator<K, T extends Persistent> extends SingleValueCloseableIterator<Result<K,T>> {

    public CloseableResultIterator(Result<K,T> value) {
        super(value);
    }

    @Override
    public void close() throws IOException {
        this.getCloseableInput().close() ;
    }
    
}
