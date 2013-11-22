package org.apache.gora.cascading.tap;

import org.apache.gora.persistency.impl.PersistentBase;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatValueCopier;

public class GoraDeprecatedInputFormatValueCopier implements DeprecatedInputFormatValueCopier<PersistentBase> {

    @Override
    public void copyValue(PersistentBase oldValue, PersistentBase newValue) {
        oldValue.copyFrom(newValue) ;
    }

}
