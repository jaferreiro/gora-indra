package org.apache.gora.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Test function: given an input tuple (argument selector of 1 field), it adds 65 to the value and
 * returns a tuple with 1 field with the 'outputSelector' name.
 *
 */
public class IncrementField extends BaseOperation<Tuple> implements Function<Tuple> {

    /**
     * @param outputSelector Field name for the returning value (1 tuple with 1 field)
     */
    public IncrementField(Fields outputSelector) {
        super(1, outputSelector);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call) {
        // create a reusable Tuple of size 1
        call.setContext(Tuple.size(1));
    }

    public void operate(FlowProcess flowProcess, FunctionCall<Tuple> call) {
        TupleEntry arguments = call.getArguments();
        Tuple result = call.getContext();

        int val = arguments.getInteger(0) + 65;
        result.set(0, val);

        call.getOutputCollector().add(result);
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<Tuple> call) {
        call.setContext(null);
    }
}
