package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

import java.util.ArrayList;

/**
 * Created by zoi on 25/1/15.
 */
public class ComputePerPartitionLocalWrapper<V, R> extends LogicalOperatorWrapper {

    Compute logOp;

    R sumOfPartition;

    public ComputePerPartitionLocalWrapper(Compute logOp) {
        this.logOp = logOp;
    }

    @Override
    public R apply(RheemContext context, Object... args) {
        Iterable<V> it = (Iterable<V>) args[0];
        ArrayList<R> list = new ArrayList<>(1);
//        Iterator<V> i = it.iterator();
//        while (i.hasNext()) {
//            list.add((R) this.logOp.compute(i.next(), context));
//        }
        it.forEach((p) -> sumOfPartition = (R) this.logOp.compute(p, context));
        return sumOfPartition;
    }

    @Override
    public void initialise() { logOp.initialise(); }

}
