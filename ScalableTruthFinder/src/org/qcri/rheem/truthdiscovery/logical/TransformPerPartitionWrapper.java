package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

import java.util.ArrayList;

/**
 * Created by zoi on 25/1/15.
 */
public class TransformPerPartitionWrapper<R> extends LogicalOperatorWrapper<Iterable> {

    Transform logOp;

    public TransformPerPartitionWrapper(Transform logOp) {
        this.logOp = logOp;
    }

    @Override
    public Iterable<R> apply(RheemContext context, Object... args) {
        Iterable it = (Iterable) args[0];
        long start = System.currentTimeMillis();
        ArrayList<R> list = new ArrayList<>();
        it.forEach((p) -> {
            R point = (R) this.logOp.transform(p, context);
            list.add(point);
        });
        System.out.println("Transform time:" + (System.currentTimeMillis() - start));
        return list;
    }

}
