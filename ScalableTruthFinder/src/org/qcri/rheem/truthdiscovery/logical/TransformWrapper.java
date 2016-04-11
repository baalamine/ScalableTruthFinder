package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 25/1/15.
 */
public class TransformWrapper<R> extends LogicalOperatorWrapper {

    Transform logOp;

    public TransformWrapper(Transform logOp) {
        this.logOp = logOp;
    }

    @Override
    public R apply(RheemContext context, Object... args) {
        return (R) this.logOp.transform(args[0], context);
    }
}
