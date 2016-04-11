package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 25/1/15.
 */
public class StagingWrapper<R> extends LogicalOperatorWrapper {

    Staging logOp;

    public StagingWrapper(Staging logOp) {
        this.logOp = logOp;
    }

    @Override
    public R apply(RheemContext context, Object... args) {
        return (R) this.logOp.staging(args[0], context);
    }

}
