/**
 * 
 */
package qcri.truthdiscovery.fullyscalable.truthfinder;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.qcri.rheem.core.data.DataSet;
import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.engine.CacheDataAgent;
import org.qcri.rheem.core.engine.DataAgent;
import org.qcri.rheem.core.engine.LoopDataAgent;
import org.qcri.rheem.core.engine.RheemJob;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;
import org.qcri.rheem.core.util.MapOptions;
import org.qcri.rheem.ml4all.logical.Compute;
import org.qcri.rheem.ml4all.logical.ComputeWrapper;
import org.qcri.rheem.ml4all.logical.LocalStaging;
import org.qcri.rheem.ml4all.logical.LocalStagingWrapper;
import org.qcri.rheem.ml4all.logical.Loop;
import org.qcri.rheem.ml4all.logical.LoopWrapper;
import org.qcri.rheem.ml4all.logical.Transform;
import org.qcri.rheem.ml4all.logical.TransformWrapper;
import org.qcri.rheem.ml4all.logical.Update;
import org.qcri.rheem.ml4all.logical.UpdateWrapper;

import qcri.dafna.dataModel.data.SourceClaim;
import qcri.dafna.dataModel.data.ValueBucket;
import qcri.truthdiscovery.scalable.truthfinder.logical.ScalableTruthFinderMerger;

/**
 * @author mlba
 * @created date: Jan 24, 2016
 * Scalable Truth Finder Rheem Plan
 * 
 * @Input: Dataset containing the claims, algorithm's initial parameters, 
 * and the set of logical operators that compose the execution plan 
 * @Output: 
 */
public class FullyScalableTruthFinderRheemPlan extends RheemJob implements Serializable {

	protected Transform<?, ?> transformOp;
	protected LocalStaging stagingOp;
	protected Compute<?, ?> computeOp;
	protected Update<?, ?> updateOp;
	protected Loop<?> loopOp;
	

	private ArrayList<Double> params ;
	private double startTrust ;
	private double startConfidence;
	
	protected ScalableTruthFinderMerger merger;
	
	/**
	 * @constructor
	 * @param dataset
	 */
	protected FullyScalableTruthFinderRheemPlan(DataSet<?, ?, ?> dataset)
	{
		super(dataset);
		// TODO Auto-generated constructor stub
	}
	public FullyScalableTruthFinderRheemPlan()
	{
		super(null) ;
	}
	
	/**
	 * 
	 * @param dataset
	 * @param params
	 * @param startTrust
	 * @param startConfidence
	 */
	public FullyScalableTruthFinderRheemPlan(DataSet<?,?,?> dataset, ArrayList<Double> params, double startTrust, double startConfidence)
	{
		super(dataset) ;
		this.setParams(params) ;
		this.setStartTrust(startTrust)         ;
		this.setStartConfidence(startConfidence)    ;
	}
	public void setMerger(ScalableTruthFinderMerger merger) { this.merger = merger; }
	long start_time ;
	@Override
	public void run()
	{
		// TODO Auto-generated method stub
		start_time = System.currentTimeMillis();
		
		RheemContext<?> context = dataSet.getDataSets().createContext();
		context.setGlobalVariableMerger(merger);
		
		try 
		{
			DataAgent<?> transformAgent = CacheDataAgent.createAgent(dataSet, context)
			        .map(new TransformWrapper(transformOp), MapOptions.PAIR_OUT)
			        .reduceByKey( new ReduceByKeyWrapper() )
			        .contextUpdate(new LocalStagingWrapper(stagingOp))
			        .evaluate();
			
			DataSet<?,?,?> transformedDataset = transformAgent.getDataSet() ;
		
			
			LoopDataAgent<?> finalAgent = LoopDataAgent.createAgent(transformedDataset, context, new LoopWrapper(loopOp))
	            	   .map(new ComputeWrapper(computeOp), MapOptions.PAIR_OUT)
	            	   .reduce(new ReduceByKeyWrapper1())
	                   .map(new UpdateWrapper(updateOp))
	                   .evaluate();
			
			System.out.println("Final time:" + (System.currentTimeMillis() - start_time));
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * 
	 * @author mlba
	 * Group claims by data item key
	 * for input data bucketing 
	 */
	static class ReduceByKeyWrapper extends LogicalOperatorWrapper<KeyValuePair<Integer, List<SourceClaim>>>
	{
		private static final long serialVersionUID = -83096869601642532L;

		@SuppressWarnings("unchecked")
		@Override
		public KeyValuePair<Integer, List<SourceClaim>> apply(RheemContext context, Object... args) 
		{
			// TODO Auto-generated method stub
			//System.out.println ("Reduce Op") ;
			List<SourceClaim> merge = new ArrayList<SourceClaim>() ;
			KeyValuePair<?, ?> kv1 = (KeyValuePair<?, ?>) args[0] ;
			KeyValuePair<?, ?> kv2 = (KeyValuePair<?, ?>) args[1] ;
			//System.out.println(kv1.key) ;
			//System.out.println(kv2.key) ;
			//System.out.println (kv1.value.getClass().getName()) ;
			//System.out.println (kv2.value.getClass().getName()) ;
			int sum = (int)kv1.key  +  (int)kv2.key ;
			merge.addAll( (Collection<? extends SourceClaim>) (kv1.value )) ;
			merge.addAll( (Collection<? extends SourceClaim>) (kv2.value )) ;
			
			//System.out.println (sum) ;
			return new KeyValuePair<>(sum, merge) ;
		}
		
	}
	//raises an exception, need to be revised and corrected
	static class ReduceByKeyWrapper1 extends LogicalOperatorWrapper<KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -7181035084095905908L;

		@SuppressWarnings("unchecked")
		@Override
		public KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>> apply(RheemContext context, Object... args) 
		{
			// TODO Auto-generated method stub
			System.out.println ("Reduce Transform") ;
			KeyValuePair<?,?> kv1 = (KeyValuePair<?, ?>) args[0] ;
			KeyValuePair<?,?> kv2 = (KeyValuePair<?, ?>) args[1] ;
			//System.out.println(kv1.key) ;
			//System.out.println(kv2.key) ;
			//System.out.println (kv1.value.getClass().getName()) ;
			//System.out.println (kv2.value.getClass().getName()) ;
			List<ValueBucket> merge = new ArrayList<ValueBucket> () ;
			int sum = (int)((KeyValuePair<?,?>)kv1.value).key  +  (int)((KeyValuePair<?,?>)kv2.value).key ;
			merge.addAll( (Collection<? extends ValueBucket>) ((KeyValuePair<?,?>)kv1.value).value ) ;
			merge.addAll( (Collection<? extends ValueBucket>) ((KeyValuePair<?,?>)kv2.value).value ) ;
	
			//System.out.println(sum) ;
			return new KeyValuePair<>(1, new KeyValuePair<>(sum, merge)) ;
		}
		
	}
	public Transform<?, ?> getTransformOp() {
		return transformOp;
	}
	public void setTransformOp(Transform<?, ?> transformOp) {
		this.transformOp = transformOp;
	}
	public LocalStaging getStagingOp() {
		return stagingOp;
	}
	public void setStagingOp(LocalStaging stagingOp) {
		this.stagingOp = stagingOp;
	}
	public Compute<?, ?> getComputeOp() {
		return computeOp;
	}
	public void setComputeOp(Compute<?, ?> computeOp) {
		this.computeOp = computeOp;
	}
	public Update<?, ?> getUpdateOp() {
		return updateOp;
	}
	public void setUpdateOp(Update<?, ?> updateOp) {
		this.updateOp = updateOp;
	}
	public Loop<?> getLoopOp() {
		return loopOp;
	}
	public void setLoopOp(Loop<?> loopOp) {
		this.loopOp = loopOp;
	}
	
	public double getStartTrust() {
		return startTrust;
	}
	public void setStartTrust(double startTrust) {
		this.startTrust = startTrust;
	}
	public double getStartConfidence() {
		return startConfidence;
	}
	public void setStartConfidence(double startConfidence) {
		this.startConfidence = startConfidence;
	}
	/**
	 * @return the params
	 */
	public ArrayList<Double> getParams() {
		return params;
	}
	/**
	 * @param params the params to set
	 */
	public void setParams(ArrayList<Double> params) {
		this.params = params;
	}

}
