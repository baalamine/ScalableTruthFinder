/**
 * 
 */
package org.qcri.truthdiscovery.truthfinder.main;




import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.qcri.rheem.truthdiscovery.logical.*;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderMerger;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;


/**
 * @author mlba
 * @created date: december 10, 2015
 * @desc: Truth Finder Rheem Plan
 */
/**
 * @author mlba
 *
 */
public class TruthFinderRheemPlan extends RheemJob implements Serializable
{

	/**
	 * 
	 */
	protected Transform transformOp;
	protected Staging stagingOp ;
	protected LocalStaging localstagingOp;
	protected Compute computeOp;
	protected Update updateOp;
	protected Loop loopOp;
	
	
	private List<Double> params ;
	private double starTrust ;
	private double startConfidence;

	protected TruthFinderMerger merger;
	
	public TruthFinderRheemPlan(DataSet dataset) {
		super(dataset);
		// TODO Auto-generated constructor stub
	}
	
	public TruthFinderRheemPlan()
	{
		super(null) ;
	}

	public TruthFinderRheemPlan(DataSet dataset, List<Double> params, double startTrust, double startConfidence) {
		// TODO Auto-generated constructor stub
		super(dataset);
		this.setParams(params) ;
		this.setStarTrust(startTrust) ;
		this.setStartConfidence(startConfidence) ;
		
	}

	public void setMerger(TruthFinderMerger merger) { this.merger = merger; }
	
	public Transform<?, ?> getTransformOp() {
		return transformOp;
	}

	public void setTransformOp(Transform<?, ?> transformOp) {
		this.transformOp = transformOp;
	}

	public Staging getStagingOp() {
		return stagingOp;
	}

	public void setStagingOp(Staging stagingOp) {
		this.stagingOp = stagingOp;
	}

	public LocalStaging getLocalstagingOp() {
		return localstagingOp;
	}

	public void setLocalstagingOp(LocalStaging localstagingOp) {
		this.localstagingOp = localstagingOp;
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

	public void setLoopOp(Loop<?> truthFinderLoop) {
		this.loopOp = truthFinderLoop;
	}
	

	/**
	 * @return the starTrust
	 */
	public double getStarTrust() {
		return starTrust;
	}

	/**
	 * @param starTrust the starTrust to set
	 */
	public void setStarTrust(double starTrust) {
		this.starTrust = starTrust;
	}

	/**
	 * @return the startConfidence
	 */
	public double getStartConfidence() {
		return startConfidence;
	}

	/**
	 * @param startConfidence the startConfidence to set
	 */
	public void setStartConfidence(double startConfidence) {
		this.startConfidence = startConfidence;
	}

	long start_time ;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		start_time = System.currentTimeMillis();
		
		RheemContext<?> context = dataSet.getDataSets().createContext();
        context.setGlobalVariableMerger(merger);
       
  

        try {
            	DataAgent<?> transformAgent = CacheDataAgent.createAgent(dataSet, context)
                    .map(new TransformWrapper(transformOp), MapOptions.PAIR_OUT)
                    .reduceByKey(new ReduceByKeyWrapper())
                    .contextUpdate(new LocalStagingWrapper(localstagingOp))
                    .evaluate();

            
            	
            	DataSet<?,?,?> transformedDataset = transformAgent.getDataSet() ;
            	
            	LoopDataAgent<?> finalAgent = LoopDataAgent.createAgent(transformedDataset, context, new LoopWrapper(loopOp))
            	   .map(new ComputeWrapper(computeOp))
                   .map(new UpdateWrapper(updateOp))
                   .evaluate();
            	System.out.println("Final time:" + (System.currentTimeMillis() - start_time));
            	
            	
            	/*
            	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("source_spark.csv"))) ;
            	ArrayList<String> trustworthinessScores  = (ArrayList<String>) context.getByKey("trustworthiness") ;
            	for (String entry : trustworthinessScores)
            	{
            		String[] fields = entry.split("===>") ;
            		System.out.println(fields[0] + " ==> " + fields[1]) ;
            		bw.write(fields[0] + "," + fields[1] + "\n");
            	}
   				
            	bw.close(); 
            	*/
				
        } catch (Exception e) {
            e.printStackTrace();
        }
		
	}

	/**
	 * @return the params
	 */
	public List<Double> getParams() {
		return params;
	}

	/**
	 * @param params the params to set
	 */
	public void setParams(List<Double> params) {
		this.params = params;
	}
	static class ReduceByKeyWrapper extends LogicalOperatorWrapper<KeyValuePair<Integer, List<SourceClaim>>>
	{
		private static final long serialVersionUID = -83096869601642532L;

		@SuppressWarnings("unchecked")
		@Override
		public KeyValuePair<Integer, List<SourceClaim>> apply(RheemContext context, Object... args) 
		{
			// TODO Auto-generated method stub
			System.out.println ("Reduce Op") ;
			List<SourceClaim> merge = new ArrayList<SourceClaim>() ;
			KeyValuePair<?, ?> kv1 = (KeyValuePair<?, ?>) args[0] ;
			KeyValuePair<?, ?> kv2 = (KeyValuePair<?, ?>) args[1] ;
			System.out.println(kv1.key) ;
			System.out.println(kv2.key) ;
			System.out.println (kv1.value.getClass().getName()) ;
			System.out.println (kv2.value.getClass().getName()) ;
			int sum = (int)kv1.key  +  (int)kv2.key ;
			merge.addAll( (Collection<? extends SourceClaim>) (kv1.value )) ;
			merge.addAll( (Collection<? extends SourceClaim>) (kv2.value )) ;
			
			System.out.println (sum) ;
			return new KeyValuePair<>(sum, merge) ;
		}
		
	}
	
}
	
