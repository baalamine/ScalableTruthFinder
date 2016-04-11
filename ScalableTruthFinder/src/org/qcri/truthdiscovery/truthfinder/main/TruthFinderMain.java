/**
 * 
 */
package org.qcri.truthdiscovery.truthfinder.main;


import java.util.ArrayList;
import java.util.List;
import org.qcri.rheemx.java.data.JavaDataSet;
import org.qcri.rheemx.java.data.JavaDataSets;
import org.qcri.rheemx.spark.data.SparkDataSet;
import org.qcri.rheemx.spark.data.SparkDataSets;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderCompute;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderLoop;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderMerger;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderStaging;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderTransform;
import org.qcri.truthdiscovery.truthfinder.logical.TruthFinderUpdate;

/**
 * @author mlba
 * @created date: december 10, 2015
 * @desc: Truth Finder main class
 */
public class TruthFinderMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		double similarityConstant = 0.5 ;
		double base_similarity = 0.5 ;
		double damping_factor = 0.1 ;
		double cosineThresold = 0.001 ; // cosine Thresold as stop criteria
		double startTrust = 0.8; //a priori source trustworthiness score
		double startConfidence = 1.0 ; //a priori value confidence score
		List<Double> params = new ArrayList<Double>() ;
		params.add(similarityConstant) ;
		params.add(base_similarity)  ;
		params.add(damping_factor) ;
	
		//String claim_dataset = "resources/data/Books_CSV/claims/claim1.txt" ; // filename for input dataset
		String claim_dataset = "resources/data/biography_10000.csv" ;
		System.out.println("Working with default given values for parameters") ;
		
		long start_time = System.currentTimeMillis() ;
		
		//JavaDataSet<?> dataset = new JavaDataSets().createBuilder(claim_dataset) ;
	    SparkDataSet<?, ?> dataset = new SparkDataSets().createBuilder(claim_dataset);
	    
	    /**
	     * Create a Truth Finder Rheem Plan
	     */
	   
	   TruthFinderRheemPlan tf_rmJob = new TruthFinderRheemPlan(dataset, params, startTrust, startConfidence) ;
	    
	    //Call Truth Finder Logical Operators
	    tf_rmJob.setTransformOp(new TruthFinderTransform()); //Transform operation
	    tf_rmJob.setLocalstagingOp(new TruthFinderStaging(params, startTrust, startConfidence)); //Staging operation
	    tf_rmJob.setComputeOp(new TruthFinderCompute()); //Compute operation
	    tf_rmJob.setUpdateOp(new TruthFinderUpdate()); //Update operation
	    tf_rmJob.setMerger( new TruthFinderMerger() ); //Merger operation
	    tf_rmJob.setLoopOp(new TruthFinderLoop(cosineThresold)); //Loop operation
	    
	    
	    //Execute Rheem Job
	    tf_rmJob.run(); 
	    
	    System.out.println();
	    System.out.println("Total time:" + (System.currentTimeMillis() - start_time));
		

	}

}
