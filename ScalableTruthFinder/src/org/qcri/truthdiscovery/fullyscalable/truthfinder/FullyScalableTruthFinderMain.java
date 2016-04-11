/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder;

import java.util.ArrayList;

import org.qcri.rheemx.java.data.JavaDataSet;
import org.qcri.rheemx.java.data.JavaDataSets;
import org.qcri.rheemx.spark.data.SparkDataSet;
import org.qcri.rheemx.spark.data.SparkDataSets;
import org.qcri.truthdiscovery.scalable.truthfinder.logical.*;


/**
 * @author mlba
 * @created date: Jan 24, 2016
 * Scalable Truth Finder Main Program
 * 
 * @Input:
 * @Output:
 */
public class FullyScalableTruthFinderMain 
{

	public static void main (String args[])
	{
		/**
		 * Init default input parameters
		 */				
		double similarityConstant = 0.5 ;
		double base_similarity = 0.5 ;
		double damping_factor = 0.1 ;
		double  cosineThresold = 0.001 ; //cosine thresold representing the stop criteria
		double startTrust = 0.8; //a priori source trustworthiness score
		double startConfidence = 1.0 ; //a priori value confidence score
		
		ArrayList<Double> params = new ArrayList<Double> () ;
		params.add(similarityConstant) ; params.add(base_similarity) ; params.add(damping_factor) ; //params global variable
		//String claim_dataset = "resources/data/Books_CSV/claims/claim1.txt" ; // input dataset filename
		String biography_claim = "resources/data/biography_80000.csv" ; //biography dataset
	
		long start_time = System.currentTimeMillis() ;
		//JavaDataSet<?> dataset = new JavaDataSets().createBuilder(biography_claim) ;
		SparkDataSet<?,?> dataset = new SparkDataSets().createBuilder(biography_claim);
    
		/**
		 * Scalable Truth Finder Rheem Plan
		 */
   
		FullyScalableTruthFinderRheemPlan TF_RheemJob = new FullyScalableTruthFinderRheemPlan(dataset, params, startTrust, startConfidence) ;
    
		/**
		 * Instantiate Scalable Truth Finder Logical Operators
		 */
		TF_RheemJob.setTransformOp(new ScalableTruthFinderTransform()); //Transform operator
		TF_RheemJob.setStagingOp(new ScalableTruthFinderStaging(params, startTrust, startConfidence)); //Staging operation
		TF_RheemJob.setComputeOp(new ScalableTruthFinderCompute()); //Compute operation
		TF_RheemJob.setUpdateOp(new ScalableTruthFinderUpdate()); //Update operation
		TF_RheemJob.setMerger( new ScalableTruthFinderMerger() ); //Merger operation
		TF_RheemJob.setLoopOp(new ScalableTruthFinderLoop(cosineThresold)); //Loop operation
		
    
		/**
		 * Rheem Job Execution
		 */
		TF_RheemJob.run(); 
    
		System.out.println();
		System.out.println("Total time:" + (System.currentTimeMillis() - start_time));
	
	}
	
}
