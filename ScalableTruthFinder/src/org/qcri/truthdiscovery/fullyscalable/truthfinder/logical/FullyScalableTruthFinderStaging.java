/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;

import java.util.ArrayList;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.LocalStaging;

/**
 * @author mlba
 * @created date: Jan 24, 2016
 * Scalable Truth Finder Staging 
 * 
 * @Input: values for algorithm's parameters
 * @Outout: initialize the values of the algorithm's parameters
 */
public class FullyScalableTruthFinderStaging extends LocalStaging
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ArrayList<Double> params ;
	double startTrust ;
	double startConfidence ;
	ArrayList<String> trustworthinessScores ;
	/**
	 * 
	 * @param params
	 * @param startTrust
	 * @param startConfidence
	 */
	public FullyScalableTruthFinderStaging(ArrayList<Double> params,
			double startTrust, double startConfidence) 
	{
		this.params = params ;
		this.startConfidence = startConfidence ;
		this.trustworthinessScores = new ArrayList<String>();
	}
	@Override
	public void staging(RheemContext context)
	{
		// TODO Auto-generated method stub
		 System.out.println("Staging Op");
		 
		 context.put("params", params) ;
	     context.put("startTrust", 0.8) ;
	     context.put("startConfidence", 1.0) ;
	     context.put("trustworthiness", this.trustworthinessScores) ;
		
	}

}
