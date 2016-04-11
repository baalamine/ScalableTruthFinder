/**
 * 
 */
package org.qcri.cleaningdata.truthdiscovery.truthfinder.logical;


import java.util.ArrayList;
import java.util.List;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.ml4all.logical.LocalStaging;


/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Stage Logical Function
 */
public class TruthFinderStaging extends LocalStaging
{

	/**
	 * 
	 */
	
	List<Double> params = new ArrayList<Double>() ;
	double startTrust ; //a priori source trustworthiness score
	double startConfidence ; //a priori value confidence score;
	List<String> trustworthinessScores ;


	public TruthFinderStaging(List<Double> params, double startTrust, double startConfidence)
	{
		this.params = params ;
		this.startTrust = startTrust ;
		this.startConfidence = startConfidence ;
		this.trustworthinessScores = new ArrayList<String>() ;
		
	}
	@Override
	public void staging(RheemContext context) 
	{
		// TODO Auto-generated method stub
		//System.out.println("Staging Op") ;
        context.put("params", this.params) ;
        context.put("startTrust", this.startTrust) ;
        context.put("startConfidence", this.startConfidence) ;
        context.put("trustworthiness", this.trustworthinessScores) ;
        
     
	}

}
