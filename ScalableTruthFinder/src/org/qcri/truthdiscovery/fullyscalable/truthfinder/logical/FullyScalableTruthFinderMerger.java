/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;

import org.qcri.rheem.core.logicallayer.GlobalVariableMerger;

import gnu.trove.map.hash.THashMap;

/**
 * @author mlba
 * Scalable Truth Finder Merger
 * Updating global shared variables
 */
public class FullyScalableTruthFinderMerger extends GlobalVariableMerger {

	@Override
	public THashMap<String, Object> merge(THashMap<String, Object> hashMap) 
	{
		// TODO Auto-generated method stub
		//System.out.println("Merger Op") ;
		THashMap<String, Object> newHashMap = new THashMap<>();
        hashMap.forEach((k,v) -> {
        	//System.out.println(v.getClass().getName());
        	//System.out.println(k+":::"+v) ;
            newHashMap.put(k, v);
            });
        
        return newHashMap;
	}

}
