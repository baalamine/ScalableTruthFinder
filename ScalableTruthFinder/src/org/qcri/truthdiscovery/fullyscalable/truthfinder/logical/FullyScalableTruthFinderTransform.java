/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Transform;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import cern.colt.Arrays;

/**
 * @author mlba
 * @created date: Jan 24, 2016
 * Scalable Truth Finder Transform 
 * 
 * @Input: line consisting of a datum
 * @Output: KeyValuePair<Integer, SourceClaim>
 */
public class FullyScalableTruthFinderTransform extends Transform<KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>, String>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	//return String[]
	public KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>> transform(String line, RheemContext context) {
		// TODO Auto-generated method stub
		System.out.println("Transform Operator") ;
		String regexp	    = "\",\"" ;
		Pattern p 			= Pattern.compile(regexp) ;
		String[] record 	= p.split(line, 7);
		
		//System.out.println(Arrays.toString(record)) ;
		int claimId 		= Integer.parseInt(record[0].replace("\"", "")) ;
		String objectId 	= record[1].replace("\"", "") ; //2 for book, 1 for bio
		String propertyName = record[2].replace("\"", "") ; 
		String stringValue 	= record[3].replace("\"", "") ; 
		String sourceId 	= record[4].replace("\"", "") ; 
		String timeStamp 	= record[5].replace("\"", "") ; 
		
		SourceClaim claim = new SourceClaim (claimId, objectId, "", propertyName, stringValue, 1.0, timeStamp, new Source(sourceId) ) ;
		//dataItem = objectId + propertyName
		String dataItemKey = claim.dataItemKey() ;
		
		
		List<SourceClaim> r = new ArrayList<SourceClaim> () ;
		r.add(claim) ;
		
		// when the dataset is clean this should be removed
		if (stringValue.equals("Not Available") || stringValue.trim().isEmpty()) {
			return new KeyValuePair<>(dataItemKey.hashCode(), new KeyValuePair<>(1, null)) ;
		}
		if (objectId == null || objectId.trim().equalsIgnoreCase("null")) {
			return new KeyValuePair<>(dataItemKey.hashCode(), new KeyValuePair<>(1, null)) ;
		}
		
		return new KeyValuePair<>(dataItemKey.hashCode(), new KeyValuePair<>(1, r)) ;
	}

}
