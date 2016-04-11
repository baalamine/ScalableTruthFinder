package org.qcri.truthdiscovery.util.dataModel.data;

import java.io.Serializable;
import org.qcri.truthdiscovery.util.dataModel.dataFormatter.DataCleaner;
import org.qcri.truthdiscovery.util.dataModel.dataFormatter.DataTypeMatcher;
import org.qcri.truthdiscovery.util.dataModel.dataFormatter.DataTypeMatcher.ValueType; 



public class SourceClaim implements Serializable {

	private int id;
//	private String entityId;
	private String objectIdentifier;
	private String uncleanedObjectIdentifier;
	private String propertyName;
	private String propertyValueString;
	private Object propertyValue;
	private ValueType valueType;
	private boolean clean;
	private Source source;
	private String timeStamp = null;
	private String timeStampFormat;
	private double weight = 1.0;

	private boolean trueClaimByVoter = false;

	private ValueBucket bucket;

	public SourceClaim(int id, String objectIdentifier, String uncleanedObjectID,
			String propertyName, String propertyValueString, double claimWeight, String timeStamp, Source source)
	{
		this.id = id;
		this.objectIdentifier = objectIdentifier;
		this.uncleanedObjectIdentifier = uncleanedObjectID;
		this.propertyName = propertyName;
		this.propertyValueString = propertyValueString;
		this.weight = claimWeight;
		this.timeStamp = timeStamp;

		this.valueType = DataTypeMatcher.getPropertyDataType(propertyName);
		this.propertyValue = DataCleaner.clean(propertyValueString, valueType);

		if (propertyValue instanceof String && ! DataTypeMatcher.savedAsString(valueType)) 
		{
			clean = false;
		} 
		else 
		{
			clean = true;
		}
		
		this.source = source;
	}

	public SourceClaim() {
		// TODO Auto-generated constructor stub
	}

	public int getId() {
		return id;
	}
//	public String getEntityId() {
//		return entityId;
//	}
	public String getObjectIdentifier() {
		return objectIdentifier;
	}
	public String getUncleanedObjectIdentifier() {
		return uncleanedObjectIdentifier;
	}
	public String getPropertyName() {
		return propertyName;
	}
	public Object getPropertyValue() {
		return propertyValue;
	}
	public Source getSource() {
		return source;
	}
	public void setSource(Source source) {
		this.source = source;
	}
	public ValueType getValueType() {
		return valueType;
	}
	public void setPropertyValue(Object propertyValue) {
		this.propertyValue = propertyValue;
	}
	public String getTimeStamp() {
		return timeStamp;
	}
	public String getTimeStampFormat() {
		return timeStampFormat;
	}
	public String getPropertyValueString() {
		return propertyValueString;
	}
	public double getWeight() {
		return weight;
	}
	public int dataItemValueHashCode() {
		String s =  /*entityId +*/ objectIdentifier + propertyName + propertyValueString;
		return s.hashCode();
	}

	public String dataItemKey() {
		String s =  /*entityId +*/ objectIdentifier + propertyName;
		return s;
	}
	public static String dataItemKey(/*String entityId,*/ String objectId, String propertyName) {
		String s =  /*entityId +*/ objectId + propertyName;
		return s;
	}
	public String objectKey() {
		String s = /*entityId +*/ objectIdentifier;
		return s;
	}

	public boolean isTrueClaimByVoter
	() {
		return trueClaimByVoter;
	}
	public void setTrueClaimByVoter(boolean trueClaim) {
		this.trueClaimByVoter = trueClaim;
	}
	public boolean isClean() {
		return clean;
	}
	/**
	 * Only the DataSet object manage the addition of a bucket.
	 */
	void setBucket(ValueBucket bucket) {
		this.bucket = bucket;
	}
	public ValueBucket getBucket() {
		return bucket;
	}
}
