package com.amazon.gdpr.model.gdpr.input;

public class ImpactFieldAnonymization {

	String impactTableName;
	String impactFieldName;
	String transformationType;
	
	public ImpactFieldAnonymization() {
		
	}
	
	public ImpactFieldAnonymization(String impactTableName, String impactFieldName, String transformationType) {
		this.impactTableName = impactTableName;
		this.impactFieldName = impactFieldName;
		this.transformationType = transformationType;
		
	}

	public String getImpactTableName() {
		return impactTableName;
	}

	public void setImpactTableName(String impactTableName) {
		this.impactTableName = impactTableName;
	}

	public String getImpactFieldName() {
		return impactFieldName;
	}

	public void setImpactFieldName(String impactFieldName) {
		this.impactFieldName = impactFieldName;
	}

	public String getTransformationType() {
		return transformationType;
	}

	public void setTransformationType(String transformationType) {
		this.transformationType = transformationType;
	}	
	
	
}