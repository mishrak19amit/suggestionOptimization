package com.moglix.orderanalysis.model;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CategoryBrandAttributeValueOrderCount {

	String categoryID;
	String brandID;
	String attributeID;
	String value;
	String orderCount;

	public String getBrandID() {
		return brandID;
	}

	public void setBrandID(String brandID) {
		this.brandID = brandID;
	}

	public String getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(String orderCount) {
		this.orderCount = orderCount;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getCategoryID() {
		return categoryID;
	}

	public void setCategoryID(String categoryID) {
		this.categoryID = categoryID;
	}

	public String getAttributeID() {
		return attributeID;
	}

	public void setAttributeID(String attributeID) {
		this.attributeID = attributeID;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
