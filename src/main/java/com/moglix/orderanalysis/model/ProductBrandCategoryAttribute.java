package com.moglix.orderanalysis.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class ProductBrandCategoryAttribute {

	String msn;
	String categoryID;
	String brandID;
	Map<String, List<String>> attributeList;
	String orderCount;

	public String getMsn() {
		return msn;
	}

	public void setMsn(String msn) {
		this.msn = msn;
	}

	public String getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(String orderCount) {
		this.orderCount = orderCount;
	}

	public String getCategoryID() {
		return categoryID;
	}

	public void setCategoryID(String categoryID) {
		this.categoryID = categoryID;
	}

	public String getBrandID() {
		return brandID;
	}

	public void setBrandID(String brandID) {
		this.brandID = brandID;
	}

	public Map<String, List<String>> getAttributeList() {
		return attributeList;
	}

	public void setAttributeList(Map<String, List<String>> attributeList) {
		this.attributeList = attributeList;
	}

	@Override
	public String toString()
	{
		return ReflectionToStringBuilder.toString(this);
	}
	
}
