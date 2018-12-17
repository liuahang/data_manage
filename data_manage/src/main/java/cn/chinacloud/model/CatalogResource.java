package cn.chinacloud.model;

import java.util.Date;

public class CatalogResource {

	private Integer id;
	private String name;
	private String originalName;
	private String description;
	private Integer openProperty;
	private String openCondition;
	private Integer deleted;
	private Date createdAt;
	private Date updatedAt;
	private String structured;
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOriginalName() {
		return originalName;
	}
	public void setOriginalName(String originalName) {
		this.originalName = originalName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public Integer getOpenProperty() {
		return openProperty;
	}
	public void setOpenProperty(Integer openProperty) {
		this.openProperty = openProperty;
	}
	public String getOpenCondition() {
		return openCondition;
	}
	public void setOpenCondition(String openCondition) {
		this.openCondition = openCondition;
	}
	public Integer getDeleted() {
		return deleted;
	}
	public void setDeleted(Integer deleted) {
		this.deleted = deleted;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	public Date getUpdatedAt() {
		return updatedAt;
	}
	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}
	public String getStructured() {
		return structured;
	}
	public void setStructured(String structured) {
		this.structured = structured;
	}
	
	
}
