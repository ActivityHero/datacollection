package com.ah.scraper.dto;

public class ActivityDTO {
	private String name;
	private int provider_id;
	private String description;
	private int from_age;
	private int to_age;
	private float from_price;
	private float to_price;
	private String program_for;
	private String activityType;
	private long created_at;
	private long updated_at;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getprovider_id() {
		return provider_id;
	}
	public void setprovider_id(int provider_id) {
		this.provider_id = provider_id;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public int getFrom_age() {
		return from_age;
	}
	public void setFrom_age(int from_age) {
		this.from_age = from_age;
	}
	public int getTo_age() {
		return to_age;
	}
	public void setTo_age(int to_age) {
		this.to_age = to_age;
	}
	public float getFrom_price() {
		return from_price;
	}
	public void setFrom_price(float from_price) {
		this.from_price = from_price;
	}
	public float getTo_price() {
		return to_price;
	}
	public void setTo_price(float to_price) {
		this.to_price = to_price;
	}

	/**
	 * @return the provider_id
	 */
	public int getProvider_id() {
		return provider_id;
	}
	/**
	 * @param provider_id the provider_id to set
	 */
	public void setProvider_id(int provider_id) {
		this.provider_id = provider_id;
	}
	/**
	 * @return the program_for
	 */
	public String getProgram_for() {
		return program_for;
	}
	/**
	 * @param program_for the program_for to set
	 */
	public void setProgram_for(String program_for) {
		this.program_for = program_for;
	}
	/**
	 * @return the activityType
	 */
	public String getActivityType() {
		return activityType;
	}
	/**
	 * @param activityType the activityType to set
	 */
	public void setActivityType(String activityType) {
		this.activityType = activityType;
	}
	public long getCreated_at() {
		return created_at;
	}
	public void setCreated_at(long created_at) {
		this.created_at = created_at;
	}
	public long getUpdated_at() {
		return updated_at;
	}
	public void setUpdated_at(long updated_at) {
		this.updated_at = updated_at;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ActivityDTO [name=" + name + ", provider_id=" + provider_id
				+ ", description=" + description + ", from_age=" + from_age
				+ ", to_age=" + to_age + ", from_price=" + from_price
				+ ", to_price=" + to_price + ", program_for=" + program_for
				+ ", activityType=" + activityType + ", created_at="
				+ created_at + ", updated_at=" + updated_at + "]";
	}
}
