package com.techmahindra.vehicletelemetry.vo;

import java.io.Serializable;

public class MaintenanceAlert implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String vin;
	private String city;
	private String model;
	private String alertDateTime;
	private String alertMsg;
	public String getVin() {
		return vin;
	}
	public void setVin(String vin) {
		this.vin = vin;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getModel() {
		return model;
	}
	public void setModel(String model) {
		this.model = model;
	}
	public String getAlertDateTime() {
		return alertDateTime;
	}
	public void setAlertDateTime(String alertDateTime) {
		this.alertDateTime = alertDateTime;
	}
	public String getAlertMsg() {
		return alertMsg;
	}
	public void setAlertMsg(String alertMsg) {
		this.alertMsg = alertMsg;
	}
	
}