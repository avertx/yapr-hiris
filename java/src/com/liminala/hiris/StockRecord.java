package com.liminala.hiris;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StockRecord implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	private Date date;
	private Ticker ticker;
	private Float unadjOpen;
	private Float unadjHigh;
	private Float unadjLow;
	private Float unadjClose;
	private Float unadjVolume;
	private Float dividend;
	private Float splitRatio;
	private Float adjOpen;
	private Float adjHigh;
	private Float adjLow;
	private Float adjClose;
	private Float adjVolume;

	public StockRecord(Date date, Ticker ticker, Float unadjOpen, Float unadjHigh, Float unadjLow, Float unadjClose,
			Float unadjVolume, Float dividend, Float splitRatio, Float adjOpen, Float adjHigh, Float adjLow,
			Float adjClose, Float adjVolume) {
		super();
		this.date = date;
		this.ticker = ticker;
		this.unadjOpen = unadjOpen;
		this.unadjHigh = unadjHigh;
		this.unadjLow = unadjLow;
		this.unadjClose = unadjClose;
		this.unadjVolume = unadjVolume;
		this.dividend = dividend;
		this.splitRatio = splitRatio;
		this.adjOpen = adjOpen;
		this.adjHigh = adjHigh;
		this.adjLow = adjLow;
		this.adjClose = adjClose;
		this.adjVolume = adjVolume;
	}

	public StockRecord(Date d, Ticker ticker) {
		this.date = d;
		this.ticker = ticker;
	}

	public String toString() {

		return String.format("%s %s, %f", ticker.getId().toString().substring(ticker.getId().toString().length() - 4),
				df.format(date), adjClose);
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Ticker getTicker() {
		return ticker;
	}

	public void setTicker(Ticker ticker) {
		this.ticker = ticker;
	}

	public Float getUnadjOpen() {
		return unadjOpen;
	}

	public void setUnadjOpen(Float unadjOpen) {
		this.unadjOpen = unadjOpen;
	}

	public Float getUnadjHigh() {
		return unadjHigh;
	}

	public void setUnadjHigh(Float unadjHigh) {
		this.unadjHigh = unadjHigh;
	}

	public Float getUnadjLow() {
		return unadjLow;
	}

	public void setUnadjLow(Float unadjLow) {
		this.unadjLow = unadjLow;
	}

	public Float getUnadjClose() {
		return unadjClose;
	}

	public void setUnadjClose(Float unadjClose) {
		this.unadjClose = unadjClose;
	}

	public Float getUnadjVolume() {
		return unadjVolume;
	}

	public void setUnadjVolume(Float unadjVolume) {
		this.unadjVolume = unadjVolume;
	}

	public Float getDividend() {
		return dividend;
	}

	public void setDividend(Float dividend) {
		this.dividend = dividend;
	}

	public Float getSplitRatio() {
		return splitRatio;
	}

	public void setSplitRatio(Float splitRatio) {
		this.splitRatio = splitRatio;
	}

	public Float getAdjOpen() {
		return adjOpen;
	}

	public void setAdjOpen(Float adjOpen) {
		this.adjOpen = adjOpen;
	}

	public Float getAdjHigh() {
		return adjHigh;
	}

	public void setAdjHigh(Float adjHigh) {
		this.adjHigh = adjHigh;
	}

	public Float getAdjLow() {
		return adjLow;
	}

	public void setAdjLow(Float adjLow) {
		this.adjLow = adjLow;
	}

	public Float getAdjClose() {
		return adjClose;
	}

	public void setAdjClose(Float adjClose) {
		this.adjClose = adjClose;
	}

	public Float getAdjVolume() {
		return adjVolume;
	}

	public void setAdjVolume(Float adjVolume) {
		this.adjVolume = adjVolume;
	}

}
