package com.liminala.hiris;

public class TwoStockRecords {
	
	private StockRecord earlier;
	private StockRecord later;
	
	private Float percentChange;

	public TwoStockRecords(){}
	
	public TwoStockRecords(StockRecord earlier, StockRecord later, Float percentChange) {
		super();
		this.earlier = earlier;
		this.later = later;
		this.percentChange = percentChange;
	}

	public StockRecord getEarlier() {
		return earlier;
	}

	public void setEarlier(StockRecord earlier) {
		this.earlier = earlier;
	}

	public StockRecord getLater() {
		return later;
	}

	public void setLater(StockRecord later) {
		this.later = later;
	}

	public Float getPercentChange() {
		return percentChange;
	}

	public void setPercentChange(Float percentChange) {
		this.percentChange = percentChange;
	}
	
}
