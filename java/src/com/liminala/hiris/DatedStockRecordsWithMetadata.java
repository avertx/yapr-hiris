package com.liminala.hiris;

import java.util.Date;
import java.util.HashMap;

public class DatedStockRecordsWithMetadata {

	private int maxNumberOfRecords = 0;
	private Ticker maxRecordTicker = null;
	private HashMap<Date, HashMap<Ticker, StockRecord>> byDate = new HashMap<Date, HashMap<Ticker, StockRecord>>();

	public HashMap<Date, HashMap<Ticker, StockRecord>> getByDate() {
		return byDate;
	}

	public int getMaxNumberOfRecords() {
		return maxNumberOfRecords;
	}

	public void setMaxNumberOfRecords(int maxNumberOfRecords) {
		this.maxNumberOfRecords = maxNumberOfRecords;
	}

	public Ticker getMaxRecordTicker() {
		return maxRecordTicker;
	}

	public void setMaxRecordTicker(Ticker maxRecordTicker) {
		this.maxRecordTicker = maxRecordTicker;
	}

	
	
	
}
