package com.liminala.hiris;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

public class Ticker implements Serializable, Tick {

	private static HashMap<String, UUID> uuidForTicker = new HashMap<String, UUID>();
	private static HashMap<UUID, String> tickerForUUID = new HashMap<UUID, String>();
	
	private String symbol;
	private UUID id;
	
	public Ticker(String symbol) {
		super();
		this.symbol = symbol;
		UUID uuidx = null;
		if((uuidx = uuidForTicker.get(symbol)) != null){
			this.id = uuidx;
		}else{
			if(!uuidForTicker.isEmpty()){
		//		System.err.println("Obfmap set in class Ticker, but this symbol wasn't found "+symbol);
			}
			this.id = UUID.randomUUID();	
			uuidForTicker.put(symbol, this.id);
		}
		
	}
	
	
	public Ticker(UUID uuid) throws NoTickerForUUIDException {
		super();
		
		String symbol = null;
		if((symbol = tickerForUUID.get(uuid)) == null){
			throw new NoTickerForUUIDException();
		}
		
		this.symbol = symbol;
		this.id = uuid;

	}
	
	public String getSymbol() {
		return symbol;
	}
	public UUID getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Ticker other = (Ticker) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public static HashMap<String, UUID> getUuidForTicker() {
		return uuidForTicker;
	}

	public static HashMap<UUID, String> getTickerForUUID() {
		return tickerForUUID;
	}
	
	@Override
	public String toString() {
		return id.toString();
	}

	
	
	
}
