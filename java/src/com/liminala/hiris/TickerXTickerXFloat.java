package com.liminala.hiris;

public class TickerXTickerXFloat implements Comparable<TickerXTickerXFloat> {

	private TickerXTicker tt;
	private Float f;
	
	public TickerXTickerXFloat(TickerXTicker tt, Float f) {
		super();
		this.tt = tt;
		this.f = f;
	}

	
	
	public Float getF() {
		return f;
	}

	public void setF(Float f) {
		this.f = f;
	}



	public TickerXTicker getTt() {
		return tt;
	}



	public void setTt(TickerXTicker tt) {
		this.tt = tt;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tt == null) ? 0 : tt.hashCode());
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
		TickerXTickerXFloat other = (TickerXTickerXFloat) obj;
		if (tt == null) {
			if (other.tt != null)
				return false;
		} else if (!tt.equals(other.tt))
			return false;
		return true;
	}


	@Override
	public int compareTo(TickerXTickerXFloat o) {
		if(this.getF() < o.getF()){
			return -1;
		}else if(this.getF() > o.getF()){
			return 1;
		}
		return 0;
		
	}
	
	
}
