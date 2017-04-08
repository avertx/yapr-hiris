package com.liminala.hiris;

public class TickerXTicker implements Tick {
	
	private Ticker t1;
	private Ticker t2;
	
	public TickerXTicker(Ticker t1, Ticker t2) {
		super();
		
		if(t1.getId().toString().compareTo(t2.getId().toString()) < 0){
		
			this.t1 = t1;
			this.t2 = t2;
			
		}else if(t1.getId().toString().compareTo(t2.getId().toString()) > 0){
			
			this.t1 = t2;
			this.t2 = t1;
			
		}else{
			
			this.t1 = t1;
			this.t2 = t2;
		}
		
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((t1 == null) ? 0 : t1.hashCode());
		result = prime * result + ((t2 == null) ? 0 : t2.hashCode());
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
		TickerXTicker other = (TickerXTicker) obj;
		if (t1 == null) {
			if (other.t1 != null)
				return false;
		} else if (!t1.equals(other.t1))
			return false;
		if (t2 == null) {
			if (other.t2 != null)
				return false;
		} else if (!t2.equals(other.t2))
			return false;
		return true;
	}
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		TickerXTicker other = (TickerXTicker) obj;
//		
//		if ((t1.equals(other.t1) && t2.equals(other.t2)) || (t2.equals(other.t1) && t1.equals(other.t2))){
//			return true;
//		}
//		
//		return false;
//	}
	@Override
	public String toString() {
		return "["+t1.getId().toString()+" "+t2.getId().toString()+"]";
	}


	public Ticker getT1() {
		return t1;
	}


	public Ticker getT2() {
		return t2;
	}

	
}
