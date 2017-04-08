package com.liminala.hiris;

import java.io.Serializable;
import java.util.Comparator;

public class DateComparator implements Comparator<StockRecord>, Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


		@Override
		public int compare(StockRecord o1, StockRecord o2) {
			return o1.getDate().compareTo(o2.getDate());
		}

	
}
