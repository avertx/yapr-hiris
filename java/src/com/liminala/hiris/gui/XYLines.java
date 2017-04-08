package com.liminala.hiris.gui;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.swing.JPanel;

import com.liminala.hiris.Tick;
import com.liminala.hiris.TickerXTicker;

public class XYLines extends JPanel implements MouseMotionListener, MouseListener {

	private String[] bucketLabels = new String[0];
	private int numBuckets = 0;
	private TreeMap<Date, HashMap<Tick, Float>> data = new TreeMap<Date, HashMap<Tick, Float>>();
	private Float max;
	private Float min = 0f;
	private Boolean onlyPositive = false;
	private boolean compareTwoPanel = false;
	private boolean range0to1 = true;
	private Notify notify;
	

	
	private static ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>(){

		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	public XYLines(){
//		addMouseMotionListener(this);
		addMouseListener(this);
	}
	
	public XYLines(String[] years, TreeMap<Date, HashMap<Tick, Float>> data, float max) {
		this.bucketLabels = years;
		this.data = data;
		this.max = max;
	}

	public static abstract class Notify {
		public abstract void onClick(Object o, Date start, Date end);
	}
	

	@Override
	public void mouseClicked(MouseEvent e) {
		
		
		System.out.println("click, examining "+hotOnes.size()+" currently selected lines");
		if(hotOnes.size() > 0){
			
			Map<Tick, Double> closest = new HashMap<>();

			for(Tick tt : hotOnes){
				
				for(Line2D.Float line : lines.get(tt)){
					
					double dist = line.ptSegDist(e.getX(), e.getY());
					closest.compute(tt, (k,v) -> v == null? new Double(dist):Math.min(v, dist) );
					
				}
			}
			
			Tick lowest = null;
			double lowestVal = Double.MAX_VALUE;
			
			for(Entry<Tick, Double> ent : closest.entrySet()){
				if(ent.getValue() < lowestVal){
					lowest = ent.getKey();
					lowestVal = ent.getValue();
				}
			}
		
			if(e.isShiftDown()){
				colors.computeIfPresent(lowest, (k,v) -> getRandomColor());
			}
			
//			closest.forEach((k,v) -> System.out.println("click distance from click to "+k.toString()+" "+v.doubleValue()));
//			System.out.println("click distance from selected click: "+closest.get(lowest));
			
			if(lowest != null){
				hotOnes.remove(lowest);
				hotOnes.add(lowest);
				
				if(notify != null){
					
					Date firstDate = this.data.firstKey();
					Date lastDate = this.getData().lastKey();
					
					notify.onClick(lowest, firstDate, lastDate);
				}
				this.repaint();
			}
		}
	}

	int mousePressedX = -1;
	int mousePressedY = -1;
	
	@Override
	public void mousePressed(MouseEvent e) {
		mousePressedX = e.getX();
		mousePressedY = e.getY();
	}

	LinkedHashSet<Tick> hotOnes = new LinkedHashSet<>();
	
	@Override
	public void mouseReleased(MouseEvent e) {

		//		if(mousePressedX != -1){
		if(e.isMetaDown()){
			boolean changed = false;

			if(!e.isShiftDown()){
				hotOnes.clear();
				changed = true;
			}


			for(Entry<Tick, ArrayList<java.awt.geom.Line2D.Float>> fa : lines.entrySet()){

				for(Line2D.Float line : fa.getValue()){

					if(line.intersectsLine(e.getX(), e.getY(), mousePressedX, mousePressedY)){

						System.out.println("mouseover line: "+fa.getKey().toString()+" "+e.getX()+", "+e.getY()+" : "+e.getXOnScreen()+","+e.getYOnScreen());
						hotOnes.add(fa.getKey());
						changed = true;
						hotOne = null;
					}
				}
			}
			if(changed){
				System.out.println("requesting repaint after mouse drag intersected lines");
				this.repaint();
			}
		}
//		}
//		mousePressedX = -1;
//		mousePressedY = -1;
	}

	@Override
	public void mouseEntered(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mouseExited(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mouseDragged(MouseEvent e) {
		
//		TickerXTicker last = null;
//		for(Entry<TickerXTicker, ArrayList<java.awt.geom.Line2D.Float>> fa : lines.entrySet()){
//			
//			for(Line2D.Float line : fa.getValue()){
//
//				if(line.ptSegDist(e.getX(), e.getY()) < 0.6f){
//					if(last != null && last.equals(fa.getKey())){
//						//System.out.println("no repaint. mouseover line: "+fa.getKey().toString()+" "+e.getX()+", "+e.getY()+" : "+e.getXOnScreen()+","+e.getYOnScreen());
//					}else{
//						System.out.println("mouseover line: "+fa.getKey().toString()+" "+e.getX()+", "+e.getY()+" : "+e.getXOnScreen()+","+e.getYOnScreen());
//						hotOne = fa.getKey();
//						this.repaint();
//						last = fa.getKey();
//					}
//					
//				}
//			}
//		}
	}

	@Override
	public void mouseMoved(MouseEvent e) {

		
	}

	TickerXTicker hotOne = null;
	
	HashMap<Tick,Color> colors = new HashMap<Tick,Color>();
	
	HashMap<Tick,ArrayList<Line2D.Float>> lines = new HashMap<Tick,ArrayList<Line2D.Float>>();
	
	DateFormat dfYearOnly = new SimpleDateFormat("yyyy");
	
	
	@Override
	protected void paintComponent(Graphics g) {

		super.paintComponent(g);

		Graphics2D g2d = (Graphics2D)g;

		g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);

		int h = this.getHeight();
		int w = this.getWidth();

		g2d.setBackground(new Color(244,244,244));
		g2d.clearRect(0, 0, w, h);
		
		Font f = new Font("COURIER",Font.BOLD, 20);
		g2d.setFont(f);
		
		if(this.data == null || this.data.size() == 0){
			System.out.println("no data yet");
			String load = "Not Selected or LOADING";
			g2d.setColor(new Color(40,40,40));
			g2d.drawString(load, (w/2)-(g2d.getFontMetrics().stringWidth(load)/2), h/2);
		}else{
			
			System.out.println("redrawing, max = "+max);
			lines.clear();
			
			HashMap<Tick, Float> d1 = null;
			
//			if(!range0to1){
//				max = (float) Math.log(max);
//				min = (float) Math.log(Math.abs(min));
//			}
			
			Set<Tick> appearances = new HashSet<Tick>();
			
			this.data.forEach((k,v) -> v.forEach((k1, v1) -> appearances.add(k1)));
			
			hotOnes.retainAll(appearances);
			
			
			Date firstDate = null;
			for(Entry<Date, HashMap<Tick, Float>> a : this.data.entrySet()){
				firstDate = a.getKey();
			}
			d1 = this.data.get(firstDate);

			float range = max;
			if(min < 0) range = range + Math.abs(min);
			
			 int fontSizeSmall = 8;
			 int fontSizeLarge= 12;
			 
			 Font smallFont = new Font("COURIER", Font.PLAIN, fontSizeSmall);
			 Font largeFont = new Font("COURIER", Font.PLAIN, fontSizeLarge);
			
			int zeroCrossY = h;
			if(onlyPositive){
				zeroCrossY = h;
			}else{
				zeroCrossY = Math.round(h*(max/range));//Math.round(h/2);
			}

			float maxOfBothMaxAndMin = Math.max(Math.abs(min),max);
			float incrementer = 20f;
			
			if(maxOfBothMaxAndMin < 1.0f){
				incrementer = .1f;
			}else if(maxOfBothMaxAndMin <= 5){
				incrementer = .5f;
			}else if(maxOfBothMaxAndMin <= 10){
				incrementer = 1;
			}else if(maxOfBothMaxAndMin <= 20){
				incrementer = 2;
			}else if(maxOfBothMaxAndMin <= 50){
				incrementer = 5;	
			}else if(maxOfBothMaxAndMin <= 100){
				incrementer = 10;
			}else{
				incrementer = 20;
			}



			float perc = 0f;
			int numLines = 0;
			while(perc <= maxOfBothMaxAndMin){
				perc += incrementer;
				numLines++;
			}
			
			float percNeg = 0f;
			int numNegLines = 0;
			while(percNeg <= Math.abs(min)){
				percNeg += incrementer;
				numNegLines++;
			}

//			numLines -= 1;
			float topLineHeight = zeroCrossY*(perc/max);
			System.out.println("top line height = "+topLineHeight+" h = "+h+" perc = "+perc+" numLines = "+numLines);
			
			Color positiveLineColor = new Color(240,240,140);
			Color negativeLineColor = new Color(230,230,120);
			
			System.out.println("Max: "+max+" numLines = "+numLines);

			if(onlyPositive){

				
				for(int k = 1; k <= numLines; k++){

					g2d.setFont(smallFont);
					g2d.setColor(positiveLineColor);
					
					int y = (h)-Math.round((topLineHeight/numLines)*k);
					g2d.drawLine(0, y, w, y);

					g2d.setFont(smallFont);
					g2d.setColor(Color.BLACK);
					g2d.drawString((incrementer*k)+"", 10, y+fontSizeSmall/2);
					System.out.println("top line height = "+topLineHeight+" h = "+h+" y = "+y+" numLines = "+numLines+" k= "+k+" subtracting: "+Math.round((topLineHeight/numLines)*k));

				}
				g2d.setFont(smallFont);
				g2d.setColor(Color.RED);
				g2d.drawString(""+max, w-g2d.getFontMetrics().stringWidth(""+max), g2d.getFontMetrics().getHeight());
				
			}else{

//				float bottomLineHeight = Math.abs((h-zeroCrossY))*(perc/((Math.abs(min)>max?Math.abs(min):max)));
				float bottomLineHeight = Math.abs((h-zeroCrossY))*(percNeg/Math.abs(min));
				
				for(int k = 1; k <= numNegLines; k++){

					g2d.setColor(negativeLineColor);
					g2d.setStroke(new BasicStroke(1f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));

					int y = (zeroCrossY)+Math.round((bottomLineHeight/numNegLines)*k);
					g2d.drawLine(0, y, w, y);

					g2d.setFont(smallFont);
					g2d.setColor(Color.BLACK);
					g2d.drawString((incrementer*k)+"", 10, y-fontSizeSmall/2);
					
					System.out.println("bottom line height = "+bottomLineHeight+" h = "+h+" y = "+y+" numLines = "+numNegLines+" k= "+k+" adding: "+Math.round((bottomLineHeight/numNegLines)*k));
					
				}
				g2d.setFont(smallFont);
				g2d.setColor(Color.BLUE);
				g2d.drawString(""+min,  w-g2d.getFontMetrics().stringWidth(""+min), h-g2d.getFontMetrics().getHeight());

//				float topLineHeight = zeroCrossY*perc;
				
				for(int k = 1; k <= numLines; k++){

					g2d.setColor(positiveLineColor);
					g2d.setStroke(new BasicStroke(1f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
					int y = (zeroCrossY)-Math.round((topLineHeight/numLines)*k);
					g2d.drawLine(0, y, w, y);
					g2d.setFont(smallFont);
					g2d.setColor(Color.BLACK);
					g2d.drawString((incrementer*k)+"", 10, y+fontSizeSmall/2);
					
					System.out.println("top line height = "+topLineHeight+" h = "+h+" y = "+y+" numLines = "+numLines+" k= "+k+" subtracting: "+Math.round((topLineHeight/numLines)*k));

				}
				g2d.setFont(smallFont);
				g2d.setColor(Color.RED);
				g2d.drawString(""+max, w-g2d.getFontMetrics().stringWidth(""+max), g2d.getFontMetrics().getHeight());
			}

			g2d.setFont(smallFont);
			g2d.setColor(Color.BLACK);
			g2d.transform(new AffineTransform());
			for(int k = 1; k < bucketLabels.length; k++){
				int x = (w/bucketLabels.length)*k;
				g2d.drawString(bucketLabels[k], x, 0+10);
			}

			g2d.setStroke(new BasicStroke(1f));
			
			for(Tick tt : appearances){

				int lastX = 0;
				int lastY = 0;

//				for(int i = 0; i < numBuckets; i++){
				int count = 0;
				for(Entry<Date, HashMap<Tick, Float>> j2 : data.entrySet()){

//					if(count == 0) System.out.println("creating segments, start date: "+df.get().format(j2.getKey()));
					HashMap<Tick, Float> j = j2.getValue();

					if(j != null){

						Float val = j.get(tt);

						if(val == null){
							Color oldC = g2d.getColor();
							Stroke oldS = g2d.getStroke();

							g2d.setColor(Color.PINK);
							g2d.setStroke(new BasicStroke(2f));

							g2d.drawLine(lastX, lastY, lastX, h);
							//System.err.println("val is null");

							g2d.setStroke(oldS);
							g2d.setColor(oldC);

						}else{


							float wi = (float)count++/(float)numBuckets;

							int destH = 0;
							
							if(val > max) throw new RuntimeException("val is over max, val: "+val+" max:"+max);
							
							if(val >= 0){
								destH = ((zeroCrossY) - Math.round((zeroCrossY) * (val/max)));
							}else{
								destH = ((zeroCrossY) + Math.round((h-zeroCrossY) * (val/min)));
							}
							
							int destW = Math.round(wi*w);


							if(lastX == 0 && lastY == 0){

//								System.out.println("starting string for : "+tt.toString()+" "+year);
							}else{
//								g2d.drawLine(lastX, lastY, destW, destH);
								
								lines.computeIfAbsent(tt, (d)->new ArrayList<Line2D.Float>()).add(new Line2D.Float(lastX, lastY, destW, destH));
								
								
								
//								System.out.println("string for : "+tt.toString()+" "+year+" w:"+destW+" h:"+destH+" val:"+val+" val/max"+(val/max)+" rounded: "+(Math.round((h/2) * (val/max))));
							}

							lastX = destW;
							lastY = destH;
						}

					}
				}
			}
			
			if(compareTwoPanel){
				
				Tick t1 = null;
				Tick t2 = null;
						
				boolean once = false;
				for(Entry<Tick, ArrayList<java.awt.geom.Line2D.Float>> d : lines.entrySet()){
					if(once){
						t2 = d.getKey();
					}else{
						t1 = d.getKey();
					}
					once = true;
				}
				
				colors.computeIfAbsent(t1, (d) -> new Color(244,20,40));
				colors.computeIfAbsent(t2, (d) -> new Color(0,0,0));
				
				if(colors.get(t1).equals(colors.get(t2))){
					colors.put(t1, new Color(244,20,40));
					colors.put(t2, new Color(0,0,0));
				}
				
			}
			
			
			for(Entry<Tick, ArrayList<java.awt.geom.Line2D.Float>> fa : lines.entrySet()){
				
				g2d.setColor(colors.computeIfAbsent(fa.getKey(), (d) -> getRandomColor()));
				
				if (hotOnes != null && hotOnes.contains(fa.getKey())){

				}else{
					g2d.setStroke(new BasicStroke(Math.max(1, 1), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
					//g2d.setColor(colors.get(fa.getKey()));
				}
				
				for(Line2D.Float line : fa.getValue()){
					
					g2d.drawLine(Math.round(line.x1), Math.round(line.y1), Math.round(line.x2), Math.round(line.y2));
				}
			}

			hotOnes.forEach(r -> {
				g2d.setColor(colors.computeIfAbsent(r, (d) -> getRandomColor()));
				if(compareTwoPanel){
					g2d.setStroke(new BasicStroke(Math.max(1, 1), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
				}else{
					g2d.setStroke(new BasicStroke(Math.max(1, 4), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
				}

				for(Line2D.Float line : lines.get(r)){
					g2d.drawLine(Math.round(line.x1), Math.round(line.y1), Math.round(line.x2), Math.round(line.y2));
				}
			//	System.out.println("drawing "+lines.get(r).size()+" line segments for "+r);
			});
			
			
			//hotOnes.clear();
			

			g2d.setColor(Color.BLACK);
			g2d.setStroke(new BasicStroke(Math.max(1, 8*(h/10000))));
			
			
			g2d.drawLine(0, zeroCrossY, w, zeroCrossY);
			
			
			
			g2d.setColor(Color.CYAN);
			g2d.drawLine(0, ((zeroCrossY) - Math.round((zeroCrossY) * (max/max))), w, ((zeroCrossY) - Math.round((zeroCrossY) * (max/max))));
			g2d.drawLine(0, ((zeroCrossY) - Math.round((h-zeroCrossY) * (min/Math.abs(min)))), w, ((zeroCrossY) - Math.round((h-zeroCrossY) * (min/Math.abs(min)))));

//			g2d.drawString("! exhibited separation", w/2, Math.round(h*.8));

		}

	}

	public static Color getRandomColor(){
		
		int R = (int) (Math.random( )*256);
		int G = (int)(Math.random( )*256);
		int B= (int)(Math.random( )*256);
		
		return new Color(R, G, B);
	}
	public String[] getBucketLabels() {
		return bucketLabels;
	}

	public void setBucketLabels(String[] years) {
		this.bucketLabels = years;
	}

	public TreeMap<Date, HashMap<Tick, Float>> getData() {
		return this.data;
	}

	public void setData(TreeMap<Date, HashMap<Tick, Float>> data) {
		this.data = data;
	}

	public Float getMax() {
		return max;
	}

	public void setMax(Float max) {
		this.max = max;
	}

	public int getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(int numBuckets) {
		this.numBuckets = numBuckets;
	}

	public Notify getNotify() {
		return notify;
	}

	public void setNotify(Notify notify) {
		this.notify = notify;
	}

	public boolean isCompareTwoPanel() {
		return compareTwoPanel;
	}

	public void setCompareTwoPanel(boolean compareTwoPanel) {
		this.compareTwoPanel = compareTwoPanel;
	}

	public Float getMin() {
		return min;
	}

	public void setMin(Float min) {
		this.min = min;
	}

	public Boolean getOnlyPositive() {
		return onlyPositive;
	}

	public void setOnlyPositive(Boolean onlyPositive) {
		this.onlyPositive = onlyPositive;
	}

	public boolean isRange0to1() {
		return range0to1;
	}

	public void setRange0to1(boolean range0to1) {
		this.range0to1 = range0to1;
	}



}
