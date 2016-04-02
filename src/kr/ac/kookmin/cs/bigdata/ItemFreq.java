package kr.ac.kookmin.cs.bigdata;

public class ItemFreq {
	
	private String item;
	private Integer freq;
	
	/* Constructor */
	public ItemFreq() {
		this.item = "";
		this.freq = 0;
	}
	
	/* Constructor */
	public ItemFreq(String item, int freq) {
		this.item = item;
		this.freq = freq;
	}

	/* item member's getter and setter */
	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	/* freq member's getter and setter */
	public Integer getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}
}