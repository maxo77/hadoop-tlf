package tlfdetail.query;

public class TLFDetailSearchField {
	private final String name;
	private final int startOffset;
	private final int endOffset;
	private boolean filter;
	private String value;
	
	public TLFDetailSearchField(String name, int startOffset, int length) {
		super();
		this.name = name;
		this.startOffset = startOffset;
		this.endOffset = length;
		this.filter = false;
		this.value = "";
	}
	
	void enable(String value) {
		this.value = value;
		filter = true;
	}
	
	boolean holdsFor(String line) {
		if (filter) {
			String fieldValue = line.substring(startOffset, endOffset).trim();
			return fieldValue.equals(value);
		} else
			return true;
	}
	
	public String getName() {
		return name;
	}
}
