package tlfdetail.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class TLFDetailSearchFieldList {
	private final List<TLFDetailSearchField> list;

	public TLFDetailSearchFieldList() {
		list = new ArrayList<>(34);
	}

	void addFilter(String name, int startOffset, int length) {
		list.add(new TLFDetailSearchField(name, startOffset, length));
	}

	public void setupFilters(Configuration conf) {
		for(TLFDetailSearchField current : list) {
			String currentValue = conf.get(current.getName()); 
			if(currentValue != null)
				current.enable(currentValue);
		}
	}
	
	boolean holdsFor(String line) {
		boolean holds = true;
		for(TLFDetailSearchField current : list) {
			holds = holds && current.holdsFor(line);
		}
		return holds;
	}
	
	void clear() {
		list.clear();
	}
}
