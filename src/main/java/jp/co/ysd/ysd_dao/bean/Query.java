package jp.co.ysd.ysd_dao.bean;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author yuichi
 *
 */
public class Query {

	private String source;

	public Query() {
	}

	public Query(String source) {
		setSource(source);
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source.replaceAll("[\\r|\\n|\\\t]", " ").trim().replaceAll(" {2,}", " ");
	}

	public Query replace(String key, String value) {
		Map<String, String> map = new HashMap<>();
		map.put(key, value);
		return replace(map);
	}

	public Query replace(Map<String, String> replaces) {
		String result = source;
		for (Entry<String, String> e : replaces.entrySet()) {
			result = result.replace(e.getKey(), e.getValue());
		}
		return new Query(result);
	}

	@Override
	public String toString() {
		return source;
	}

}
