package jp.co.ysd.ysd_dao.util;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author yuichi
 *
 */
public class MapBuilder {

	private Map<String, Object> map = new HashMap<>();

	public MapBuilder() {
	}

	public MapBuilder(String key, Object value) {
		map.put(key, value);
	}

	public MapBuilder put(String key, Object value) {
		map.put(key, value);
		return this;
	}

	public Map<String, Object> build() {
		return map;
	}

}