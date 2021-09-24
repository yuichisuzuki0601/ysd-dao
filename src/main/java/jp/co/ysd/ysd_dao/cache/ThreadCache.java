package jp.co.ysd.ysd_dao.cache;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author yuichi
 *
 */
public final class ThreadCache {

	private static Map<Thread, Map<String, Object>> threadMaps = new HashMap<>();

	public static void invalidateThreadMap() {
		threadMaps.keySet().forEach(thread -> {
			if (!thread.isAlive()) {
				threadMaps.remove(thread);
			}
		});
	}

	public static Object get(String key) {
		invalidateThreadMap();
		Map<String, Object> map = threadMaps.get(Thread.currentThread());
		if (map == null) {
			return null;
		} else {
			return map.get(key);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T getOrDefault(String key, T defaultValue) {
		T value = (T) get(key);
		return value != null ? value : defaultValue;
	}

	public static void put(String key, Object value) {
		invalidateThreadMap();
		Thread thread = Thread.currentThread();
		Map<String, Object> map = threadMaps.get(thread);
		if (map == null) {
			map = new HashMap<>();
		}
		map.put(key, value);
		threadMaps.put(thread, map);
	}

}
