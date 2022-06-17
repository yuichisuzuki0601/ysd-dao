package jp.co.ysd.ysd_dao.bean;

import static jp.co.ysd.ysd_util.stream.StreamWrapperFactory.stream;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.UUID;

/**
 *
 * @author yuichi
 *
 */
public class Query {

	private String id;
	private Map<String, String> sources = new LinkedHashMap<>();

	public Query() {
	}

	public Query(String source) {
		sources.put(UUID.randomUUID().toString(), source);
	}

	public Query(Map<String, String> sources) {
		setSources(sources);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSource() {
		return String.join(" ", sources.values());
	}

	public void setSources(Map<String, String> sources) {
		for (Entry<String, String> e : sources.entrySet()) {
			e.setValue(e.getValue().replaceAll("[\\r|\\n|\\\t]", " ").trim().replaceAll(" {2,}", " "));
		}
		this.sources = sources;
	}

	public Query replace(String key, String value) {
		Map<String, String> map = new HashMap<>();
		map.put(key, value);
		return replace(map);
	}

	public Query replace(Map<String, String> replaces) {
		Map<String, String> results = new LinkedHashMap<>();
		for (Entry<String, String> source : sources.entrySet()) {
			String result = source.getValue();
			for (Entry<String, String> e : replaces.entrySet()) {
				result = result.replace(e.getKey(), e.getValue());
			}
			results.put(source.getKey(), result);
		}
		return new Query(results);
	}

	public Query iterated(String name, int count) {
		Map<String, String> newSources = new LinkedHashMap<>();
		for (Entry<String, String> source : sources.entrySet()) {
			String key = source.getKey();
			String orgSource = source.getValue();
			if (count >= 1 && key.equals(name)) {
				StringJoiner newSource = new StringJoiner(" ");
				for (int i = 1; i <= count; ++i) {
					newSource.add(orgSource.replaceAll("\\B:[^\\s]+\\b", "$0" + i));
				}
				newSources.put(key, newSource.toString());
			} else {
				newSources.put(key, orgSource);
			}
		}
		return new Query(newSources);
	}

	public Query excluded(String... names) {
		Map<String, String> newSources = new LinkedHashMap<>(sources);
		stream(names).forEach(name -> newSources.remove(name));
		return new Query(newSources);
	}

	@Override
	public String toString() {
		return getSource();
	}

}
