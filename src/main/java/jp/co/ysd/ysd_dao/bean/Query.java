package jp.co.ysd.ysd_dao.bean;

import static jp.co.ysd.ysd_util.stream.StreamWrapperFactory.*;

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
	private Map<String, SourceContent> sources = new LinkedHashMap<>();

	public static class SourceContent {
		private boolean use = true;
		private String source;

		public SourceContent(boolean use, String source) {
			this.use = use;
			this.source = source;
		}

		public boolean canUse() {
			return use;
		}

		public void setUse(boolean use) {
			this.use = use;
		}

		public String getSource() {
			return source;
		}

		public void setSource(String source) {
			this.source = source;
		}
	}

	public Query() {
	}

	public Query(String source) {
		addSource(source);
	}

	public Query(Map<String, SourceContent> sources) {
		setSources(sources);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSource() {
		return String.join(" ", stream(sources).filter(e -> e.getValue().canUse()).map(e -> e.getValue().getSource())
				.end());
	}

	public Query addSource(String source) {
		Map<String, SourceContent> newSources = new LinkedHashMap<>(sources);
		newSources.put(UUID.randomUUID().toString(), new SourceContent(true, source));
		return new Query(newSources);
	}

	public void setSources(Map<String, SourceContent> sources) {
		for (Entry<String, SourceContent> e : sources.entrySet()) {
			SourceContent sourceContent = e.getValue();
			sourceContent.setSource(
					sourceContent.getSource().replaceAll("[\\r|\\n|\\\t]", " ").trim().replaceAll(" {2,}", " "));
			e.setValue(sourceContent);
		}
		this.sources = sources;
	}

	public Query replace(String key, String value) {
		Map<String, String> map = new HashMap<>();
		map.put(key, value);
		return replace(map);
	}

	public Query replace(Map<String, String> replaces) {
		Map<String, SourceContent> results = new LinkedHashMap<>();
		for (Entry<String, SourceContent> e1 : sources.entrySet()) {
			SourceContent org = e1.getValue();
			SourceContent dest = new SourceContent(org.canUse(), "");
			for (Entry<String, String> e2 : replaces.entrySet()) {
				String newSource = org.getSource().replace(e2.getKey(), e2.getValue());
				dest.setSource(newSource);
			}
			results.put(e1.getKey(), dest);
		}
		return new Query(results);
	}

	public Query iterated(String name, int count) {
		Map<String, SourceContent> newSources = new LinkedHashMap<>();
		for (Entry<String, SourceContent> e : sources.entrySet()) {
			String key = e.getKey();
			SourceContent org = e.getValue();
			String orgSource = org.getSource();
			if (count >= 1 && key.equals(name)) {
				StringJoiner newSource = new StringJoiner(" ");
				for (int i = 1; i <= count; ++i) {
					newSource.add(orgSource.replaceAll("\\B:[^\\s]+\\b", "$0" + i));
				}
				newSources.put(key, new SourceContent(org.canUse(), newSource.toString()));
			} else {
				newSources.put(key, new SourceContent(org.canUse(), orgSource));
			}
		}
		return new Query(newSources);
	}

	public Query included(String... names) {
		Map<String, SourceContent> newSources = new LinkedHashMap<>(sources);
		stream(names).forEach(name -> newSources.values().forEach(s -> s.setUse(true)));
		return new Query(newSources);
	}

	public Query excluded(String... names) {
		Map<String, SourceContent> newSources = new LinkedHashMap<>(sources);
		stream(names).forEach(name -> newSources.values().forEach(s -> s.setUse(false)));
		return new Query(newSources);
	}

	@Override
	public String toString() {
		return getSource();
	}

}
