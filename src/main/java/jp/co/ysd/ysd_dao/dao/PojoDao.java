package jp.co.ysd.ysd_dao.dao;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.support.SqlLobValue;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.base.CaseFormat;

import jp.co.ysd.ysd_dao.annotation.Snapshot;
import jp.co.ysd.ysd_dao.bean.Query;
import jp.co.ysd.ysd_dao.exception.OverUpdateException;
import jp.co.ysd.ysd_dao.exception.UnableNarrowDownException;
import jp.co.ysd.ysd_util.map.MapBuilder;

/**
 *
 * @author yuichi
 *
 */
@Component
public class PojoDao extends BasicDao {

	private static final ObjectMapper SNAPSHOT_MAPPER = new ObjectMapper();
	static {
		SNAPSHOT_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
	}

	private Logger l = LoggerFactory.getLogger(getClass());

	private class SimpleRowMapper<T> implements RowMapper<T> {
		private Class<T> clazz;

		private SimpleRowMapper(Class<T> clazz) {
			this.clazz = clazz;
		}

		protected T getPojo(ResultSet rs) {
			try {
				T obj = clazz.newInstance();
				List<Field> fields = new ArrayList<>();
				Class<?> i = clazz;
				while (i != null && i != Object.class) {
					Collections.addAll(fields, i.getDeclaredFields());
					i = i.getSuperclass();
				}
				for (Field field : fields) {
					String propertyName = field.getName();
					if ("$jacocoData".equals(propertyName)) {
						continue;
					}
					try {
						field.setAccessible(true);
						String k_e_y = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, propertyName);
						Object value = rs.getObject(k_e_y);
						if (field.getAnnotation(Snapshot.class) != null && value != null) {
							value = SNAPSHOT_MAPPER.readValue(value.toString(), field.getType());
						}
						field.set(obj, value);
					} catch (SQLException e) {
						// pojo側にプロパティはあるがselectの結果に含まれていない場合はsetせずに返す
						if (!"S1093".equals(e.getSQLState()) && !"S0022".equals(e.getSQLState())) {
							throw new IOException(e);
						}
					}
				}
				return obj;
			} catch (IOException | InstantiationException | IllegalAccessException e) {
				l.error("An error has occurred.", e);
			}
			return null;
		}

		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			return getPojo(rs);
		}
	}

	public interface PojoDaoFetchProcess<T> {
		public void process(T pojo);
	}

	private class FetchRowMapper<T> extends SimpleRowMapper<T> {
		private PojoDaoFetchProcess<T> proc;

		private FetchRowMapper(Class<T> clazz, PojoDaoFetchProcess<T> proc) {
			super(clazz);
			this.proc = proc;
		}

		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			proc.process(getPojo(rs));
			return null;
		}

	}

	private SqlParameterSource getSqlParameterSource(Object obj) {
		MapSqlParameterSource params = new MapSqlParameterSource();
		Class<?> clazz = obj.getClass();
		for (Field field : clazz.getDeclaredFields()) {
			String propertyName = field.getName();
			if ("$jacocoData".equals(propertyName)) {
				continue;
			}
			try {
				field.setAccessible(true);
				Object value = field.get(obj);
				if (value instanceof byte[]) {
					byte[] data = (byte[]) value;
					value = new SqlLobValue(new ByteArrayInputStream(data), data.length);
					params.addValue(propertyName, value, Types.BLOB);
				} else {
					params.addValue(propertyName, value);
				}
			} catch (IllegalAccessException e) {
				l.error("An error has occurred.", e);
			}
		}
		return params;
	}

	public <T> List<T> query(Class<T> clazz, Query query) {
		return query(this.nj, clazz, query);
	}

	public <T> List<T> query(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query) {
		return nj.query(query.getSource(), new SimpleRowMapper<T>(clazz));
	}

	public <T> List<T> query(Class<T> clazz, Query query, Map<String, Object> params) {
		return query(this.nj, clazz, query, params);
	}

	public <T> List<T> query(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query, Map<String, Object> params) {
		return nj.query(query.getSource(), new MapSqlParameterSource(params), new SimpleRowMapper<T>(clazz));
	}

	public <T> T queryForSingle(Class<T> clazz, Query query) {
		return queryForSingle(this.nj, clazz, query);
	}

	public <T> T queryForSingle(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query) {
		List<T> list = query(nj, clazz, query);
		if (list.isEmpty()) {
			return null;
		} else if (list.size() == 1) {
			return list.get(0);
		} else {
			throw new UnableNarrowDownException(query);
		}
	}

	public <T> T queryForSingle(Class<T> clazz, Query query, Map<String, Object> params) {
		return queryForSingle(this.nj, clazz, query, params);
	}

	public <T> T queryForSingle(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query,
			Map<String, Object> params) {
		List<T> list = query(nj, clazz, query, params);
		if (list.isEmpty()) {
			return null;
		} else if (list.size() == 1) {
			return list.get(0);
		} else {
			throw new UnableNarrowDownException(query);
		}
	}

	public <T> T queryById(Class<T> clazz, Query query, long id) {
		return queryById(this.nj, clazz, query, id);
	}

	public <T> T queryById(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query, long id) {
		return queryForSingle(nj, clazz, query, new MapBuilder("id", id).build());
	}

	public <T> List<T> queryByIds(Class<T> clazz, Query query, List<Long> ids) {
		return queryByIds(this.nj, clazz, query, ids);
	}

	public <T> List<T> queryByIds(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query, List<Long> ids) {
		return query(nj, clazz, query, new MapBuilder("ids", ids).build());
	}

	public <T> void fetch(Class<T> clazz, Query query, PojoDaoFetchProcess<T> proc) {
		fetch(this.nj, clazz, query, proc);
	}

	public <T> void fetch(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query, PojoDaoFetchProcess<T> proc) {
		nj.query(query.getSource(), new FetchRowMapper<T>(clazz, proc));
	}

	public <T> void fetch(Class<T> clazz, Query query, Map<String, Object> params, PojoDaoFetchProcess<T> proc) {
		fetch(this.nj, clazz, query, params, proc);
	}

	public <T> void fetch(NamedParameterJdbcTemplate nj, Class<T> clazz, Query query, Map<String, Object> params,
			PojoDaoFetchProcess<T> proc) {
		nj.query(query.getSource(), new MapSqlParameterSource(params), new FetchRowMapper<T>(clazz, proc));
	}

	public Long insert(Query query, Object obj) {
		return insert(this.nj, query, obj);
	}

	public Long insert(NamedParameterJdbcTemplate nj, Query query, Object obj) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		nj.update(query.getSource(), getSqlParameterSource(obj), keyHolder, new String[] { "id" });
		Number key = keyHolder.getKey();
		return key != null ? key.longValue() : null;
	}

	public int update(Query query, Object obj) {
		return update(this.nj, query, obj);
	}

	public int update(NamedParameterJdbcTemplate nj, Query query, Object obj) {
		return nj.update(query.getSource(), getSqlParameterSource(obj));
	}

	public <T> T updateForSingle(Query query, T obj) {
		int successCnt = update(query, obj);
		if (successCnt == 0) {
			return null;
		} else if (successCnt == 1) {
			return obj;
		} else {
			throw new OverUpdateException(query);
		}
	}

}
