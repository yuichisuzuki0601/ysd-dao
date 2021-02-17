package jp.co.ysd.ysd_dao.dao;

import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.support.SqlLobValue;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.CaseFormat;

import jp.co.ysd.ysd_dao.bean.Query;
import jp.co.ysd.ysd_dao.exception.UnableNarrowDownException;
import jp.co.ysd.ysd_dao.util.MapBuilder;

/**
 *
 * @author yuichi
 *
 */
@Component
public class MapDao extends BasicDao {

	private class SimpleRowMapper implements RowMapper<Map<String, Object>> {
		protected Map<String, Object> getMap(ResultSet rs) throws SQLException {
			Map<String, Object> map = new HashMap<>();
			ResultSetMetaData meta = rs.getMetaData();
			for (int i = 1; i <= meta.getColumnCount(); ++i) {
				String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, meta.getColumnLabel(i));
				map.put(key, rs.getObject(i));
			}
			return map;
		}

		@Override
		public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
			return getMap(rs);
		}
	}

	public interface MapDaoFetchProcess {
		public void process(Map<String, Object> map);
	}

	private class FetchRowMapper extends SimpleRowMapper {
		private MapDaoFetchProcess proc;

		private FetchRowMapper(MapDaoFetchProcess proc) {
			this.proc = proc;
		}

		@Override
		public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
			proc.process(getMap(rs));
			return null;
		}
	}

	private SqlParameterSource getSqlParameterSource(Map<String, Object> map) {
		MapSqlParameterSource params = new MapSqlParameterSource();
		for (Entry<String, Object> entry : map.entrySet()) {
			String propertyName = entry.getKey();
			Object value = entry.getValue();
			if (value instanceof byte[]) {
				byte[] data = (byte[]) value;
				value = new SqlLobValue(new ByteArrayInputStream(data), data.length);
				params.addValue(propertyName, value, Types.BLOB);
			} else {
				params.addValue(propertyName, value);
			}
		}
		return params;
	}

	public List<Map<String, Object>> query(Query query) {
		return query(this.nj, query);
	}

	public List<Map<String, Object>> query(NamedParameterJdbcTemplate nj, Query query) {
		return nj.query(query.getSource(), new SimpleRowMapper());
	}

	public List<Map<String, Object>> query(Query query, Map<String, Object> params) {
		return query(this.nj, query, params);
	}

	public List<Map<String, Object>> query(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> params) {
		return nj.query(query.getSource(), getSqlParameterSource(params), new SimpleRowMapper());
	}

	public Map<String, Object> queryForSingle(Query query) {
		return queryForSingle(this.nj, query);
	}

	public Map<String, Object> queryForSingle(NamedParameterJdbcTemplate nj, Query query) {
		List<Map<String, Object>> list = query(nj, query);
		if (list.isEmpty()) {
			return null;
		} else if (list.size() == 1) {
			return list.get(0);
		} else {
			throw new UnableNarrowDownException(query);
		}
	}

	public Map<String, Object> queryForSingle(Query query, Map<String, Object> params) {
		return queryForSingle(this.nj, query, params);
	}

	public Map<String, Object> queryForSingle(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> params) {
		List<Map<String, Object>> list = query(nj, query, params);
		if (list.isEmpty()) {
			return null;
		} else if (list.size() == 1) {
			return list.get(0);
		} else {
			throw new UnableNarrowDownException(query);
		}
	}

	public Map<String, Object> queryById(Query query, long id) {
		return queryById(this.nj, query, id);
	}

	public Map<String, Object> queryById(NamedParameterJdbcTemplate nj, Query query, long id) {
		return queryForSingle(nj, query, new MapBuilder("id", id).build());
	}

	public Map<String, Object> queryByIds(Query query, List<Long> ids) {
		return queryByIds(this.nj, query, ids);
	}

	public Map<String, Object> queryByIds(NamedParameterJdbcTemplate nj, Query query, List<Long> ids) {
		return queryForSingle(nj, query, new MapBuilder("ids", ids).build());
	}

	public void fetch(Query query, MapDaoFetchProcess proc) {
		fetch(this.nj, query, proc);
	}

	public void fetch(NamedParameterJdbcTemplate nj, Query query, MapDaoFetchProcess proc) {
		nj.query(query.getSource(), new FetchRowMapper(proc));
	}

	public void fetch(Query query, Map<String, Object> params, MapDaoFetchProcess proc) {
		fetch(this.nj, query, params, proc);
	}

	public void fetch(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> params, MapDaoFetchProcess proc) {
		nj.query(query.getSource(), getSqlParameterSource(params), new FetchRowMapper(proc));
	}

	public Long insert(Query query, Map<String, Object> map) {
		return insert(this.nj, query, map);
	}

	public Long insert(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> map) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		nj.update(query.getSource(), new MapSqlParameterSource(map), keyHolder, new String[] { "id" });
		Number key = keyHolder.getKey();
		return key != null ? key.longValue() : null;
	}

}
