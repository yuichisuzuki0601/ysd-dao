package jp.co.ysd.ysd_dao.exception;

import jp.co.ysd.ysd_dao.bean.Query;

/**
 *
 * @author yuichi
 *
 */
public class UnableNarrowDownException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private static final String MESSAGE = "Cannot narrow down data with query.";

	public UnableNarrowDownException(Query query) {
		super(MESSAGE + ":" + query);
	}

}
