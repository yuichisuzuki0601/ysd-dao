package jp.co.ysd.ysd_dao.definition;

import static jp.co.ysd.ysd_util.stream.StreamWrapperFactory.*;

import java.util.UUID;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import jp.co.ysd.ysd_dao.bean.Query;
import jp.co.ysd.ysd_dao.bean.Query.SourceContent;

/**
 *
 * @author yuichi
 *
 */
public class QueryBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return Query.class;
	}

	@Override
	protected void doParse(Element element, BeanDefinitionBuilder bean) {
		bean.addPropertyValue("id", element.getAttribute("id"));
		bean.addPropertyValue("sources", stream(DomUtils.getChildElementsByTagName(element, "source")).end(s -> {
			String name = s.getAttribute("name");
			return StringUtils.hasText(name) ? name : UUID.randomUUID().toString();
		}, s -> {
			String _defaultUse = s.getAttribute("defaultUse");
			boolean defaultUse = StringUtils.hasText(_defaultUse) ? Boolean.valueOf(_defaultUse) : true;
			return new SourceContent(defaultUse, s.getTextContent());
		}));
	}

}
