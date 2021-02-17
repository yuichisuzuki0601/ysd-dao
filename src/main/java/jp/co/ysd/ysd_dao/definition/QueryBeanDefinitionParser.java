package jp.co.ysd.ysd_dao.definition;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import jp.co.ysd.ysd_dao.bean.Query;

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
		bean.addPropertyValue("source", DomUtils.getChildElementByTagName(element, "source").getTextContent());
	}

}
