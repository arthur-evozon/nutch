package com.evozon.mining.product.parsers;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

import static org.slf4j.LoggerFactory.getLogger;

public class ProductParserUtils {
	private static final Logger LOG = getLogger(ProductParserUtils.class);

	public static String extractFirstChildText(Document document, String selector) {
		return extractNthChildText(document, selector, 1);
	}

	public static String extractNthChildText(Document document, String selector, int nth) {
		return extractNthChildText(document.select(selector), nth);
	}

	public static String extractFirstChildText(Elements elements) {
		return extractNthChildText(elements, 1);
	}

	public static String extractNthChildText(Elements elements, int nth) {
		String ret = "";

		if (elements != null && elements.size() > 0) {
			Element element = elements.get(0);
			for (Node child : element.childNodes()) {
				ret = child.toString();
				if (child instanceof TextNode) {
					ret = ((TextNode) child).text();
				}

				if (--nth <= 0) {
					break;
				}
			}
		}

		return ret.trim();
	}

	public static String extractFirstAttrText(Document document, String attr, String selector) {
		return extractFirstAttrText(document.select(selector), attr);
	}

	public static String extractNthAttrText(Document document, String attr, String selector, int nth) {
		return extractNthAttrText(document.select(selector), attr, nth);
	}


	public static String extractFirstAttrText(Elements elements, String attr) {
		return extractNthAttrText(elements, attr, 1);
	}

	public static String extractNthAttrText(Elements elements, String attr, int nth) {
		String ret = "";

		if (elements != null && elements.size() > 0) {
			for (int i = 0; i < Math.min(elements.size(), nth + 1); i++) {
				Element element = elements.get(i);
				if (element != null && element.hasAttr(attr)) {
					ret = element.attr(attr);
					nth--;
				}
			}
		}

		return ret.trim();
	}

	public static byte[] toByteArray(double value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		return bytes;
	}

	public static double toDouble(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getDouble();
	}

	public static Double buildPrice(String priceWhole, String pricePart) {
		Double price = null;
		try {
			if (StringUtils.isBlank(pricePart)) {
				pricePart = "0";
			}

			price = Double.parseDouble(String.format("%s.%s", priceWhole, pricePart));
		} catch (NumberFormatException e) {
			LOG.debug("Could not extract price from [{}.{}]", priceWhole, pricePart);
		}

		return price;
	}

	public static String removeTrailing(String suffix, String text) {
		String result = text;
		while (result.endsWith(suffix)) {
			result = StringUtils.reverse(StringUtils.reverse(result).replaceFirst(suffix, "")).trim();
		}

		return result;
	}

	public static Map<String, Set<String>> getAllNameValuesBySelector(Document document, String metaSelectors) {
		return getAllNameValuesBySelector(document, metaSelectors, ":");
	}

	public static Map<String, Set<String>> getAllNameValuesBySelector(Document document, String metaSelectors, String keySuffix) {
		Map<String, Set<String>> nameValues = new HashMap<>();
		Elements valueElements = document.select(metaSelectors);

		if (valueElements != null) {
			for (Element valueElement : valueElements) {
				Element keyElement = valueElement.previousElementSibling();
				if (keyElement == null) {
					continue;
				}

				List<Node> keyElementText = keyElement.childNodes();

				if (keyElementText == null || keyElementText.size() != 1) {
					continue;
				}

				String key = keyElementText.get(0).toString().trim();
				key = removeTrailing(keySuffix, key);

				List<Node> valueElementText = valueElement.childNodes();

				if (valueElementText == null || valueElementText.size() != 1) {
					continue;
				}

				String value = valueElementText.get(0).toString().trim();
				if (StringUtils.isBlank(value)) {
					continue;
				}

				if (!nameValues.containsKey(key)) {
					nameValues.put(key, new HashSet<String>());
				}

				Set<String> values = nameValues.get(key);
				values.add(value);
			}
		}

		return nameValues;
	}

	public static Map<String, Set<String>> getAllNameValuesWithSelectorPattern(Document document, String metaSelector, String...
			nestedSelectors) {
		Elements valueElements = document.select(metaSelector);
		Deque<String> selectorQueue = new LinkedList<>();

		for (String selector : nestedSelectors) {
			selectorQueue.add(selector);
		}
		Map<String, Set<String>> nameValues = new HashMap<>();
		for (Element el : valueElements) {
			nameValues.putAll(buildNestedNameValues(el, selectorQueue));
		}

		return nameValues;
	}

	public static Map<String, Set<String>> buildNestedNameValues(Element root, Deque<String> selectors) {
		if (selectors.size() <= 0) {
			return Collections.emptyMap();
		}

		Map<String, Set<String>> nameValues = new HashMap<>();
		String currentSelector = selectors.removeFirst();

		for (Element currentElement : root.select(currentSelector)) {
			if (selectors.size() > 0) {
				nameValues.putAll(buildNestedNameValues(currentElement, selectors));
			} else {
				Elements leaf = root.select(currentSelector);
				if (leaf.size() >= 2) {
					Element valueElement = leaf.last();
					Element keyElement = valueElement.previousElementSibling();

					List<Node> keyElementText = keyElement.childNodes();
					if (keyElementText == null || keyElementText.size() != 1) {
						return nameValues;
					}

					String key = keyElementText.get(0).toString().trim();
					if (StringUtils.isBlank(key)) {
						return nameValues;
					}

					List<Node> valueElementText = valueElement.childNodes();
					if (valueElementText == null || valueElementText.size() != 1) {
						return nameValues;
					}

					String value = valueElementText.get(0).toString().trim();

					if (!nameValues.containsKey(key)) {
						nameValues.put(key, new HashSet<String>());
					}

					Set<String> values = nameValues.get(key);
					values.add(value);

					selectors.addFirst(currentSelector);
					return nameValues;
				}
			}
		}

		selectors.addFirst(currentSelector);
		return nameValues;
	}

	public static String buildNameValuesString(Map<String, Set<String>> nameValues) {
		return buildNameValuesString(nameValues, "\n", ":", ",");
	}

	public static String buildNameValuesString(Map<String, Set<String>> nameValues, String nameValueLineSeparator, String
			nameValueSeparator, String valueSeparator) {
		if (nameValues == null || nameValues.isEmpty()) {
			return "";
		}

		StringBuilder nameValuesString = new StringBuilder("");
		for (Iterator<String> nameItr = nameValues.keySet().iterator(); nameItr.hasNext(); ) {
			String name = nameItr.next();

			if (StringUtils.isBlank(name)) {
				continue;
			}

			nameValuesString.append(name).append(nameValueSeparator);

			Set<String> values = nameValues.get(name);
			for (Iterator<String> valueItr = values.iterator(); valueItr.hasNext(); ) {
				String value = valueItr.next();
				if (StringUtils.isBlank(value)) {
					continue;
				}

				nameValuesString.append(value);
				if (valueItr.hasNext()) {
					nameValuesString.append(valueSeparator);
				}
			}

			if (nameItr.hasNext()) {
				nameValuesString.append(nameValueLineSeparator);
			}
		}

		return nameValuesString.toString();
	}
}
