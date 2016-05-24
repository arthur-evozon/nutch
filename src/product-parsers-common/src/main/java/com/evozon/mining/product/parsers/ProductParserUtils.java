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

	public static String extractText(Document document, String selector) {
		return extractText(document, selector, 1);
	}

	public static String extractText(Document document, String selector, int nth) {
		return extractText(document.select(selector), nth);
	}

	public static String extractText(Elements elements) {
		return extractText(elements, 1);
	}

	public static String extractText(Elements elements, int nth) {
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
