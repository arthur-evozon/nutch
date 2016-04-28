package com.evozon.mining.product.parsers.emag;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EmagProductParser extends DefaultProductParser implements ProductParser {

	private static final Logger LOG = LoggerFactory.getLogger(EmagProductParser.class);

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {

		StringBuilder metaLines = new StringBuilder();

		Elements valueElements = document.select(metaSelectors);

		if (valueElements != null) {
			for (Element valueElement : valueElements) {
				Element keyElement = valueElement.previousElementSibling();
				List<Node> keyElementText = keyElement.childNodes();

				if (keyElementText == null || keyElementText.size() != 1) {
					continue;
				}

				String key = keyElementText.get(0).toString().trim();
				key = removeTrailing(":", key);

				List<Node> valueElementText = valueElement.childNodes();

				if (valueElementText == null || valueElementText.size() != 1) {
					continue;
				}
				String value = valueElementText.get(0).toString().trim();

				if (metaLines.length() > 0) {
					metaLines.append("\n");
				}
				metaLines.append(key + ":" + value);
			}
		}

		return metaLines.toString();
	}
}