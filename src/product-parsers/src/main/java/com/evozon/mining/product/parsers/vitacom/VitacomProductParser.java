package com.evozon.mining.product.parsers.vitacom;

import com.evozon.mining.product.parsers.DefaultProductParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VitacomProductParser extends DefaultProductParser implements com.evozon.mining.product.parsers.ProductParser {

	private static final Logger LOG = LoggerFactory.getLogger(VitacomProductParser.class);


	protected Double parseProductPrice(String url, WebPage page, Document document, String priceWholeSelector, String pricePartSelector) {
		String extractedPriceData = extractText(document.getElementsByClass(priceWholeSelector));
		if (StringUtils.isBlank(extractedPriceData)) {
			return null;
		}

		String[] priceData = extractedPriceData.split(" ");
		if (priceData == null || priceData.length < 2) {
			return null;
		}

		String[] priceFigures = priceData[0].split(",");
		if (priceFigures == null || priceFigures.length != 2) {
			return null;
		}

		String priceWhole = priceFigures[0];
		String pricePart = priceFigures[1];

		if (StringUtils.isBlank(priceWhole)) {
			return null;
		}

		return buildPrice(priceWhole, pricePart);
	}

	@Override
	protected String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		String extractedPriceData = extractText(document.getElementsByClass(selector));
		if (StringUtils.isBlank(extractedPriceData)) {
			return null;
		}

		String[] priceData = extractedPriceData.split(" ");
		if (priceData == null || priceData.length < 2) {
			return null;
		}

		return priceData[1];
	}

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		StringBuilder productDetails = new StringBuilder();

		Elements productDetailsElement = document.select(metaSelectors.trim());
		if (productDetailsElement != null) {
			for (Element element : productDetailsElement) {
				for (Node child : element.childNodes()) {
					String details = child.toString();
					if (details.startsWith("<")) {
						continue;
					}

					if (productDetails.length() > 0) {
						productDetails.append("\n");
					}

					productDetails.append(details.trim().replaceAll("&gt;",""));
				}
			}
		}

		return productDetails.toString();
	}
}