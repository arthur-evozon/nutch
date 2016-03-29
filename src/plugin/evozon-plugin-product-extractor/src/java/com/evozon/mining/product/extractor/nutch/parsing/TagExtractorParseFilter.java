package com.evozon.mining.product.extractor.nutch.parsing;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.w3c.dom.DocumentFragment;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags
 * from the body of the document and sets it into the ParseResult object.
 */
public class TagExtractorParseFilter implements ParseFilter {

	public static final String PRODUCT_KEY = "product-name";
	public static final String PRODUCT_DETAILS = "product-details";

	private static final Logger LOG = Logger.getLogger(TagExtractorParseFilter.class);

	private static final List<String> htmlMimeTypes = Arrays.asList(new String[]{"text/html", "application/xhtml+xml"});

	// Configuration
	private Configuration configuration;
	private String defaultEncoding;

	private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

	static {
		FIELDS.add(WebPage.Field.METADATA);
	}


	private Configuration conf;

	/**
	 * We use regular expressions to parse out the Labels section from
	 * the section snippet shown below:
	 * <pre>
	 * Labels:
	 * <a href='http://sujitpal.blogspot.com/search/label/ror' rel='tag'>ror</a>,
	 * ...
	 * </span>
	 * </pre>
	 * Accumulate the tag values into a List, then stuff the list into the
	 * parseResult with a well-known key (exposed as a public static variable
	 * here, so the indexing filter can pick it up from here).
	 */

	@Override
	public Parse filter(String url, WebPage page, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {
		LOG.debug("Parsing URL: " + url);

		Document document = Jsoup.parse(new String(page.getContent().array()));

		StringBuilder productDefinition = new StringBuilder();
		Elements productElement = document.select("h2[class=sectionname]");
		if (productElement != null) {
			for (Element element : productElement) {
				for (Node child : element.childNodes()) {
					if (productDefinition.length() > 0) {
						productDefinition.append("\n");
					}

					productDefinition.append(child.toString().trim());
				}
			}
		}

		StringBuilder productDetails = new StringBuilder();
		Elements productDetailsElement = document.select("p[class=product_details]");
		if (productDetailsElement != null) {
			for (Element element : productDetailsElement) {
				for (Node child : element.childNodes()) {

					String[] details = child.toString().split(",");
					for (String detail : details) {
						if (productDetails.length() > 0) {
							productDetails.append("\n");
						}

						productDetails.append(detail.trim());
					}
				}
			}
		}

		Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
		metadata.put(PRODUCT_KEY, ByteBuffer.wrap(productDefinition.toString().getBytes()));
		metadata.put(PRODUCT_DETAILS, ByteBuffer.wrap(productDetails.toString().getBytes()));

		return parse;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}
}