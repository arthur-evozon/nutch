package com.evozon.mining.product.extractor.nutch;

import com.evozon.mining.product.parsers.ProductParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags
 * from the body of the document and sets it into the ParseResult object.
 */
public class ProductExtractorParseFilter implements ParseFilter {
	private static final Logger LOG = LoggerFactory.getLogger(ProductExtractorParseFilter.class);

	static final Map<String, ProductParser> PARSER_MAP = new HashMap<>();

	private static final List<String> htmlMimeTypes = Arrays.asList(new String[]{"text/html", "application/xhtml+xml"});

	// Configuration
	private Configuration configuration;
	private String defaultEncoding;


	private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

	static {
		FIELDS.add(WebPage.Field.CONTENT);

		try {
			Properties p = new Properties();
			p.load(ProductExtractorParseFilter.class.getResourceAsStream("product-parser.properties"));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				if (StringUtils.isBlank(key) || StringUtils.isBlank(p.getProperty(key))) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				String host = key.toLowerCase().trim();
				String implementation = p.getProperty(key).trim();
				Class c = Class.forName(implementation);
				ProductParser productParser = (ProductParser) c.newInstance();

				productParser.initializeParser();

				PARSER_MAP.put(host, productParser);
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(e.toString());
			}
		}
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
		String host = null;
		try {
			URL u = new URL(url);
			host = u.getHost();
		} catch (MalformedURLException e) {
			return parse;
		}

		LOG.trace("Loading parsers for {}", host);

		ProductParser productParser = PARSER_MAP.get( host.trim().toLowerCase() );
		if( productParser != null ) {
			LOG.trace("Found parsers for {}: {}", host, productParser);
			productParser.parse(url, page);
		}

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