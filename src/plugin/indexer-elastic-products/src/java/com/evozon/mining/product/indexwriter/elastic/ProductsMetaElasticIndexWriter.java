package com.evozon.mining.product.indexwriter.elastic;

import com.evozon.mining.product.parsers.ProductParserConstants;
import org.apache.nutch.indexer.NutchDocument;

import java.util.Map;

public class ProductsMetaElasticIndexWriter extends DefaultElasticIndexWriter {
	@Override
	Map<String, Object> processField(NutchDocument doc, Map<String, Object> source, String fieldName) {
		if (doc.getFieldValues(fieldName).size() < 1 || !ProductParserConstants.PRODUCT_DETAILS.equals(fieldName)) {
			return super.processField(doc, source, fieldName);
		}

		// add it as an array: meta data will be a collection of text
		source.put(fieldName, doc.getFieldValues(fieldName));

		if (doc.getFieldValues(fieldName).size() > 1) {
			// Loop through the values to keep track of the size of this document
			for (Object value : doc.getFieldValues(fieldName)) {
				bulkLength += value.toString().length();
			}
		} else {
			bulkLength += doc.getFieldValue(fieldName).toString().length();
		}

		return source;
	}
}
