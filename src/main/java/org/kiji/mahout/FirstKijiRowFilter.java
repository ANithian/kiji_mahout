package org.kiji.mahout;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.codehaus.jackson.JsonNode;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterDeserializer;

public class FirstKijiRowFilter extends KijiRowFilter {

	public FirstKijiRowFilter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public KijiDataRequest getDataRequest() {
		// TODO Auto-generated method stub
		return KijiDataRequest.builder().build();
	}

	@Override
	protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Filter toHBaseFilter(Context arg0) throws IOException {
		// TODO Auto-generated method stub
		return new FirstKeyOnlyFilter();
	}

	@Override
	protected JsonNode toJsonNode() {
		// TODO Auto-generated method stub
		return null;
	}

}
