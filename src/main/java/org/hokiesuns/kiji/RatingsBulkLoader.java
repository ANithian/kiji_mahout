package org.hokiesuns.kiji;

import static org.kiji.mahout.KijiDataModel.RATING_COLUMN_NAME;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.util.NumberUtils;
import org.kiji.mahout.KijiDataModel;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

public class RatingsBulkLoader extends KijiBulkImporter<LongWritable, Text> {

	@Override
	public void produce(LongWritable arg0, Text ratingLine, KijiTableContext tableContext)
			throws IOException {
		//Rating is a 4 tuple but we care about the first 3 (user, product, rating)
		//We will generate two tuples, one that is user => product:rating and another that is product => user:rating
		final String line = ratingLine.toString();
		String[] parts = line.split(",");
		//parts=>user,product,rating
		if(parts.length >= 3)
		{
			long userId = Long.parseLong(parts[0]);
			long productId = Long.parseLong(parts[1]);
			float rating = Float.parseFloat(parts[2]);
			
			EntityId userEntityId = tableContext.getEntityId(KijiDataModel.USER,userId);
			EntityId productEntityId = tableContext.getEntityId(KijiDataModel.ITEM,productId);
			
			tableContext.put(userEntityId, RATING_COLUMN_NAME, NumberUtils.long2sortableStr(productId), rating);
			tableContext.put(productEntityId, RATING_COLUMN_NAME, NumberUtils.long2sortableStr(userId), rating);
		}
	}
}
