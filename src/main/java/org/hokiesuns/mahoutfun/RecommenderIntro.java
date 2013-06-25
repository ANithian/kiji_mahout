package org.hokiesuns.mahoutfun;

import java.io.File;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.kiji.mahout.KijiDataModel;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.filter.FormattedEntityIdRowFilter;
import org.kiji.schema.filter.KijiRowFilter;

public class RecommenderIntro {

	public static void main(String[] args) throws Exception {
		ch2ItemItem();
		ch2KijiItemItem();
//		ch2();
//		ch2Kiji();
//		ch2Neighborhoods();
	}
	
	public static void ch2() throws Exception 
	{
		RandomUtils.useTestSeed();
		
		DataModel model = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			
			@Override
			public Recommender buildRecommender(DataModel dataModel)
					throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, dataModel);
				
				Recommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
//				return new SlopeOneRecommender(dataModel);
				return recommender;
			}
		};
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
		System.out.println(score);
		
//		Recommender recommender = builder.buildRecommender(model);
//		LongPrimitiveIterator it = model.getUserIDs();
//		while(it.hasNext())
//		{
//			long lUser = it.nextLong();
//			System.out.print("User " + lUser + " ");
//			List<RecommendedItem> recommendations = recommender.recommend(lUser, 1);
//			for(RecommendedItem r:recommendations)
//			{
//				System.out.print(r);
//			}
//			System.out.println();
//		}
	}
	
	public static void ch2Kiji() throws Exception 
	{
		RandomUtils.useTestSeed();
		
		final KijiURI kijiURI = KijiURI.newBuilder("kiji://localhost:2181").withInstanceName("default").build();
		final Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("product_ratings");
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		KijiRowFilter userIdFilter = new FormattedEntityIdRowFilter(format, "user");
		KijiRowFilter itemIdFilter = new FormattedEntityIdRowFilter(format, "product");
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		
		KijiDataModel model = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
//		DataModel fileModel = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			
			@Override
			public Recommender buildRecommender(DataModel dataModel)
					throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, dataModel);
				Recommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
				return recommender;
			}
		};
		
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
		System.out.println(score);
		
//		double fileScore = evaluator.evaluate(builder, null, fileModel, 0.7, 1.0);
//		System.out.println(fileScore);
		
//		long lUser = 1;
//		Recommender recommender = builder.buildRecommender(model);
//		System.out.print("User " + lUser + " ");
//		List<RecommendedItem> recommendations = recommender.recommend(lUser, 1);
//		for(RecommendedItem r:recommendations)
//		{
//			System.out.print(r);
//		}
//		System.out.println();
//		
//		recommender = builder.buildRecommender(fileModel);
//		System.out.print("File User " + lUser + " ");
//		recommendations = recommender.recommend(lUser, 1);
//		for(RecommendedItem r:recommendations)
//		{
//			System.out.print(r);
//		}
//		System.out.println();		
	}	
	
	public static void ch2ItemItem() throws Exception 
	{
		RandomUtils.useTestSeed();
		
		DataModel model = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			
			@Override
			public Recommender buildRecommender(DataModel dataModel)
					throws TasteException {
				// TODO Auto-generated method stub
				ItemSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
				
				Recommender recommender = new GenericItemBasedRecommender(dataModel, similarity);
				return recommender;
			}
		};
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
		System.out.println(score);
	}
	
	public static void ch2KijiItemItem() throws Exception 
	{
		RandomUtils.useTestSeed();
		
		final KijiURI kijiURI = KijiURI.newBuilder("kiji://localhost:2181").withInstanceName("default").build();
		final Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("product_ratings");
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		KijiRowFilter userIdFilter = new FormattedEntityIdRowFilter(format, "user");
		KijiRowFilter itemIdFilter = new FormattedEntityIdRowFilter(format, "product");
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		
		KijiDataModel model = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
//		DataModel fileModel = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			
			@Override
			public Recommender buildRecommender(DataModel dataModel)
					throws TasteException {
				ItemSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
//				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, dataModel);
				Recommender recommender = new GenericItemBasedRecommender(dataModel, similarity);
				return recommender;
			}
		};
		
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
		System.out.println(score);
		
//		double fileScore = evaluator.evaluate(builder, null, fileModel, 0.7, 1.0);
//		System.out.println(fileScore);
		
//		long lUser = 1;
//		Recommender recommender = builder.buildRecommender(model);
//		System.out.print("User " + lUser + " ");
//		List<RecommendedItem> recommendations = recommender.recommend(lUser, 1);
//		for(RecommendedItem r:recommendations)
//		{
//			System.out.print(r);
//		}
//		System.out.println();
//		
//		recommender = builder.buildRecommender(fileModel);
//		System.out.print("File User " + lUser + " ");
//		recommendations = recommender.recommend(lUser, 1);
//		for(RecommendedItem r:recommendations)
//		{
//			System.out.print(r);
//		}
//		System.out.println();		
	}
	
	public static void ch2Neighborhoods() throws Exception {
		
		final KijiURI kijiURI = KijiURI.newBuilder("kiji://localhost:2181").withInstanceName("default").build();
		final Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("product_ratings");
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		KijiRowFilter userIdFilter = new FormattedEntityIdRowFilter(format, "user");
		KijiRowFilter itemIdFilter = new FormattedEntityIdRowFilter(format, "product");
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		
		KijiDataModel model = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
		System.out.println(model.getPreferencesFromUser(1));
		System.out.println(model.getPreferencesFromUser(351));
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
//		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
		
		System.out.println(similarity.userSimilarity(1, 351));
		
//		long[] similarUsers = neighborhood.getUserNeighborhood(1);
//		for(long l:similarUsers) {
//			System.out.println(l);
//		}
		System.out.println("File Based");
		DataModel fileModel = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		System.out.println(model.getPreferencesFromUser(1));
		System.out.println(fileModel.getPreferencesFromUser(351));
		similarity = new PearsonCorrelationSimilarity(fileModel);
		System.out.println(similarity.userSimilarity(1, 351));
//		neighborhood = new NearestNUserNeighborhood(2, similarity, fileModel);
//		similarUsers = neighborhood.getUserNeighborhood(1);
//		for(long l:similarUsers) {
//			System.out.println(l);
//		}		
	}
	public static void ch4() throws Exception 
	{
		RandomUtils.useTestSeed();
		
		DataModel model = new FileDataModel(new File("input/ch4/gl_10m.csv"));
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			
			@Override
			public Recommender buildRecommender(DataModel dataModel)
					throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
//				UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, similarity, dataModel);
				UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.5, similarity, dataModel);
				Recommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
				return recommender;
			}
		};
		double score = evaluator.evaluate(builder, null, model, 0.95, .05);
		System.out.println(score);		
	}	
}
