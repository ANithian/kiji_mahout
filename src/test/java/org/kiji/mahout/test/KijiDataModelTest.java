package org.kiji.mahout.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.solr.util.NumberUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.mahout.FirstKijiRowFilter;
import org.kiji.mahout.KijiDataModel;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.filter.Filters;
import org.kiji.schema.filter.FormattedEntityIdRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

import com.google.common.collect.Lists;

//Adapted from the FileModel test in Mahout.
public class KijiDataModelTest {
	
	private Kiji mFakeKiji;
	private DataModel mDataModel;
	private KijiRowFilter userIdFilter;
	private KijiRowFilter itemIdFilter;
	
	  private static final String[] DATA = {
	      "123,456,0.1",
	      "123,789,0.6",
	      "123,654,0.7",
	      "234,123,0.5",
	      "234,234,1.0",
	      "234,999,0.9",
	      "345,789,0.6",
	      "345,654,0.7",
	      "345,123,1.0",
	      "345,234,0.5",
	      "345,999,0.5",
	      "456,456,0.1",
	      "456,789,0.5",
	      "456,654,0.0",
	      "456,999,0.2",};
	/**
	 * Opens a new unique test Kiji instance, creating it if necessary.
	 * 
	 * Each call to this method returns a fresh new Kiji instance. All generated
	 * Kiji instances are automatically cleaned up by KijiClientTest.
	 * 
	 * @return a fresh new Kiji instance.
	 * @throws Exception
	 *             on error.
	 */
	public Kiji createTestKiji() throws Exception {
		final String hbaseAddress = String
				.format(".fake.%s-%d", "kiji_rest", 0);
		final KijiURI uri = KijiURI.newBuilder(
				String.format("kiji://%s/%s", hbaseAddress, "default")).build();
		final Kiji kiji = Kiji.Factory.open(uri);

		return kiji;
	}

	/**
	 * {@inheritDoc}
	 */
	@Before
	public void setupTable() throws Exception {
		InstanceBuilder builder = new InstanceBuilder("default");
		mFakeKiji = builder.build();
		TableLayoutDesc desc = KijiTableLayouts.getLayout("org/kiji/mahout/test/product_ratings_test.json");

	    mFakeKiji.createTable(desc);
		
		KijiTable table = mFakeKiji.openTable("product_ratings_test");
		KijiTableWriter writer = table.openTableWriter();
		for(String s:DATA) {
			String[] row = s.split(",");
			long userId = Long.parseLong(row[0]);
			long itemId = Long.parseLong(row[1]);
			float rating = Float.parseFloat(row[2]);
			EntityId userEid = table.getEntityId(KijiDataModel.USER, userId);
			EntityId productEid = table.getEntityId(KijiDataModel.ITEM, itemId);
			writer.put(userEid, KijiDataModel.RATING_COLUMN_NAME, NumberUtils.long2sortableStr(itemId), rating);
			writer.put(productEid, KijiDataModel.RATING_COLUMN_NAME, NumberUtils.long2sortableStr(userId), rating);
		}
		writer.close();
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		userIdFilter = new FormattedEntityIdRowFilter(format, KijiDataModel.USER) ;
		itemIdFilter = new FormattedEntityIdRowFilter(format, KijiDataModel.ITEM) ;
		
		mDataModel = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
	}
	
	@Test
	public void testEntityIds() throws Exception {
		KijiTable table = mFakeKiji.openTable("product_ratings_test");
		EntityId eid1 = table.getEntityId(KijiDataModel.USER, 678l);
		EntityId eid2 = table.getEntityId(KijiDataModel.USER, 689l);
		
		KijiTableWriter writer = table.openTableWriter();
		writer.put(eid1, KijiDataModel.RATING_COLUMN_NAME, "1234", 1.2f);
		writer.put(eid2, KijiDataModel.RATING_COLUMN_NAME, "1235", 4.2f);
		writer.close();
		
		KijiTableReader reader = table.openTableReader();
		KijiDataRequest request = KijiDataRequest.builder().addColumns(ColumnsDef.create().add(new KijiColumnName(KijiDataModel.RATING_COLUMN_NAME) )).build();
		KijiScannerOptions options = new KijiScannerOptions();
		options.setKijiRowFilter(Filters.and(new FirstKijiRowFilter(),userIdFilter));
		
		KijiRowScanner scanner = reader.getScanner(request,options);
		Iterator<KijiRowData> it = scanner.iterator();
		
		List<Long> expectedEntityIds = Lists.newArrayList();
		expectedEntityIds.add(123l);
		expectedEntityIds.add(234l);
		expectedEntityIds.add(345l);
		expectedEntityIds.add(456l);
		expectedEntityIds.add(678l);
		expectedEntityIds.add(689l);
		
		List<Long> actualEntityIds = Lists.newArrayList();
		while(it.hasNext()) {
			KijiRowData row1 = it.next();
			actualEntityIds.add((Long)row1.getEntityId().getComponentByIndex(1));
		}
		assertEquals(expectedEntityIds, actualEntityIds);
	}
	
	@Test
	public void testFile() throws Exception {
	  UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(mDataModel);
	  UserNeighborhood neighborhood = new NearestNUserNeighborhood(3, userSimilarity, mDataModel);
	  Recommender recommender = new GenericUserBasedRecommender(mDataModel, neighborhood, userSimilarity);
	  assertEquals(1, recommender.recommend(123, 3).size());
	  assertEquals(0, recommender.recommend(234, 3).size());
	  assertEquals(1, recommender.recommend(345, 3).size());
	}


	  @Test(expected = NoSuchElementException.class)
	  public void testGetItems() throws Exception {
	    LongPrimitiveIterator it = mDataModel.getItemIDs();
	    assertNotNull(it);
	    assertTrue(it.hasNext());
	    assertEquals(123, it.nextLong());
	    assertTrue(it.hasNext());
	    assertEquals(234, it.nextLong());
	    assertTrue(it.hasNext());
	    assertEquals(456, it.nextLong());
	    assertTrue(it.hasNext());
	    assertEquals(654, it.nextLong());
	    assertTrue(it.hasNext());
	    assertEquals(789, it.nextLong());
	    assertTrue(it.hasNext());
	    assertEquals(999, it.nextLong());
	    assertFalse(it.hasNext());
	    it.next();
	  }

	  @Test
	  public void testPreferencesForItem() throws Exception {
	    PreferenceArray prefs = mDataModel.getPreferencesForItem(456);
	    assertNotNull(prefs);
	    Preference pref1 = prefs.get(0);
	    assertEquals(123, pref1.getUserID());
	    assertEquals(456, pref1.getItemID());
	    Preference pref2 = prefs.get(1);
	    assertEquals(456, pref2.getUserID());
	    assertEquals(456, pref2.getItemID());
	    assertEquals(2, prefs.length());
	  }

	  @Test
	  public void testGetNumUsers() throws Exception {
	    assertEquals(4, mDataModel.getNumUsers());
	  }

	  @Test
	  public void testNumUsersPreferring() throws Exception {
	    assertEquals(2, mDataModel.getNumUsersWithPreferenceFor(456));
	    assertEquals(0, mDataModel.getNumUsersWithPreferenceFor(111));
	    assertEquals(0, mDataModel.getNumUsersWithPreferenceFor(111, 456));
	    assertEquals(2, mDataModel.getNumUsersWithPreferenceFor(123, 234));
	  }
}
