package org.hokiesuns.mahoutfun;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.kiji.mahout.KijiDataModel;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.filter.FormattedEntityIdRowFilter;
import org.kiji.schema.filter.KijiRowFilter;

public class MahoutSandbox {

	public static void main(String[] args) throws Exception {
		
//		testKijiModel();
//		compareModels();
		String sTest = longToSortableString(1001l);
		System.out.println(sortableLongStringToLong(sTest));
		long[] myLongs = {1,2,3,4,5,6,7,8,9,10,678,689};
		String[] sRawLongs = new String[myLongs.length];
		String[] sSortableLongs = new String[myLongs.length];
		for(int i=0; i < myLongs.length; i++) {
			sRawLongs[i] = Long.toString(myLongs[i]);
			sSortableLongs[i] = longToSortableString(myLongs[i]);
		}
		Arrays.sort(sRawLongs);
		Arrays.sort(sSortableLongs);
		for(String s:sRawLongs) {
			System.out.print(s + " ");
		}
		System.out.println();
		for(String s:sSortableLongs) {
			System.out.print(sortableLongStringToLong(s) + " ");
		}
	}
	
	public static String longToSortableString(long val) {
		byte[] tempBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE)
	              .putLong(val).array();
	    tempBytes[0] = (byte)((int) tempBytes[0] ^ (int)Byte.MIN_VALUE);
	    return new String(tempBytes);		
	}
	
	public static long sortableLongStringToLong(String val) {
		byte[] tempBytes = val.getBytes();
		tempBytes[0] = (byte)((int) tempBytes[0] ^ (int)Byte.MIN_VALUE);
		return ByteBuffer.wrap(tempBytes).getLong();
	}
	
	public static void compareModels() throws Exception {
		final KijiURI kijiURI = KijiURI.newBuilder("kiji://localhost:2181").withInstanceName("default").build();
		final Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("product_ratings");
		
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		KijiRowFilter userIdFilter = new FormattedEntityIdRowFilter(format, "user");
		KijiRowFilter itemIdFilter = new FormattedEntityIdRowFilter(format, "product");
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		
		KijiDataModel model = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
		DataModel fileModel = new FileDataModel(new File("input/ch2/gl_100k.csv"));
		LongPrimitiveIterator it = fileModel.getItemIDs();
		System.out.println("Printing items..");
		while(it.hasNext()) {
			System.out.println(it.nextLong());
		}
		System.out.println("Using Kiji:");
		it = model.getItemIDs();
		while(it.hasNext()) {
			System.out.println(it.nextLong());
		}
	}
	public static void testKijiModel() throws Exception {
		final KijiURI kijiURI = KijiURI.newBuilder("kiji://localhost:2181").withInstanceName("default").build();
		final Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("product_ratings");
		
		
		RowKeyFormat2 format = (RowKeyFormat2)table.getLayout().getDesc().getKeysFormat();
		KijiRowFilter userIdFilter = new FormattedEntityIdRowFilter(format, "user");
		KijiRowFilter itemIdFilter = new FormattedEntityIdRowFilter(format, "product");
		KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
		dataRequestBuilder.addColumns(KijiDataModel.getColumnsDef());
		
		KijiDataModel model = new KijiDataModel(table, dataRequestBuilder.build(), userIdFilter, itemIdFilter);
		
		LongPrimitiveIterator it = model.getUserIDs();
		System.out.println("All user ids");
		while(it.hasNext())
		{
			System.out.println(it.nextLong());
		}
		
		System.out.println("Items for user 299");
		FastIDSet items = model.getItemIDsFromUser(299);
		Iterator<Long> it2 = items.iterator();
		while(it2.hasNext()) {
			System.out.println(it2.next());
		}
		
		System.out.println("Number of items is " + model.getNumItems());
		System.out.println("Number of users is " + model.getNumUsers());
		System.out.println("Number of users interested in item 229 " + model.getNumUsersWithPreferenceFor(229));
		System.out.println("Number of users interested in item 144 " + model.getNumUsersWithPreferenceFor(144));
		System.out.println("Number of users interested in item 229 AND 144 " + model.getNumUsersWithPreferenceFor(229,144));
	}
	
}
