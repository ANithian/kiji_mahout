package org.kiji.mahout;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.AbstractLongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.solr.util.NumberUtils;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.filter.Filters;
import org.kiji.schema.filter.KijiRowFilter;

import com.google.common.collect.Lists;

/**
 * A proof of concept implementation of a Kiji backed data model. This interacts with a
 * Kiji table whose data is stored as follows:
 * entity_id=['user' | 'item', <user|item id>] rating:<rating_id> value <br/>
 * <br/>
 * For example, if user 1 rated item 10 with a value of 4.5, the following two entries are added
 * to the ratings table:
 * <ul>
 * <li>['user', 1] => rating:10 4.5
 * <li>['item', 10] => user:1 4.5
 * </ul>
 * <b>Note:</b> The column family is called "rating" and the qualifier is either the user or
 * item_id. To ensure
 * proper sorting semantics, the user/item id is encoded so that upon iteration, columns are
 * returned in the proper
 * lexicographical sort order. This means when reading the table through standard tools, the
 * qualifiers may look like junk.
 */
public class KijiDataModel implements DataModel {

  private static final long serialVersionUID = 7792642631457914263L;

  public static final String ITEM = "item";
  public static final String USER = "user";
  public static final String RATING_COLUMN_NAME = "rating";

  private KijiDataRequest mRequest = null;
  private KijiRowFilter mUserIdFilter = null;
  private KijiRowFilter mItemIdFilter = null;
  private KijiTable mTable = null;

  // Assumes the entity_id is [ 'user|item' , id ]
  /**
   * Wraps a KijiScanner to ensure either user or item id is returned
   * for those methods requiring a long iterator.
   *
   */
  private class KijiLongPrimitiveIterator extends AbstractLongPrimitiveIterator {

    private Iterator<KijiRowData> mRowIterator = null;
    private Long mPeekValue = null;
    private KijiRowScanner mScanner = null;

    public KijiLongPrimitiveIterator(KijiRowScanner scanner) {
      mRowIterator = scanner.iterator();
      mScanner = scanner;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = mPeekValue != null || mRowIterator.hasNext();
      if (!hasNext) {
        try {
          mScanner.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return hasNext;
    }

    @Override
    public void remove() {
      mRowIterator.remove();
    }

    @Override
    public long nextLong() {
      KijiRowData data = mRowIterator.next();
      long returnVal = 0;
      if (mPeekValue != null) {
        returnVal = mPeekValue;
        mPeekValue = null;
      } else {
        returnVal = (Long) data.getEntityId().getComponentByIndex(1);
      }
      return returnVal;
    }

    @Override
    public long peek() {
      if (mPeekValue == null && hasNext()) {
        mPeekValue = next();
      }
      return mPeekValue;
    }

    @Override
    public void skip(int n) {
      for (int i = 0; i < n && hasNext(); i++) {
        next();
      }
    }

  }

  public static final ColumnsDef getColumnsDef() {
    return ColumnsDef.create().withMaxVersions(1).add(new KijiColumnName(RATING_COLUMN_NAME));
  }

  // Data request should contain necessary information to fetch ratings.
  // userId filter should filter the rows containing user => item ratings
  // itemId filter should filter the rows for item => list of users who rated that item.
  public KijiDataModel(KijiTable table, KijiDataRequest request, KijiRowFilter userIdFilter,
      KijiRowFilter itemIdFilter) {
    mRequest = request;
    mUserIdFilter = userIdFilter;
    mItemIdFilter = itemIdFilter;
    mTable = table;
  }

  @Override
  public void refresh(Collection<Refreshable> alreadyRefreshed) {
    // No need to refresh, not caching anything.
  }

  @Override
  public LongPrimitiveIterator getUserIDs() throws TasteException {
    return getIterator(mUserIdFilter);
  }

  private LongPrimitiveIterator getIterator(KijiRowFilter filter) throws TasteException {
    KijiScannerOptions options = new KijiScannerOptions();
    options.setKijiRowFilter(filter);
    KijiTableReader reader = mTable.openTableReader();
    try {
      KijiRowScanner scanner = reader.getScanner(mRequest, options);
      return new KijiLongPrimitiveIterator(scanner);
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
  }

  @Override
  public PreferenceArray getPreferencesFromUser(long userID) throws TasteException {
    // TODO: Let's see if this can be HBase backed so we aren't bringing all the data
    // down to the client but that means a new implementation of the PreferenceArray. Let's
    // start with something simple to prove the point.

    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(USER, userID);
    KijiDataRequest request = KijiDataRequest.builder().addColumns(getColumnsDef()).build();
    List<GenericPreference> preferences = Lists.newArrayList();
    try {
      KijiRowData data = reader.get(id, request);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        KijiCell<Float> rating = it.next();
        long itemId = NumberUtils.SortableStr2long(rating.getQualifier());
        float ratingVal = rating.getData();
        preferences.add(new GenericPreference(userID, itemId, ratingVal));
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return new GenericUserPreferenceArray(preferences);
  }

  @Override
  public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(USER, userID);
    FastIDSet itemIdsForUser = new FastIDSet();

    try {
      KijiRowData data = reader.get(id, mRequest);

      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        long itemId = NumberUtils.SortableStr2long(it.next().getQualifier());
        itemIdsForUser.add(itemId);
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return itemIdsForUser;
  }

  @Override
  public LongPrimitiveIterator getItemIDs() throws TasteException {
    return getIterator(mItemIdFilter);
  }

  @Override
  public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
    // TODO: Let's see if this can be HBase backed so we aren't bringing all the data
    // down to the client but that means a new implementation of the PreferenceArray. Let's
    // start with something simple to prove the point.

    // Looks like this returns user + item + pref values
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(ITEM, itemID);
    KijiDataRequest request = KijiDataRequest.builder().addColumns(getColumnsDef()).build();
    List<GenericPreference> preferences = Lists.newArrayList();
    try {
      KijiRowData data = reader.get(id, request);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        KijiCell<Float> rating = it.next();
        long userId = NumberUtils.SortableStr2long(rating.getQualifier());
        float ratingVal = rating.getData();
        preferences.add(new GenericPreference(userId, itemID, ratingVal));
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }

    // Collections.sort(preferences, new PrefUserIdComparator());
    return new GenericItemPreferenceArray(preferences);
  }

  @Override
  public Float getPreferenceValue(long userID, long itemID) throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(USER, userID);
    try {
      KijiRowData data = reader.get(id, mRequest);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        KijiCell<Float> rating = it.next();
        long itemRated = NumberUtils.SortableStr2long(rating.getQualifier());
        float ratingVal = rating.getData();
        if (itemRated == itemID)
          return ratingVal;
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return null;
  }

  @Override
  public Long getPreferenceTime(long userID, long itemID) throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(USER, userID);
    try {
      KijiRowData data = reader.get(id, mRequest);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        KijiCell<Float> rating = it.next();
        long itemRated = NumberUtils.SortableStr2long(rating.getQualifier());
        if (itemRated == itemID)
          return rating.getTimestamp();
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return null;
  }

  @Override
  public int getNumItems() throws TasteException {
    // Looks like this returns user + item + pref values
    KijiTableReader reader = mTable.openTableReader();
    KijiScannerOptions options = new KijiScannerOptions();

    options.setKijiRowFilter(Filters.and(new FirstKijiRowFilter(), mItemIdFilter));
    int numItems = 0;

    try {
      KijiRowScanner scanner = reader.getScanner(mRequest, options);
      Iterator<KijiRowData> it = scanner.iterator();
      while (it.hasNext()) {
        it.next();
        numItems++;
      }
      scanner.close();
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return numItems;
  }

  @Override
  public int getNumUsers() throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    KijiScannerOptions options = new KijiScannerOptions();
    options.setKijiRowFilter(Filters.and(new FirstKijiRowFilter(), mUserIdFilter));
    int numUsers = 0;

    try {
      KijiRowScanner scanner = reader.getScanner(mRequest, options);
      Iterator<KijiRowData> it = scanner.iterator();
      while (it.hasNext()) {
        it.next();
        numUsers++;
      }
      scanner.close();
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return numUsers;
  }

  @Override
  public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(ITEM, itemID);
    int userCount = 0;
    try {
      KijiRowData data = reader.get(id, mRequest);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      while (it.hasNext()) {
        userCount++;
        it.next();
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return userCount;
  }

  @Override
  public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) throws TasteException {
    KijiTableReader reader = mTable.openTableReader();
    EntityId id = mTable.getEntityId(ITEM, itemID1);
    EntityId id2 = mTable.getEntityId(ITEM, itemID2);
    int userCount = 0;
    try {
      KijiRowData data = reader.get(id, mRequest);
      KijiRowData data2 = reader.get(id2, mRequest);
      Iterator<KijiCell<Float>> it = data.iterator(RATING_COLUMN_NAME);
      Iterator<KijiCell<Float>> it2 = data2.iterator(RATING_COLUMN_NAME);
      if (it.hasNext() && it2.hasNext()) {
        // Prime the loop
        KijiCell<Float> cell1 = it.next();
        KijiCell<Float> cell2 = it2.next();

        while (it.hasNext() && it2.hasNext()) {
          String cell1Qualifier = cell1.getQualifier();
          String cell2Qualifier = cell2.getQualifier();
          int cellComparison = cell1Qualifier.compareTo(cell2Qualifier);
          switch (cellComparison) {
          case 0: // both cells equal, increment both
            userCount++;
            cell1 = it.next();
            cell2 = it2.next();
            break;
          case -1: // cell1 < cell2. Increment right side iterator
            cell2 = it2.next();
            break;
          default: // cell1 > cell2. Increment left side iterator
            cell1 = it.next();
            break;
          }
        }

        // Final check for the last cells.
        String cell1Qualifier = cell1.getQualifier();
        String cell2Qualifier = cell2.getQualifier();
        if (cell1Qualifier.equalsIgnoreCase(cell2Qualifier)) {
          userCount++;
        }
      }
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
    return userCount;
  }

  @Override
  public void setPreference(long userID, long itemID, float value) throws TasteException {
    KijiTableWriter writer = mTable.openTableWriter();

    EntityId userEntityId = mTable.getEntityId(KijiDataModel.USER, userID);
    EntityId productEntityId = mTable.getEntityId(KijiDataModel.ITEM, itemID);
    try {
      writer.put(userEntityId, RATING_COLUMN_NAME, NumberUtils.long2sortableStr(itemID), value);
      writer.put(productEntityId, RATING_COLUMN_NAME, NumberUtils.long2sortableStr(userID), value);
    } catch (IOException e) {
      throw new TasteException(e);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new TasteException(e);
      }
    }
  }

  @Override
  public void removePreference(long userID, long itemID) throws TasteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasPreferenceValues() {
    return true;
  }

  @Override
  public float getMaxPreference() {
    // TODO change this to be more flexible (user passed in?)
    return 5;
  }

  @Override
  public float getMinPreference() {
    // TODO change this to be more flexible (user passed in?)
    return 1;
  }
}
