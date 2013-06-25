Kiji Mahout
=============
This project contains some code to try and integrate Kiji and Mahout together. This project contains an implementation of the DataModel interface that works with a Kiji table of a certain format.

Running Instructions:
--------------
1. Create the table using the input/ratings_table.ddl file. Use the default kiji instance.
2. Run the bulk loader ./run_bulk_loader.sh
3. Run org.kiji.mahoutfun.RecommenderIntro.

The class in step 3 contains some code from chapter 2 of the Mahout in Action book adapted a bit to use the KijiDataModel. I compare against the file model to make sure things worked properly. This simple driver class has implementations for Item-Item and User-Item.

The data in input/ch2/gl_100k.csv is the group lens 100k ratings set. 

<b>Note:</b> I don't make any guarantees about the performance of this code and is a proof of concept.