# row key is like ['user', user_id ]
# rating family. qualifier is the product_id or user_id depending on the the context of the row key
# value is the actual rating (float)
# timestamp is of course the time of the rating
# product => user should be symmetric with user => product for fast lookups. 
CREATE TABLE product_ratings WITH DESCRIPTION 'ratings for some product'
ROW KEY FORMAT (rating_type STRING, id LONG)
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  MAP TYPE FAMILY rating { "type" : "float" } WITH DESCRIPTION 'rating information'
);