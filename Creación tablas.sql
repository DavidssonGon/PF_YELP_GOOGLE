CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.google_metadata` (
  Gmap_Id STRING,
  Name STRING,
  Avg_Rating FLOAT64,
  Description STRING,
  Number_of_Reviews INT,
  Address STRING,
  Latitude FLOAT64,
  Longitude FLOAT64,
  Category STRING,
  State STRING,
  Hours STRING,
  Relative_Results STRING
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.google_reviews` (
  Gmap_Id STRING,
  User_Id STRING,
  Name STRING,
  Date TIMESTAMP,
  Comment STRING,
  Rating INTEGER,
  Resp_Date TIMESTAMP,
  Resp_Comment STRING
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.google_attributes` (
  Gmap_Id STRING,
  Accessibility STRING,
  Activities STRING,
  Amenities STRING,
  Atmosphere STRING,
  Crowd STRING,
  Dining_options STRING,
  From_the_business STRING,
  Getting_here STRING,
  Health_and_safety STRING,
  Highlights STRING,
  Offerings STRING,
  Payments STRING,
  Planning STRING,
  Popular_for STRING,
  Recycling STRING,
  Service_options STRING,
  Avg_Rating FLOAT64
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_checkin` (
  Business_Id STRING,
  Date TIMESTAMP
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_reviews` (
  Business_Id STRING,
  User_Id STRING,
  Date TIMESTAMP,
  Comment STRING,
  Cool INTEGER,
  Funny INTEGER,
  Useful INTEGER,
  Stars FLOAT64
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_tip` (
  Business_Id STRING,
  User_Id STRING,
  Date TIMESTAMP,
  Comment STRING,
  Compliment_Count INTEGER
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_user` (
  User_Id STRING,
  Name STRING,
  Yelping_Since TIMESTAMP,
  Elite STRING,
  Review_Count INTEGER,
  Average_Stars STRING,
  Useful INTEGER,
  Funny INTEGER,
  Cool INTEGER,
  Fans INTEGER,
  Compliment_Hot INTEGER,
  Compliment_More INTEGER,
  Compliment_Profile INTEGER,
  Compliment_Cute INTEGER,
  Compliment_List INTEGER,
  Compliment_Note INTEGER,
  Compliment_Plain INTEGER,
  Compliment_Cool INTEGER,
  Compliment_Funny INTEGER,
  Compliment_Writer INTEGER,
  Compliment_Photos INTEGER,
  Friends STRING
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_attributes` (
  Business_Id STRING,
  Stars FLOAT64,
  RestaurantsDelivery STRING,
  RestaurantsTakeOut STRING,
  RestaurantsPriceRange2 STRING,
  RestaurantsGoodForGroups STRING,
  OutdoorSeating STRING,
  RestaurantsReservations STRING,
  GoodForKids STRING,
  BusinessAcceptsCreditCards STRING,
  HasTV STRING,
  Alcohol STRING,
  Garage STRING,
  Street STRING,
  Validated STRING,
  Lot STRING,
  Valet STRING
);

CREATE TABLE `prometheus-data-solutions.Datawarehouse_prometheus.yelp_business` (
  Business_Id STRING,
  Name STRING,
  Address STRING,
  City STRING,
  State STRING,
  Postal_Code STRING,
  Latitude FLOAT64,
  Longitude FLOAT64,
  Stars FLOAT64,
  Review_count INTEGER,
  Is_open INTEGER,
  Categories STRING
);
