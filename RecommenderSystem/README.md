##  Movie Recommender System

#### Problem
Implement a movie recommender system using an item-based collaborative filtering method (IBCF) and the MapReduce paradigm on Hadoop


#### Algorithm
1. Use users' historical ratings to generate a similarity matrix (#items by #items) between different items. Here, we define a similarity measure by the co-occurrence of two items.
2. Generate a rating matrix (#items by #users), where any unrated movie by a user is replaced by the mean rating of the user's rating history.
3. Obtain a predicted rating matrix (#users by #items) from the product of similarity matrix and the rating matrix
4. Recommend the top-K movies given a specific user


#### Workflow - 6 MapReduce jobs
1. RatingDataReader: 
    - Mapper: read the raw rating data which is in the format of "userId,movieId,rating"  
        input: <offset, userId,movieId,rating>  
        output: <userId, movieId:rating>  
    - Reducer: aggregate the rating data by userId  
        input: <userId, (movieId1:rating1, movieId2:rating2, ...)>
        output: <userId, movieId1:rating1,movieID2:rating2,...>  

2. CooccurrenceMatrixGenerator: 
    - Mapper: read the aggregated rating data  
        input: <offset, userId \t movieId1:rating1,movieId2:rating2,...>  
        output: <movieIdi:movieIdj, 1>  
    - Reducer: generate the co-occurrence of each movieId pair  
        input: <movieIdi:movieIdj, (1, 1, ...)>  
        output: <movieIdi:movieIdj, 1+1+...>  
        
3. CooccurrenceMatrixNormalization:
    - Mapper: read the unnormalized co-occurrence matrix  
        input: <offset, movieIdi:movieIdj \t count>  
        output: <movieIdi, movieIdj:count>  
    - Reducer: normalize the row of movieIdi  
        input: <movieIdi, (movieIdj1:count1, movieIdj2:count2, ...)>  
        output: <movieIdj, movieIdi=countj/totalCount>  

4. UserRatingAveraging:  
    - Mapper: read the raw rating data which is in the format of "userId,movieId,rating"  
        input: <offset, userId,movieId,rating>
        output: <userId, rating>
    - Reducer: calculate the average rating given by a user  
        input: <userId, (rating1, rating2, ...)>  
        output: <userId, (rating1 + rating2 + ...) / #rating>

5. MatrixCellMultiplication: Mapper1 + Mapper2 ---> Reducer  
    - Mapper1: read the normalized co-occurrence matrix  
        input: <offset, movieIdk \t movieIdi=weighti>  
        output: <movieIdk, movieIdi=weighti>  
    - Mapper2: generate the rating matrix by reading the raw rating data  
        input: <offset, userId,movieId,rating>  
        output: <movieId, userId:rating>   
    - Reducer: multiply a cell of co-occurrence matrix with the corresponding cell of rating matrix  
        setup: read the average rating for each user: HashMap<userId, averageRating>  
        input: <movieIdk, (movieIdi1=weighti1, movieIdi2=weighti2, ..., userIdj1:ratingj1, userIdj2:ratingj2, ...)>  
        output: <userId:movieId, weight * rating>  
    
6. MatrixCellSum: 
    - Mapper: read the sub product of each cell  
        input: <offset, userId:movieId \t subRating>    
        output: <userId:movieId, subRating>  
    - Reducer: sum up the sub rating to the final rating
        input: <userId:movieId, (subRating1, subRating2, ...)>  
        output: <userId:movieId, subRating1 + subRating2 + ...>  
        

#### Usage:
First copy this folder to HDFS and navigate to this folder, then follow commands below:
```bash
$ hdfs dfs -mkdir /input
$ hdfs dfs -put ./input/* /input
$ cd src/main/java
$ hadoop com.sun.tools.javac.Main *.java
$ jar cf recommender.jar *.class
$ hadoop jar recommender.jar Driver /input /output/userRating /output/cooccurrenceGenerator /output/cooccurrenceNormal /output/userAverageRating /output/cellMultiplication /output/cellSum
```

#### Dataset:
From the [Netflix Prize dataset](https://www.kaggle.com/netflix-inc/netflix-prize-data)

