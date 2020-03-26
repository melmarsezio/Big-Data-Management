# User Movie Rating(Hadoop)
#### Coursework assignment for Big Data Management
## Specification
In this assignment we will be working on the processing of a Movie dataset:  
[https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/ratings.dat](https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/ratings.dat)

In this dataset, each row contains a movie rating done by a user (e.g., user1 has rated Titanic as 10). Here is the format of the dataset: `user_id::movie_id::rating::timestamp`

In this assignment, for each pair of movies A and B, you need to find all the users who rated both movie A and B. For example, given the following dataset (for the sake of illustration we have used U and M to represent users and movies respectively in the example):
```
U1::M1::2::11111111
U2::M2::3::11111111
U2::M3::1::11111111
U3::M1::4::11111111
U4::M2::5::11111111
U5::M2::3::11111111
U5::M1::1::11111111
U5::M3::3::11111111
```
The assumption is that User and Movie names are in String format and Rating is an Integer value. You should ignore the timestamp in the Mapper.

The output of your code should be in the form as below:
```
(M1,M2) [(U5,1,3)]
(M2,M3) [(U5,3,3),(U2,3,1)]
(M1,M3) [(U5,1,3)]
```
where (M,M) shows pairs of movies, [] indicates the list of users and their ratings. For example, (U5,1,3) shows U5 has rated M1 and M2 with 1 and 3 respectively.

**(please note that Your output should exactly be formatted as the output example above.)**


**Tips:**
+ You may need to implement more than one Mapper/Reducer in this assignment. You need to look at chaining in MapReduce jobs: [https://stackoverflow.com/questions/38111700/chaining-of-mapreduce-jobs#answer-38113499](https://stackoverflow.com/questions/38111700/chaining-of-mapreduce-jobs#answer-38113499)
+ You also may need a self-join to find movie pairs, the reduce-side join pattern can help: [https://www.edureka.co/blog/mapreduce-example-reduce-side-join/](https://www.edureka.co/blog/mapreduce-example-reduce-side-join/)
+ If the key and the values for a Mapper differ from those of Reduce, you need to set the following configurations:
job.setMapOutputKeyClass(), job.setOutputKeyClass(), job.setMapOutputValueClass(), job.setapOutputValueClass()
+ Do not set Combiner in this assignment ( job.setCombiner() )
+ If the Value for the Mapper/Reducer is a complex object, you need to implement a Writerable Interface class
+ If the Key for the Mapper/Reducer is a complex object, you also need to implement a WritableComparable Interface
+ You can use ArrayWritable to store Array values, but you need to implement its toString() function to be able to write the object into a text file.
+ Please be aware of iterating over values inside a reducer (Iterable<MyWritable> values). When looping through the Iterable value list, each Object instance is reused internally by the reducer. So if you add them to another list, at the end of the process, all of the elements in the new list will be the same as the last object you added to the list.

## Instruction
>+ cmd run `$ javac -cp ".:Hadoop-Core.jar" AssigOne{zid}.java ` to compile [`AssigOne{zid}.java`](https://github.com/melmarsezio/Big-Data-Management/blob/master/User%20Movie%20Rating(Hadoop)/AssigOnez5237028.java) with dependency [`Hadoop-Core.jar`](https://github.com/melmarsezio/Big-Data-Management/blob/master/User%20Movie%20Rating(Hadoop)/Hadoop-Core.jar)  
>+ cmd run `$ java -cp ".:Hadoop-Core.jar" AssigOne{zid}  INPUT_PATH OUTPUT_PATH` to test the file, `INPUT_PATH` could be [`ratings.dat`](https://github.com/melmarsezio/Big-Data-Management/blob/master/User%20Movie%20Rating(Hadoop)/ratings.dat) for example.
