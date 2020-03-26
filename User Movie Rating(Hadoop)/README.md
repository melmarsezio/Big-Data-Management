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
