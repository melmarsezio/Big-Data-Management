//The raw data provided to us is in format of user_id::movie_id::rating::timestamp
//Firstly, use UserMapper to map the data to key(user_id): value(movie_id, rating) ->
//		where key is Text(user_id) and value is Custom Writable: MovieRatingWritable(movid_id, rating).
//Secondly, use UserReducer to reduce the data to key(user_id): value((movid_id1, rating1), (movid_id2, rating2)...) ->
//		where value is Custom ArrayWritable: MovieRatingArrayWritable containing array of MovieRatingWritable(movid_id, rating).
//Thirdly, use MovieMapper to map data to key(movie_id1, movid_id2): value(user_id, rating1, rating2) ->
//		key(movie_id1, movie_id2) are paired with two nested loop through all movie rated by this user
//		and stored in Custom Writable: MoviePairWritable(movie_id1, movie_id2),
//		value(user_id, rating1, rating2) are set to Custom Writable: UserScoreWritable
//		which has 3 variable: Text user, IntWritable score_1 & score_2.
//		Also, movie_ids are paired in ascending order (movid_id1 < movid_id2),
//		so when other user has same combination of movies but in different order(movie_id2, movie_id1),
//		key pair can be treated as same key.
//Lastly, use MovieReducer to reduce data to key(movid_id1, movie_id2):value((user_id1, rating1_1, rating1_2),(user_id2, rating2_1, rating2_2)...) ->
//		where values are stored in UserScoreArrayWritable (modified toString() so output can match the required format).
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class AssigOnez5237028 {

	public static class MoviePairWritable implements WritableComparable{
		private Text movie_1;
		private Text movie_2;

		public MoviePairWritable() {
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
		}
		public Text getMovie_1() {
			return movie_1;
		}
		public void setMovie_1(Text movie_1) {
			this.movie_1 = movie_1;
		}
		public Text getMovie_2() {
			return movie_2;
		}
		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_1.readFields(data);
			this.movie_2.readFields(data);
		}
		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_1.write(data);
			this.movie_2.write(data);
		}
		@Override
		public String toString() {
			//"(movie_1, movie_2)"
			return "(" + this.movie_1.toString() + "," + this.movie_2.toString() + ")";
		}
		@Override
		public int compareTo(Object o) {
			MoviePairWritable other = (MoviePairWritable) o;
			if (movie_1.compareTo(other.movie_1) != 0) {
				return movie_1.compareTo(other.movie_1);
			}
			else {
				return movie_2.compareTo(other.movie_2);
			}
		}
	}

	public static class MovieRatingWritable implements Writable{
		private Text movie;
		private IntWritable rating;

		public MovieRatingWritable() {
			this.movie = new Text("");
			this.rating = new IntWritable(-1);
		}
		public Text getMovie() {
			return movie;
		}
		public void setMovie(Text movie) {
			this.movie = movie;
		}
		public IntWritable getRating() {
			return rating;
		}
		public void setRating(IntWritable rating) {
			this.rating = rating;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie.readFields(data);
			this.rating.readFields(data);
		}
		@Override
		public void write(DataOutput data) throws IOException {
			this.movie.write(data);
			this.rating.write(data);
		}
	}

	public static class MovieRatingArrayWritable extends ArrayWritable{
		public MovieRatingArrayWritable() {
			super(MovieRatingWritable.class);
		}
		public MovieRatingArrayWritable(ArrayList<MovieRatingWritable> array) {
			super(MovieRatingWritable.class);
			MovieRatingWritable[]  movieRating = new MovieRatingWritable[array.size()];
			for (int i = 0; i < array.size(); i++) {
				movieRating[i] = new MovieRatingWritable();
				movieRating[i].setMovie(new Text(array.get(i).getMovie()));
				movieRating[i].setRating(new IntWritable(Integer.parseInt(array.get(i).getRating().toString())));
			}
			set(movieRating);
		}
		@Override
		public MovieRatingWritable[] get() {
			Writable[] tempList = super.get();
			MovieRatingWritable[] retVal = new MovieRatingWritable[tempList.length];
			int i = 0;
			for (Writable temp:tempList) {
				retVal[i] = (MovieRatingWritable) temp;
				i++;
			}
			return retVal;
		}
	}

	public static class UserScoreWritable implements Writable{
		private Text user;
		private IntWritable score_1;
		private IntWritable score_2;

		public UserScoreWritable() {
			this.user = new Text("");
			this.score_1 = new IntWritable(-1);
			this.score_2 = new IntWritable(-1);
		}
		public Text getUser() {
			return user;
		}
		public void setUser(Text user) {
			this.user = user;
		}
		public IntWritable getScore_1() {
			return score_1;
		}
		public void setScore_1(IntWritable score_1) {
			this.score_1 = score_1;
		}
		public IntWritable getScore_2() {
			return score_2;
		}
		public void setScore_2(IntWritable score_2) {
			this.score_2 = score_2;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.score_1.readFields(data);
			this.score_2.readFields(data);
		}
		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.score_1.write(data);
			this.score_2.write(data);
		}
		@Override
		public String toString() {
			//"(user_id, rating1, rating2)"
			return "(" + this.user + "," + this.score_1.toString() + "," + this.score_2.toString() + ")";
		}
	}

	public static class UserScoreArrayWritable extends ArrayWritable{
		public UserScoreArrayWritable() {
			super(UserScoreWritable.class);
		}
		public UserScoreArrayWritable(ArrayList<UserScoreWritable> array) {
			super(UserScoreWritable.class);
			UserScoreWritable[] userScore = new UserScoreWritable[array.size()];
			for (int i = 0; i < array.size(); i++) {
				UserScoreWritable val = new UserScoreWritable();
				val.setUser(new Text(array.get(i).getUser()));
				val.setScore_1(new IntWritable(Integer.parseInt(array.get(i).getScore_1().toString())));
				val.setScore_2(new IntWritable(Integer.parseInt(array.get(i).getScore_2().toString())));
				userScore[i] = val;
			}
			set(userScore);
		}

		@Override
		public String toString() {
			UserScoreWritable[] values = (UserScoreWritable[]) get();
			String retVal = "";
			for (UserScoreWritable value : values) {
				retVal += value.toString() + ",";
			}
			//retVal = "(user,rating1,rating2),(user,rating1,rating2)...."
			retVal = retVal.substring(0, retVal.length() - 1);
			//"[(user,rating1,rating2),(user,rating1,rating2)....]"
			retVal = "[" + retVal + "]";
			return retVal;
		}
	}

	public static class UserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieRatingWritable>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("::");
			MovieRatingWritable retVal = new MovieRatingWritable();
			retVal.setMovie(new Text(tokens[1]));
			retVal.setRating(new IntWritable(Integer.parseInt(tokens[2])));
			context.write(new Text(tokens[0]), retVal);
		}
	}

	public static class UserReducer extends Reducer<Text, MovieRatingWritable, Text, MovieRatingArrayWritable>{

		@Override
		protected void reduce(Text key, Iterable<MovieRatingWritable> values,
				Reducer<Text, MovieRatingWritable, Text, MovieRatingArrayWritable>.Context context) throws IOException, InterruptedException {
			ArrayList<MovieRatingWritable> array = new ArrayList<MovieRatingWritable>();
			for (MovieRatingWritable value:values) {
				MovieRatingWritable val = new MovieRatingWritable();
				val.setMovie(new Text(value.getMovie()));
				val.setRating(new IntWritable(Integer.parseInt(value.getRating().toString())));
				array.add(val);
			}
			MovieRatingArrayWritable retVal = new MovieRatingArrayWritable(array);
			context.write(key, retVal);
		}
	}

	public static class MovieMapper extends Mapper<Text, MovieRatingArrayWritable, MoviePairWritable, UserScoreWritable>{

		@Override
		protected void map(Text key, MovieRatingArrayWritable value,
				Mapper<Text, MovieRatingArrayWritable, MoviePairWritable, UserScoreWritable>.Context context)
				throws IOException, InterruptedException {
			ArrayList<MovieRatingWritable> movieRating = new ArrayList<MovieRatingWritable>();
			for (MovieRatingWritable movRat: value.get()) {
				MovieRatingWritable val = new MovieRatingWritable();
				val.setMovie(new Text(movRat.getMovie()));
				val.setRating(new IntWritable(Integer.parseInt(movRat.getRating().toString())));
				movieRating.add(val);
			}

			for (int i = 0; i < movieRating.size()-1; i++) {
				for (int j = i+1; j < movieRating.size(); j++) {
					MoviePairWritable keyPair = new MoviePairWritable();
					UserScoreWritable retVal = new UserScoreWritable();
					// If movie_id1 > movie_id2, pair "(movid_id2, movie_id1)" and value(user_id, rating2, rating1)
					if (movieRating.get(i).getMovie().compareTo(movieRating.get(j).getMovie()) > 0) {
						keyPair.setMovie_1(movieRating.get(j).getMovie());
						keyPair.setMovie_2(movieRating.get(i).getMovie());
						retVal.setUser(new Text(key.toString()));
						retVal.setScore_1(new IntWritable(Integer.parseInt(movieRating.get(j).getRating().toString())));
						retVal.setScore_2(new IntWritable(Integer.parseInt(movieRating.get(i).getRating().toString())));
					}
					// otherwise, pair "(movid_id1, movie_id2)" and value(user_id, rating1, rating2)
					else {
						keyPair.setMovie_1(movieRating.get(i).getMovie());
						keyPair.setMovie_2(movieRating.get(j).getMovie());
						retVal.setUser(new Text(key.toString()));
						retVal.setScore_1(new IntWritable(Integer.parseInt(movieRating.get(i).getRating().toString())));
						retVal.setScore_2(new IntWritable(Integer.parseInt(movieRating.get(j).getRating().toString())));
					}
					context.write(keyPair, retVal);
				}
			}
		}
	}

	public static class MovieReducer extends Reducer<MoviePairWritable, UserScoreWritable, MoviePairWritable, UserScoreArrayWritable> {

		@Override
		protected void reduce(MoviePairWritable key, Iterable<UserScoreWritable> values,
				Reducer<MoviePairWritable, UserScoreWritable, MoviePairWritable, UserScoreArrayWritable>.Context context)
				throws IOException, InterruptedException {

			ArrayList<UserScoreWritable> array = new ArrayList<UserScoreWritable>();
			for (UserScoreWritable value: values) {
				UserScoreWritable val = new UserScoreWritable();
				val.setUser(new Text(value.getUser()));
				val.setScore_1(new IntWritable(Integer.parseInt(value.getScore_1().toString())));
				val.setScore_2(new IntWritable(Integer.parseInt(value.getScore_2().toString())));
				array.add(val);
			}
			UserScoreArrayWritable retVal = new UserScoreArrayWritable(array);
			context.write(key, retVal);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		//Job 1
		Job job1 = Job.getInstance(conf, "User: Movie, Score");
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(UserReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieRatingWritable.class);
		job1.setOutputKeyClass(Text.class);			//for job output
		job1.setOutputValueClass(MovieRatingArrayWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out, "job1"));
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		//Job 2
		Job job2 = Job.getInstance(conf, "Movie Pair: User, Score");
		job2.setMapperClass(MovieMapper.class);
		job2.setReducerClass(MovieReducer.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(MoviePairWritable.class);
		job2.setMapOutputValueClass(UserScoreWritable.class);
		job2.setOutputKeyClass(MoviePairWritable.class);
		job2.setOutputValueClass(UserScoreArrayWritable.class);
		FileInputFormat.addInputPath(job2, new Path(out, "job1"));
		FileOutputFormat.setOutputPath(job2, new Path(out, "job2"));
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
