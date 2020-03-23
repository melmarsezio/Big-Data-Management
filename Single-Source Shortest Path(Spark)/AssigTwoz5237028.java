//The input data of edges are provided with format "From, To, Distance", store in "input" RDD.
//First, flat map "input" into individual vertices, so we will have a list of all vertices, called "vertices" RDD.
//Second, map the union of "input" and "vertices" into pair of ('From', ('To', Distance)). The reason of having "input"
//      union with "vertices" is some node might only have out-edge, but no in-edge, and we don't want to miss these nodes.
//      This file is grouped by key('From' node) and called "edges" PairRDD.
//Third, set up PairRDD called "graph" containing ('From', (curDist, curPath, adjacency)), where 'curDist' is the current
//      shortest distance from StartNode (args[0]), initialized with 0 if it's StartNode, otherwise -1 to indicate infinity.
//      'curPath' is current Path from StartNode, stored using class Path which has "pathList" in type ArrayList<String>
//      "curPath" is initialized with empty list of nodes. 'adjacency' is a list of all connected nodes with corresponding distance.
//Fourth, repeatedly get new node distances from "graph", for each node in adjacency list, update distance
//      by current distance plus the edge distance, and update path by adding 'From' node to the current Path. This new node
//      distances is stored in "new_dist" PairRDD: ('To', (Distance, Path)).
//Fifth, update the old "graph" PairRDD by compare new distance with old distance. Update distance and path when needed.
//      This is stored is PairRDD called "update_graph".
//Sixth, compare the "graph" with "new_dist" to count the number of changes that was made during last transformation. If
//      we have some changes, loop back to step four, otherwise break the loop.
//Seventh, now we have a "graph" with ('Node', (Distance, Path, AdjacencyList)). Abandon the AdjacencyList, filter out the
//      StartNode row, sort using Distance length and save it as text file with expect format.


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;
import scala.Tuple3;

public class AssigTwoz5237028 {
    public static class MyComparator implements  Serializable, Comparator<Integer> {
        @Override
        public int compare(Integer num1, Integer num2) {
            if (num1 == -1) {
                return Integer.MAX_VALUE;
            }
            else if(num2 == -1) {
                return Integer.MIN_VALUE;
            }
            else{
                return num1-num2;
            }
        }
    }

    public static class Path implements  Serializable {
        ArrayList<String> pathList;

        Path(ArrayList<String> Nodes) {
            pathList = Nodes;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            String separator = "";
            for (String node : pathList) {
                sb.append(separator);
                sb.append(node);
                separator = "-";
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Single-Source Shortest Path")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        String startNode = args[0];
        JavaRDD<String> input = context.textFile(args[1]);

        JavaRDD<String> vertices = input.flatMap((FlatMapFunction<String, String>)
                line -> {
                    ArrayList<String> retVal = new ArrayList<>();
                    retVal.add(line.split(",")[0]);
                    retVal.add(line.split(",")[1]);
                    return retVal.iterator();
                }).distinct();

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> edges = input.union(vertices)
                .mapToPair((PairFunction<String, String, Tuple2<String, Integer>>)
                line -> {
                    String[] parts = line.split(",");
                    String from;
                    String to;
                    int distance;
                    if (parts.length == 3) {
                        from = parts[0];
                        to = parts[1];
                        distance = Integer.parseInt(parts[2]);
                    } else {
                        from = parts[0];
                        to = "None";
                        distance = -1;
                    }
                    return new Tuple2<>(from, new Tuple2<>(to, distance));
                }).groupByKey();

        JavaPairRDD<String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>> graph = edges
                .mapToPair((PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>,
                        String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>>)
                s -> {
                    String node = s._1;
                    Iterable<Tuple2<String, Integer>> adjacencyList = s._2;
                    ArrayList<Tuple2<String, Integer>> adjacency = new ArrayList<>();
                    Path emptyPathList = new Path(new ArrayList<>());
                    for (Tuple2<String, Integer> i: adjacencyList) {
                        Tuple2<String, Integer> group = new Tuple2<>(i._1, i._2);
                        adjacency.add(group);
                    }

                    if (adjacency.size() > 1) {
                        adjacency.remove(adjacency.size() - 1);
                    }
                    int dist;
                    if (node.equals(startNode))
                        dist = 0;
                    else
                        dist = -1;
                    return new Tuple2<>(node, new Tuple3<>(dist, emptyPathList, adjacency));
                });

        while (true) {
            JavaPairRDD<String, Tuple2<Integer, Path>> new_dist = graph
                    .flatMapToPair((PairFlatMapFunction<Tuple2<String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>>,
                    String, Tuple2<Integer, Path>>)
                    s -> {
                        String curNode = s._1;
                        Integer curDist = s._2._1();
                        Path curPath = s._2._2();
                        ArrayList<Tuple2<String, Tuple2<Integer, Path>>> retVal = new ArrayList<>();
                        Iterable<Tuple2<String, Integer>> nextNode = s._2._3();
                        if (curDist == -1) {
                            for (Tuple2<String, Integer> i : nextNode) {
                                if (!i._1().equals("None"))
                                    retVal.add(new Tuple2<>(i._1(),new Tuple2<>(-1, curPath)));
                            }
                            retVal.add(new Tuple2<>(curNode,new Tuple2<>(-1, curPath)));
                        } else {
                            for (Tuple2<String, Integer> i : nextNode) {
                                ArrayList<String> nodeList = new ArrayList<>(curPath.pathList);
                                nodeList.add(curNode);
                                Path updatePath = new Path(nodeList);
                                if (!i._1().equals("None")) {
                                    retVal.add(new Tuple2<>(i._1(), new Tuple2<>(i._2() + curDist, updatePath)));
                                }
                            }
                        }
                        return retVal.iterator();
                    }).reduceByKey((Function2<Tuple2<Integer, Path>, Tuple2<Integer, Path>, Tuple2<Integer, Path>>)
                    (first, second)->{
                        if (first._1 == -1){
                            return second;
                        }
                        else if(second._1 == -1) {
                            return first;
                        }
                        else if (first._1 > second._1) {
                            return second;
                        }
                        else {
                            return first;
                        }
                    });

            JavaPairRDD<String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>> update_graph = graph.join(new_dist)
                    .mapToPair((PairFunction<Tuple2<String, Tuple2<Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>, Tuple2<Integer, Path>>>,
                            String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>>)
                            s -> {
                                String start_Node = s._1;
                                Integer curDist = s._2._1._1();
                                Path curPath = s._2._1._2();
                                Integer newDist = s._2._2._1;
                                Path newPath = s._2._2._2;
                                Iterable<Tuple2<String, Integer>> adjacencyList = s._2._1._3();
                                if (newDist == -1) {
                                    return new Tuple2<>(start_Node, new Tuple3<>(curDist, curPath, adjacencyList));
                                } else if (curDist == -1) {
                                    return new Tuple2<>(start_Node, new Tuple3<>(newDist, newPath, adjacencyList));
                                } else if (newDist > curDist) {
                                    return new Tuple2<>(start_Node, new Tuple3<>(curDist, curPath, adjacencyList));
                                } else {
                                    return new Tuple2<>(start_Node, new Tuple3<>(newDist, newPath, adjacencyList));
                                }
                            });

            JavaPairRDD<String, Integer> change = graph.join(new_dist)
                    .mapToPair((PairFunction<Tuple2<String, Tuple2<Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>,
                            Tuple2<Integer, Path>>>, String, Integer>)
                    s->{
                        String node = s._1;
                        Integer privDist = s._2._1._1();
                        Integer curDist = s._2._2._1;
                        if (curDist.equals(-1)){
                            return new Tuple2<>(node, 0);
                        }
                        else if (privDist.equals(-1)) {
                            return new Tuple2<>(node, 1);
                        }
                        else if (privDist > curDist) {
                            return new Tuple2<>(node, 1);
                        }
                        else
                            return new Tuple2<>(node, 0);
            });
			graph = update_graph;
			if (change.values().reduce(Integer::sum) == 0){
                break;
            }
        }
        graph = graph.filter((Function<Tuple2<String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>>, Boolean>)
                s-> !s._1.equals(startNode));
        JavaPairRDD<Integer, Tuple2<String, Path>> sortPathLength = graph
                .mapToPair((PairFunction<Tuple2<String, Tuple3<Integer, Path, Iterable<Tuple2<String, Integer>>>>,
                        Integer, Tuple2<String, Path>>)
                s->{
                    ArrayList<String> nodeList = new ArrayList<>(s._2._2().pathList);
                    if (s._2._1() != -1) {
                        nodeList.add(s._1);
                    }
                    Path finalPath = new Path(nodeList);
                    return new Tuple2<>(s._2._1(), new Tuple2<>(s._1, finalPath));
        }).sortByKey(new MyComparator(), true, 1);
        JavaRDD<String> finalResult = sortPathLength.map(s-> s._2._1 + ',' + s._1 + ',' + s._2._2);
        finalResult.saveAsTextFile(args[2]);
    }
}
