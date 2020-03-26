# Single-Source Shortest Path(Spark)
#### Coursework assignment for Big Data Management
## Specification (Problem Definition)
![](https://i1.wp.com/algorithms.tutorialhorizon.com/files/2018/03/Weighted-Graph.png?ssl=1)  
A path in a graph can be defined as the set of consecutive nodes such that there is an edge from one node to the next node in the sequence. The shortest path between two nodes can be defined as the path that has the minimum total weight of the edges along the path. For example, the shortest path from Node 1 to Node 4 is Node1-> Node3 -> Node4 with the distance of 4.

In this assignment, you are aksed to calculate the distance from a starting node (e.g., N0) to all other nodes.

**Please consider the following tips while implementing your solution:**
1. In this assignment we assume there is no abandoned node, meaning that each node at least has one edge to another node.
2. The minimum weight of an edge between two individual nodes is 1.
3. You need to use Spark Core (not GraphX or similar solutions) to implement a solution for this given problem.
4. The output must be sorted (ascending) by length of shortest path.
5. The output should be written into a txt file.
6. The output should be formatted as follow ( as shown in the example below) containing 3 columns, comma delimited : first column contains the destination node, followed by the length of the shortest path in the second column, and the actual shortest path from the starting node to the destination node.
7. You should NOT assume that the starting node is always N0 (it can be any given node).
8. You can download a sample input , and expected output .
9. If the start-node is the last node of the graph (e.g.,N5), which means it has no way to get to another nodes, the output should be -1 for all the nodes as shown below :
```
  N0,-1,
  N1,-1,
  ...
```

## Input Format
You can assume that the input represents a connected directed graph.
The input is formatted as below (represents the same graph as the above image). Each line of the input file represents a vertex of the graph formatted like: starting node, end node, and the distance
```
N0,N1,4
N0,N2,3
N1,N2,2
N1,N3,2
N2,N3,7
N3,N4,2
N4,N0,4
N4,N1,4
N4,N5,6
```

## Output Format
The output should be formats as below. Each line of output represents the distance from the starting node to another node, and it is formatted as: the destination node, shortest distance, and the path from the starting node to the destination node. The file should be sorted by the shortest path:
```
N2,3,N0-N2
N1,4,N0-N1
N3,6,N0-N1-N3
...
```

## Instruction
>+ cmd run `$ javac -cp ".:Spark-Core.jar" AssigTwo{zid}.java ` to compile [`AssigTwo{zid}.java`](https://github.com/melmarsezio/Big-Data-Management/blob/master/Single-Source%20Shortest%20Path(Spark)/AssigTwoz5237028.java) with dependency [`Spark-Core.jar`](https://github.com/melmarsezio/Big-Data-Management/blob/master/Single-Source%20Shortest%20Path(Spark)/Spark-Core.jar)  
>+ cmd run `$ java -cp ".:Spark-Core.jar" AssigTwo{zid} STARTING_NODE INPUT_PATH OUTPUT_PATH` to test the file, `INPUT_PATH` could be [`graph.txt`](https://github.com/melmarsezio/Big-Data-Management/blob/master/Single-Source%20Shortest%20Path(Spark)/graph.txt) for example.   
>
> All results will be generated and saved in `OUTPUT_PATH`
