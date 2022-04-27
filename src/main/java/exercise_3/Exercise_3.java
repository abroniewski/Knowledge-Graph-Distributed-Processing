package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;


// to keep track of the shortest path, we can include an additional stored value in each
// vertex of the path that we used to get there. Then, whenever we have a new extension
// to the path, we add in the path that was stored in sourceVertex.
//
// Essentially, we will change the parameter VD from Ex2 from an Integer that stored the distance to a
// list that stores the distance and a list containing the shortest path to get to that vertex
// [ID, [shortestDistanceValue, [shortestPathToThisID]]]
// This will require us to update every place the methods are called or defined to ensure we have consistency
// in the new class being used. Any place where [VD] is referenced is where we need to look.
// Our return values also need to be update to return our new version of VD instead of just an integer.
//
// reference: https://www.geeksforgeeks.org/printing-paths-dijkstras-shortest-path-algorithm/
// reference: GraphX https://spark.apache.org/docs/latest/graphx-programming-guide.html


public class Exercise_3  {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Object>> apply(Long vertexID, Tuple2<Integer,List<Object>> vertexValue, Tuple2<Integer,List<Object>> message) {

//            System.out.println("1. VProg: vertexID: " + vertexID + ". vertexValue: " + vertexValue._1 + ". message: " + message._1);
            if (vertexValue._1 <= message._1) {
                return vertexValue;
            }
            else {
                return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Object>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer,List<Object>>>>> implements Serializable {
        @Override
        // The triplet has values of ((SourceVertexID, (SourceVertexValue, SourceVertexPath)), (DstVertexID, (DstVertexValue, DstVertexPath), (EdgeValue))

        public Iterator<Tuple2<Object,Tuple2<Integer,List<Object>>>> apply(EdgeTriplet<Tuple2<Integer,List<Object>>, Integer> triplet) {
            // to make things more readable, I define all the variables.
            Tuple2<Object,Tuple2<Integer,List<Object>>> sourceVertex = triplet.toTuple()._1();
            Integer sourceVertexCurrentDistance = sourceVertex._2._1;
            List sourceVertexShortestPath = sourceVertex._2._2;

            Tuple2<Object,Tuple2<Integer,List<Object>>> dstVertex = triplet.toTuple()._2();
            Integer dstVertexCurrentDistance = dstVertex._2._1;

            Integer edgeDistance = triplet.toTuple()._3();

            // this if statement required, otherwise the MAX_VALUE is increased by edge distance and becomes negative.
            if  (sourceVertexCurrentDistance == Integer.MAX_VALUE) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>().iterator()).asScala();
            }

            else if (sourceVertexCurrentDistance + edgeDistance <= dstVertexCurrentDistance) {
                // here we will append the ID of the dstVertex to the shortest path that existed in the source vertex
                // this will be returned and sent in the message
                List<Object> shortestPath = sourceVertexShortestPath;
                shortestPath.add(Long.parseLong(String.valueOf(dstVertex._1)));
//                System.out.println("2. sendMsg IF: srcDist (" + sourceVertex._2 + ") + edgeDist (" + edgeDistance + ") <= dstDist (" + dstVertex._2 + ")");
//                System.out.println("2. sendMsg IF: shortestPath" + Collections.addAll(sourceVertexShortestPath, triplet.dstId()));
//                System.out.println("2. Shortest Path: " + shortestPath);
//                System.out.println("");

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer,List<Object>>>(triplet.dstId(), new Tuple2<Integer,List<Object>>(sourceVertexCurrentDistance + edgeDistance, shortestPath))).iterator()).asScala();

            } else {
                // do nothing
                System.out.println("2. sendMsg ELSE: sourceVertex._2 (" + sourceVertex._2 + ") + edgeDistance (" + edgeDistance + ") <= dstVertex._2(" + dstVertex._2 + ")");
                System.out.println("2. Return array: " + new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>());
                System.out.println("");

                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>().iterator()).asScala();

            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>> implements Serializable {
        @Override

        public Tuple2<Integer, List<Object>> apply(Tuple2<Integer, List<Object>> o, Tuple2<Integer, List<Object>> o2) {
            // instead of using the method from exercise to, we use an if statement so that we can return the full
            // VD parameter while only comparing the distances.
//            System.out.println("3. merge: o" + o + ". o2: " + o2 + ". return: " + Math.min(o._1, o2._1));
//            System.out.println("");
//            System.out.println("MERGE!");
            if (o._1 <= o2._1) {
                return o;
            }
            else {
                return o2;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();


        // here is where the magic happens. We replaced the Integer with a Tuple2 that contains the shortestPath distance
        // as well as a list that will be appended to in order to keep track of the path.
        List<Tuple2<Object,Tuple2<Integer,List<Object>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(1l, new Tuple2<Integer,List<Object>>(0, Lists.newArrayList(1l))),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(2l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(3l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(4l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(5l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(6l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList()))
        );


        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );


        // Through all of the following code, we have to update our class to match the new VD
        JavaRDD<Tuple2<Object,Tuple2<Integer,List<Object>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        // note: here we updated the VD classtage from Integer to Tuple2
        // TODO: What exactly is this classtag, and why wouldn't I include a full Tuple2<int<List>> type of definition?
        Graph<Tuple2<Integer,List<Object>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<Integer,List<Object>>(0, Lists.newArrayList(1l)), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // Here we have to update the pregel parameter "A" as it is the type for the initial message that is passed to
        // all the vertexes
        ops.pregel(new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList()), // initialMsg
                        Integer.MAX_VALUE,      // maxIterations
                        EdgeDirection.Out(),    //activeDirection
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))      // VD classTag
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                // TODO: Need to add a way to pull the list vertex._2._1 and remove all the duplicates before printing.
                    Tuple2<Object,Tuple2<Integer,List<Object>>> vertex = (Tuple2<Object,Tuple2<Integer,List<Object>>>)v;
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2._2+" with cost "+vertex._2._1);
                });
    }

}
