package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {

    //TODO: What is AbstractFunction3? It looks like AbstractFunction3 is the "base" class that is being extended by "VProg".
    //  so in this case that means we are adding the additional method (function) in the "apply" line.
    //  @Override is just a flag that indicates we are replacing the existing method in AF3 with a new one.
    //  Are we overriding a method called "Integer" or simply returning a value of type Integer?
    //TODO: It looks like the first if statement is just checking to see if the vertexValue is the maximum allowable
    //  integer value (2^31 - 1). Otherwise, it will return the maximum of vertexValue or message
    //TODO: Serializable means the output? is in byte format? what does this mean? Why do we need to do it?
    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            if (message == Integer.MAX_VALUE) {             // superstep 0
                System.out.println("1. VProg: IF. vertexID: " + vertexID + ". vertexValue: " + vertexValue);
                System.out.println("");
                return vertexValue;

            } else {                                        // superstep > 0
                System.out.println("1. VProg: ELSE. vertexID: " + vertexID + ". vertexValue: " + vertexValue + ". message: " + message);
                System.out.println("");
                return Math.max(vertexValue,message);
            }
        }
    }
    //TODO: AbstractFunction (1,2,3) are all imported from the Scala (library?), giving us the ability to create
    //  functions (which is not possible in java? I thought a method is a function.).
    //NOTES on Scala: Scala is important in the big data world,
    //  and necessary to be used in Spark. THAT is WHY we are using Scala. Scala removes the boiler-plate code requirements
    //  that exist in java. Scala supports concurrency (running code on multiple cores). Scala is functional programming,
    //  instead of object oriented like Java. Scala is faster than Python, ruby and NodeJS. Both Java and Scala run on
    //  JVM (java virtual machine), we can use java/scala libraries and codes together.
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();

            System.out.println("2. sendMsg: General. triplet: " + triplet);
            System.out.println("2. sendMsg: General. sourceVertex: " + sourceVertex);
            System.out.println("2. sendMsg: General. dstVertex: " + dstVertex);

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // do nothing
                System.out.println("2. sendMsg: IF. Return empty array");
                System.out.println("");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                // the new Tuple is where we assign a new value to the vertex. So the vertex that has the smaller of the two
                // values will be assigned a larger value.
                System.out.println("2. sendMsg: ELSE. The output dstID and new vertex: " + (Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2))));
                System.out.println("");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            System.out.println("This is merge");
            return null;
        }
    }

    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
            new Tuple2<Object,Integer>(1l,9),
            new Tuple2<Object,Integer>(2l,1),
            new Tuple2<Object,Integer>(3l,6),
            new Tuple2<Object,Integer>(4l,8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
            new Edge<Integer>(1l,2l, 1),
            new Edge<Integer>(2l,3l, 1),
            new Edge<Integer>(2l,4l, 1),
            new Edge<Integer>(3l,4l, 1),
            new Edge<Integer>(3l,1l, 1)
        );

        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        Tuple2<Long,Integer> max = (Tuple2<Long,Integer>)ops.pregel(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,      // Run until convergence
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
        .vertices().toJavaRDD().first();

        System.out.println(max._2 + " is the maximum value in the graph");
	}
	
}
