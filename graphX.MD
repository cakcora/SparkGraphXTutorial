<html>
<body>
<h2>Issues in GraphX</h2>
<h3>Problems with GraphGenerators.logNormalGraph()</h3>
<ul>
	<li> <p>This is a serious bug that would affect research experiments with synthetic data: </p>
		<p>val gr = GraphGenerators.logNormalGraph(sparkContext,numVertices,numPartitions,meanOutDegree,deviationOutDegree)</p>
		<p>When numVertices=10, meanOutDegree=2 you would expect less than 20 edges, but there are less than 30 edges. So, for any graph with n number of vertices and m mean degrees, there will be at most (m+1)*n edges.</p></li>
	<li><p>It seems that  GraphGenerators.logNormalGraph creates a graph where multiple edges can exist between a pair of vertices. This graph creation function takes mean out degrees and std of out degrees as params, but there is no way to stop it from creating multiple edges unfortunately. What you can do is to take the edges and filter duplicates. Then the graph may not conform to the params it was created with. Its mean degree will be reduced, dev can change etc.  </p>
		<p><code>val gr = GraphGenerators.logNormalGraph(sc,6,1,avgDegree,dev).removeSelfEdges()</code></p>
    		<p><code>println(gr.numVertices+" "+gr.numEdges)</code></p>

    		<p><code>val setA:scala.collection.mutable.Set[(Long,Long)] =scala.collection.mutable.Set[(Long,Long)]()</code></p>
    		<p><code>val j:Array[Edge[Int]] = graph.edges.distinct().collect()</code></p>

    		 
    		<p><code>val gr2: Graph[Int, Int]  = Graph.fromEdges(sc.parallelize(j),defaultValue = 1)</code></p>
     		<p><code>//gr2 is the new cleaned graph</code></p>
		<p><code>println(gr2.numVertices+" "+gr2.numEdges)</code></p>
	</li>
	
</ul>

</body>
</html>
