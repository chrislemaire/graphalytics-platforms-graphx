/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.graphx

import nl.tudelft.graphalytics.graphx.pr.PageRankJob
import nl.tudelft.graphalytics.graphx.sssp.SingleSourceShortestPathJob
import nl.tudelft.graphalytics.{PlatformExecutionException, Platform}
import nl.tudelft.graphalytics.domain._
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import nl.tudelft.graphalytics.graphx.bfs.BreadthFirstSearchJob
import nl.tudelft.graphalytics.graphx.cdlp.CommunityDetectionLPJob
import nl.tudelft.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
import nl.tudelft.graphalytics.graphx.ffm.ForestFireModelJob
import nl.tudelft.graphalytics.graphx.lcc.LocalClusteringCoefficientJob

/**
 * Constants for GraphXPlatform
 */
object GraphXPlatform {
	val OUTPUT_DIRECTORY = "benchmark.run.output-directory"
	val HDFS_DIRECTORY_KEY = "hadoop.hdfs.directory"
	val HDFS_DIRECTORY = "graphalytics"

	val CONFIG_PATH = "graphx.properties"
	val CONFIG_JOB_NUM_EXECUTORS = "graphx.job.num-executors"
	val CONFIG_JOB_EXECUTOR_MEMORY = "graphx.job.executor-memory"
	val CONFIG_JOB_EXECUTOR_CORES = "graphx.job.executor-cores"
}

/**
 * Graphalytics Platform implementation for GraphX. Manages the datasets on HDFS and launches the appropriate
 * GraphX jobs.
 */
class GraphXPlatform extends Platform {
	import GraphXPlatform._

	var pathsOfGraphs : Map[String, (String, String)] = Map()

	/* Parse the GraphX configuration file */
	val config = Properties.fromFile(CONFIG_PATH).getOrElse(Properties.empty())
	System.setProperty("spark.executor.cores", config.getString(CONFIG_JOB_EXECUTOR_CORES).getOrElse("1"))
	System.setProperty("spark.executor.memory", config.getString(CONFIG_JOB_EXECUTOR_MEMORY).getOrElse("2g"))
	System.setProperty("spark.executor.instances", config.getString(CONFIG_JOB_NUM_EXECUTORS).getOrElse("1"))

	val hdfsDirectory = config.getString(HDFS_DIRECTORY_KEY).getOrElse(HDFS_DIRECTORY)

	def uploadGraph(graph : Graph) = {
		val localVertexPath = new Path(graph.getVertexFilePath)
		val localEdgePath = new Path(graph.getEdgeFilePath)
		val hdfsVertexPath = new Path(s"$hdfsDirectory/$getName/input/${graph.getName}.v")
		val hdfsEdgePath = new Path(s"$hdfsDirectory/$getName/input/${graph.getName}.e")

		val fs = FileSystem.get(new Configuration())
		fs.copyFromLocalFile(localVertexPath, hdfsVertexPath)
		fs.copyFromLocalFile(localEdgePath, hdfsEdgePath)
		fs.close()

		pathsOfGraphs += (graph.getName -> (hdfsVertexPath.toUri.getPath, hdfsEdgePath.toUri.getPath))
	}

	def executeAlgorithmOnGraph(benchmark : Benchmark) : PlatformBenchmarkResult = {
		val graph = benchmark.getGraph
		val algorithmType = benchmark.getAlgorithm
		val parameters = benchmark.getAlgorithmParameters
		try  {
			val (vertexPath, edgePath) = pathsOfGraphs(graph.getName)
			val outPath = s"$hdfsDirectory/$getName/output/${algorithmType.name}-${graph.getName}"
			val format = graph.getGraphFormat

			val job = algorithmType match {
				case Algorithm.BFS => new BreadthFirstSearchJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.CDLP => new CommunityDetectionLPJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.WCC => new WeaklyConnectedComponentsJob(vertexPath, edgePath, format, outPath)
				case Algorithm.FFM => new ForestFireModelJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.LCC => new LocalClusteringCoefficientJob(vertexPath, edgePath, format, outPath)
				case Algorithm.PR => new PageRankJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.SSSP => new SingleSourceShortestPathJob(vertexPath, edgePath, format, outPath, parameters)
				case x => throw new IllegalArgumentException(s"Invalid algorithm type: $x")
			}

			if (job.hasValidInput) {
				job.runJob()

				// TODO: fetch output from hdfs. This should not be in this section!
				val fs = FileSystem.get(new Configuration())
				fs.copyToLocalFile(new Path(outPath), new Path(OUTPUT_DIRECTORY))
				fs.close()

				// TODO: After executing the job, any intermediate and output data should be
				// verified and/or cleaned up. This should preferably be configurable.
				new PlatformBenchmarkResult(NestedConfiguration.empty())
			} else {
				throw new IllegalArgumentException("Invalid parameters for job")
			}

		} catch {
			case e : Exception => throw new PlatformExecutionException("GraphX job failed with exception: ", e)
		}
	}

	def deleteGraph(graphName : String) = {
		// TODO: Delete graph data from HDFS to clean up. This should preferably be configurable.
	}

	def getName : String = "graphx"

	def getPlatformConfiguration: NestedConfiguration =
		try {
			val configuration: PropertiesConfiguration = new PropertiesConfiguration("graphx.properties")
			NestedConfiguration.fromExternalConfiguration(configuration, "graphx.properties")
		}
		catch {
			case ex: ConfigurationException => NestedConfiguration.empty
		}
}
