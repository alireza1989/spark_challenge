## Problem

### To setup the spark cluster goto Environment Setup below this section.

## Problems

0. `Cleanup`: A sample dataset of request logs is given in `data/DataSample.csv`. We consider records that have identical `geoinfo` and `timest` as suspicious. Please clean up the sample dataset by filtering out those suspicious request records.
1. `Label`: assign each _request_ (from `data/DataSample.csv`) to one of the _POI locations_ (from `data/POIList.csv`) that has minimum distance to the request location.
2. `Analysis`:
    1. With respect to each _POI location_, calculate the average and standard deviation of distance between the _POI location_ to each of its assigned _requests_.
    2. Draw circles that are centered at each of the POIs, each includes all _requests_ assigned to the _POI location_. Find out the radius and density (i.e. request count/circle area) for each _POI location_.
3. `Model`:
    1. To visualize the popularity of the _POI locations_, we need to map the density of POIs into a scale from -10 to 10. Please give a mathematical model to implement the mapping. (Hint: please take consideration about extreme case/outliers, and aim to be more sensitive around mean average to give maximum visual differentiability.)
    2. Try to come up with some reasonable hypotheses regarding POIs, state all assumptions, testing steps and conclusions. Include this as a text file (with a name `bonus`) in your final submission


----

## Environment Setup

Unless you already have a working [Apache Spark](http://spark.apache.org/) cluster, you will need to have [Docker](https://docs.docker.com/) for simple environment setup.

The provided `docker-compose.yml` and Spark configurations in `conf` directory are cloned from <https://github.com/gettyimages/docker-spark>.

## Setup

0. Make sure Docker is installed properly and `docker-compose` is ready to use
1. Run `$ docker-compose up -d` under the `data-mr` directory
2. Check Spark UI at `http://localhost:8080` and you should see 1 master and 1 worker
3. Run `$ docker exec -it datamr_master_1 /bin/bash` to get into the container shell, and start utilizing Spark commands such as `# spark-shell`, `# pyspark` or `# spark-submit`. You may want to replace `datamr_master_1` with actual container name that's spawned by the `docker-compose` process

![demo.gif](https://user-images.githubusercontent.com/2837532/27649289-4fdffd52-5bff-11e7-9236-0a1d063461cb.gif)

## Notes on working through the problems

If you're not already familiar with [Apache Spark](http://spark.apache.org/), you'll need to go through its documentation for available APIs. The version that comes with the Docker Spark setup depends on https://github.com/gettyimages/docker-spark.

For jobs that rely on external dependencies and libraries, make sure they are properly packaged on submission.

On submission, we will need:

1. Source code of the solution
2. Build instructions for job packaging (unless your solution is a single `.py`), such as [Maven](https://maven.apache.org/) or [SBT](http://www.scala-sbt.org/) for Scala/Java, or `setup.py` for Python `.zip/.egg`

Make sure the jobs can be submitted (through `spark-submit` command) in the Spark Master container shell. There is a `data` directory provided that maps between the Spark Master container and your host system, which is accessible as `/tmp/data` within the Docker container -- this is where you want to place both your jobs and work sample data, the latter is already included.

------
