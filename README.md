# COMPASS: Online Sketch-based Query Optimization for In-Memory Databases

# UPDATE: Reproducing results and figures
Follow the detailed instructions from [instructions.pdf](https://github.com/yizenov/compass_optimizer/blob/master/instructions.pdf). </br>

## Table of Contents
1. [Experimental Setup](#setup)
2. [Install MapD Database System](#installation)
3. [Create Database and Load Data](#load)
4. [Download IMDB Dataset](#dataset)
5. [Download Join Order Benchmark](#benchmark)
6. [Pre-build Sketch Templates](#templates)
7. [Run a Query](#runquery)
8. [Questions](#questions)
9. [Acknowledgments](#acknowledgments)
10. [References](#references)
11. [Citation](#citation)

## 1. Experimental Setup <a name="setup"></a>
COMPASS was developed in the following environment:
- Machine: 2x Intel(R) Xeon(R) CPU E5-2660 v4 and NVIDIA Tesla K80
- OS: Ubuntu 16.04 LTS
- CUDA v10, Clang/LLVM

## 2. Install MapD Database System <a name="installation"></a>
COMPASS is built as an extension on top of a clone of [MapD System](https://github.com/omnisci/omniscidb), version 3.6.1 and later it was rebranded to [OmniSciDB](https://www.omnisci.com/). Find location where the project is cloned and follow the commands below:

    unzip compass_query_optimizer-master.zip
    cd compass_query_optimizer-master/mapd-core/
    source scripts/mapd-deps-ubuntu.sh
    cd ThirdParty/librdkafka/
    source /usr/local/mapd-deps/mapd-deps.sh && cd ../../
    mkdir build && cd build
    cmake -DCMAKE_BUILD_TYPE=debug ..
    make clean & make -j 56
    
The number of threads is `56`. COMPASS has hyper parameters such as sketch size in `mapd-core/Catalog/COMPASS_init_variables.txt`. See `variables_details.txt` for more details about COMPASS hyper parameters.

## 3. Create Database and Load Data <a name="load"></a>
All databases created by the user are at `.mapd/data/`. The default user is `mapd`.
    
- Run server: `./bin/mapd_server --data ~/.mapd/data/`
- Run client and create database called `imdb`:

    `./bin/mapdql mapd -u mapd -p HyperInteractive` \
    `CREATE DATABASE imdb (owner = 'mapd');` \
    `\q`
- Run client and create schemas: `cat schematext.sql | ./bin/mapdql imdb -u mapd -p HyperInteractive`
- Stop server `ctrl + c` on keyboard

See `dataset/README.md` for more details about IMDB schemas.

## 4. Download IMDB Dataset <a name="dataset"></a>
The dataset that was used is [Internet Movie Data Base (IMDB)](https://www.imdb.com/). The data is [publicly availabe](ftp://ftp.fu-berlin.de/pub/misc/movies/database/) in text files and [open-source imdbpy package](https://bitbucket.org/alberanid/imdbpy/get/5.0.zip) was used to transform txt files to CSV files in [[1]](#1). See for more details in [here](https://github.com/gregrahn/join-order-benchmark). The 3.6 DB snapshot is from May 2013 and it can be downloaded [here](homepages.cwi.nl/~boncz/job/imdb.tgz). The dataset includes 21 CSV files i.e. 21 relations in total. It also includes queries to create the necessary relations written in schema.sql or schematext.sql files.

## 5. Download Join Order Benchmark <a name="benchmark"></a>
The workload that COMPASS's performance was evaluated based on [Join Order Benchmark(JOB)](http://www-db.in.tum.de/~leis/qo/job.tgz). JOB consists of 113 queries in total including 33 query families that each family queries differ only in selection predicates. See `queries/README.md` for more details about workload.

## 6. Pre-build Sketch Templates <a name="templates"></a>
In the case of relations with no selection predicates, building the sketches can be avoided by using pre-built sketch templates. This is an additional optimization in order to keep the overhead low. See `sketch-templates/README.md` for more details about building templates. We already uploaded the prebuilt sketches for sketch sizes `73 x 1021` on `imdb` workload. However one needs to re-create the templates in case of changing sketch sizes by running the queries in `sketch-templates`.

## 7. Run Query <a name="runquery"></a>
Current implementation is only on GPU part of MapD system. However one can implement COMPASS on CPU mode. In order to run a query follow the instructions below:

- Run server: `./bin/mapd_server --data ~/.mapd/data/`
- Run client: `./bin/mapdql imdb -u mapd -p HyperInteractive`
- Choose GPU mode: `\gpu`
- Turn on COMPASS: `\fpd`
- Paste query from a query file.
- Stop client: `\q`
- Stop server: `ctrl + c` on keyboard
    
## 8. Questions <a name="questions"></a>
If you have questions, please contact:
- Yesdaulet Izenov [yizenov@ucmerced.edu], (https://yizenov.github.io/)
- Asoke Datta [adatta2@ucmerced.edu], (https://asoke26.github.io/adatta2/)
- Florin Rusu [frusu@ucmerced.edu], (https://faculty.ucmerced.edu/frusu/)

## 9. Acknowledgments <a name="acknowledgments"></a>
This work is supported by [NSF award (number 2008815)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2008815&HistoricalAwards=false) and by [the U.S. Department of Energy Early Career Award (DOE Career)](http://ascr-discovery.science.doe.gov/2014/08/leaping-to-exascale/).

## 10. References <a name="references"></a>
<a id="1">[1]</a> [Query optimization through the looking glass, and what we found running the Join Order Benchmark](https://doi.org/10.1007/s00778-017-0480-7)</br>
<a id="2">[2]</a> [Filter Push-Down extension for MapD](https://github.com/junhyungshin/mapd-core-fpd)</br>

## 11. Citation <a name="citation"></a>
```bibtex
@misc{compass-github,
  author = {Yesdaulet Izenov},
  title = "{The COMPASS Query Optimizer}",
  howpublished = "\url{https://github.com/yizenov/compass_query_optimizer}"
}
```
