# COMPASS: Online Sketch-based Query Optimization for In-Memory Databases

# UPDATE: Reproducing results and figures
Follow the detailed instructions from [instructions.pdf](https://github.com/yizenov/compass_optimizer/blob/master/instructions.pdf). </br>

# Table of Contents
- [Install MapD Database System](#installation)
- [Create Database and Load Data](#load)
- [Download IMDB Dataset](#dataset)
- [Download Join Order Benchmark](#benchmark)
- [Pre-build Sketch Templates](#templates)
- [Run a Query](#runquery)

# Experimental Setup
COMPASS was developed in the following environment:
- Machine: 2x Intel(R) Xeon(R) CPU E5-2660 v4 and NVIDIA Tesla K80
- OS: Ubuntu 16.04 LTS
- CUDA v10, Clang/LLVM

# 1. Install MapD Database System
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

# 2. Create Database and Load Data
All databases created by the user are at `.mapd/data/`. The default user is `mapd`.
    
- Run server: `./bin/mapd_server --data ~/.mapd/data/`
- Run client and create database called `imdb`:

    `./bin/mapdql mapd -u mapd -p HyperInteractive` \
    `CREATE DATABASE imdb (owner = 'mapd');` \
    `\q`
- Run client and create schemas: `cat schematext.sql | ./bin/mapdql imdb -u mapd -p HyperInteractive`
- Stop server `ctrl + c` on keyboard

See `dataset/README.md` for more details about IMDB schemas.

# 3. Download IMDB Dataset
The dataset that was used is [Internet Movie Data Base (IMDB)](https://www.imdb.com/). The data is [publicly availabe](ftp://ftp.fu-berlin.de/pub/misc/movies/database/) in text files and [open-source imdbpy package](https://bitbucket.org/alberanid/imdbpy/get/5.0.zip) was used to transform txt files to CSV files in [1]. See for more details in [here](https://github.com/gregrahn/join-order-benchmark). The 3.6 DB snapshot is from May 2013 and it can be downloaded [here](homepages.cwi.nl/~boncz/job/imdb.tgz). The dataset includes 21 CSV files i.e. 21 relations in total. It also includes queries to create the necessary relations written in schema.sql or schematext.sql files.

# 4. Download Join Order Benchmark
The workload that COMPASS's performance was evaluated based on [Join Order Benchmark(JOB)](http://www-db.in.tum.de/~leis/qo/job.tgz). JOB consists of 113 queries in total including 33 query families that each family queries differ only in selection predicates. See `queries/README.md` for more details about workload.

# 5. Pre-build Sketch Templates
In the case of relations with no selection predicates, building the sketches can be avoided by using pre-built sketch templates. This is an additional optimization in order to keep the overhead low. See `sketch-templates/README.md` for more details about building templates. We already uploaded the prebuilt sketches for sketch sizes `73 x 1021` on `imdb` workload. However one needs to re-create the templates in case of changing sketch sizes by running the queries in `sketch-templates`.

# 6. Run Query
Current implementation is only on GPU part of MapD system. However one can implement COMPASS on CPU mode. In order to run a query follow the instructions below:

- Run server: `./bin/mapd_server --data ~/.mapd/data/`
- Run client: `./bin/mapdql imdb -u mapd -p HyperInteractive`
- Choose GPU mode: `\gpu`
- Turn on COMPASS: `\fpd`
- Paste query from a query file.
- Stop client: `\q`
- Stop server: `ctrl + c` on keyboard
    
# Questions
If you have questions, please contact:
- Yesdaulet Izenov [yizenov@ucmerced.edu], (https://yizenov.github.io/yizenov/)
- Asoke Datta [adatta2@ucmerced.edu], (https://asoke26.github.io/adatta2/)
- Florin Rusu [frusu@ucmerced.edu], (https://faculty.ucmerced.edu/frusu/)

# Acknowledgments
This work is supported by [NSF award (number 2008815)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2008815&HistoricalAwards=false) and by [the U.S. Department of Energy Early Career Award (DOE Career)](http://ascr-discovery.science.doe.gov/2014/08/leaping-to-exascale/).

# Links:
1.  [Query optimization through the looking glass, and what we found running the Join Order Benchmark](https://doi.org/10.1007/s00778-017-0480-7)
2.  [Filter Push-Down extension for MapD](https://github.com/junhyungshin/mapd-core-fpd)
