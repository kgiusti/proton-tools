This directory contains a tool to benchmark Proton Messenger, and
store the results of the benchmark in a database.  This tool is
intended for Proton developers.  It can be used to monitor the
performance of the implementation over time.  By storing the results
of the benchmarks in a database, a developer can determine if changes
made to the Proton implementation has negatively or positively
affected the performance over time.

RUNNING THE BENCHMARKS
----------------------

This tool expects to be run against a directory containing the full
source distribution of Qpid/Proton - it will not function against
installed packages.

Start by downloading the Proton sources - see
http://qpid.apache.org/proton for the details.

Note well: when building the downloaded Proton sources, you'll want to
build the "Release" version of the code.  Otherwise you'll be
benchmarking unoptimized code, which you probably don't want.
Specifically, when running cmake, set the CMAKE_BUILD_TYPE variable to
'Release'.  Example:

    cmake -DCMAKE_BUILD_TYPE=Release ..

See the instructions for building Proton that comes with the download
for the complete build instructions.

Once Proton is built, you'll need to set up the environment so that
the benchmark is running against the built executables.  To set up the
environment, source the file 'config.sh' that is included in the
Proton source distribution.  Example:

    $ source ./config.sh

You can verify that the environment is correct by checking that the
'msgr-recv' executable is in your path.  Example:

    $ which msgr-recv
    ~/work/proton/qpid-proton/build/tests/tools/apps/c/msgr-recv

Be sure that the msgr-recv that was found is actually the one you
built - this is the executable that is actually benchmarked!

The benchmark tool takes several options (use ./msgr-benchmark --help
to see the complete list).  The most important are:

   --save - Store the results of the benchmark in a database.  By
     default, the results are not stored.

   --db=DB - The name of the database to add the results to.  By
     default, this is '.msgr-db' in the local directory.

   --label=LABEL - the name to assign this set of benchmark results
     when stored in the database.  This label can be used on the
     "X-Axis" when graphing the results of several runs of the
     benchmarks, for example.  By default, the current date and time
     are used.


Example:

Let's assume a developer is preparing a release of Proton, say version
13.  Prior to release, a series of release candidates (rc) are made
available.  The developer can use this tool to create a database of
the benchmark results for each release candidate, then graph the
results.

When the first RC is available, the developer would run the benchmark
and store the results, like so:

./msgr-benchmark --store --db REL_13 --label RC1

This will create the database REL_13 in the current directory, run the
benchmarks, and store the results under the label "RC1".

When the next RC is made available, the developer runs the benchmarks
against that release candidate, and stores the results in the database
using a new label:

./msgr-benchmark --store --db REL_13 --label RC2

Repeat for each new candidate.  This results in a database of relative
benchmark results for the REL_13 release, making it trivial to
determine if there has been a performance degredation (or improvement)
as release candidates become available.

DATABASE FORMAT
---------------

The benchmark tool operates by running a series of tests.  The results
of each test is written to the database.  The database is merely a
directory that contains text files.  Each text file corresponds to a
metric from a test.  Currently the only metrics that are stored are
average latency and message throughput.  So each test may generate up
to two files - one containing latency data, and one containing message
throughput data.  Files containing average latency data are given a
filename in the format "AL_%s.csv", where 'AL_' indicates the file
contents (average latency) and %s is derived from the name of the
test.  Files containing throughput data are given the filename
"T_%s.csv", using the same derivied test name.

All files are text files in Comma Separated Value (csv) format.  Each
line in the file is a record containing the results of one run of the
benchmark.  The format of the record is as follows:

 <label>,<low-value>,<mean-value>,<high-value>

The <label> field is specified by the --label argument passed to the
msgr-benchmark tool.  The remaining fields are determined thusly:

Each test of the benchmark is repeated a number of times (5 by
default, but this can be manually set by the --iterations option).
This results in a set of metrics - one for each run.  The benchmark
tool takes this set and writes the lowest value from the set as the
<low-value> field, the mean of all values as the <mean-value> field,
and the highest value as the <high-value> field.

GRAPHIC REPRESENTATION
----------------------

The graph-results.sh shell script provides an example of how the
database results may be graphed.  It uses gnuplot - a freeware
graphing tool available for most Linux/unix systems - to graph the
contents of a database.  Each test metric is graphed individually,
with each --label value used as the X-Axis values.  Each data point is
graphed as a vertical bar running from the low-value to the
high-value.  Each bar is then interconnected by a horizontal line
graphing the mean-value points.


TODO:
-----

I'd like to see the addition of the following test cases:

o) transfers requiring explicit settlement (as opposed to pre-setted)
o) SSL traffic (using anonymous ciphers - no need for certs)
o) multiple links per connection, and multiple connections
