This directory contains a few demos I've developed around the Proton
messaging library.  Nothing really special, just a few bits of code
I've created while learning how to develop messaging applications with
Proton.

More information about the Proton project may be found here:

http://qpid.apache.org/proton

CONTENTS
--------

fortune/ - a simple client/server, REST-like messaging example
banco-de-justin/ - 'Bank of Justin', a transaction-like messaging
                    example using Proton Messenger's delivery state.
benchmark/ - a to benchmark Proton Messenger

BUILDING
--------

To build, create a "build" directory at the same level as this
README.txt file:

  mkdir build
  cd build

If you've installed the proton developer libraries and include files,
you should be able to simply type:

  cmake ..

to generate the Makefiles.

If you don't have the proton developer libraries/includes installed,
you'll need to check out a copy of the proton build tree and build the
C libraries (see the proton project for details) .  Once built, set
the path to the top level directory (the directory that contains
'proton-c') via the "PROTON_SOURCE_DIR" build variable:

  cmake -DPROTON_SOURCE_DIR=/home/kgiusti/work/proton/qpid-proton ..

Once the Makefiles have been generated, simply:

  make


