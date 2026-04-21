#!/bin/bash

# submit the inverted index job to the MapReduce framework (sequential mode)
go run ii/ii.go master sequential pg-*.txt

# compare a set of well-known words against the reference output
grep -E "^(a|and|of|the|to): " mrtmp.iiseq | diff - ii-testout.txt > diff.out
if [ -s diff.out ]
then
  echo "Failed test. Output should be as in ii-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo -e "\e[32mpassed test case! \e[0m" > /dev/stderr
fi

# clean up
# you could comment out the following two lines if you wish to examine the intermediate outputs
rm mrtmp.*
rm diff.out
