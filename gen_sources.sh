#!/bin/bash

set -e

cd templates

rm -f *.class

javac --release 25 GenerateSources.java Preprocessor.java

java GenerateSources . ..