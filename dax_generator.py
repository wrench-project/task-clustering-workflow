#!/usr/bin/env python

import HTMLParser
import json
import os

from Pegasus.DAX3 import *
from optparse import OptionParser

html_parser = HTMLParser.HTMLParser()

# Options
parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0")
parser.add_option('-d', '--dax', action='store', dest='daxfile', default='wrench-task-clustering.dax', help='DAX filename')
parser.add_option('-c', '--config', action='store', dest='config', help='Simulation configuation file')
(options, args) = parser.parse_args()

with open(options.config) as config_file:
  data = json.load(config_file)

  # Create a abstract dag
  workflow = ADAG("WRENCH-task-clustering")

  # Executables
  e_simulator = Executable('wrench_simulator', arch='x86_64', installed=True)
  e_simulator.addPFN(PFN('file:///simulator/task_clustering_batch_simulator/simulator', 'local'))
  workflow.addExecutable(e_simulator)

  # simulation jobs
  for tf in data['trace_files']:

    output_file = "output-file.json"
    j = Job("wrench_simulator")
    j.addProfile(Profile(Namespace.CONDOR, key="+SingularityImage", value=html_parser.unescape("&quot;/cvmfs/singularity.opensciencegrid.org/wrenchproject/task-clustering:latest&quot;")))
    j.addArguments(tf[1]) # num_compute_nodes
    j.addArguments("/simulator/trace_files/" + tf[0]) # job trace file
    j.addArguments("") # max jobs in system
    j.addArguments("") # workflow specification
    j.addArguments("") # workflow start time
    j.addArguments("") # algorithm
    j.addArguments("") # batch algorithm
    j.addArguments("--wrench-no-log")
    j.addArguments(output_file) # json file to write results to
    j.uses(File(output_file), link=Link.OUTPUT, transfer=True)
    workflow.addJob(j)

  # Write the DAX to file
  f = open(options.daxfile, "w")
  workflow.writeXML(f)
  f.close()
