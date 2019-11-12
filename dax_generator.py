#!/usr/bin/env python

import hashlib
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
  e_simulator.addProfile(Profile(Namespace.PEGASUS, key="clusters.size", value=100))
  workflow.addExecutable(e_simulator)

  subwf_dir = "subwfs"
  os.mkdir(subwf_dir)

  # simulation jobs
  for wf in data['workflows']:

    # generate sub workflow per workflow
    subwf_id = "subwf_" + wf.replace(".dax", "")
    subwf = ADAG(subwf_id)

    for tf in data['trace_files']:
      for max_sys_jobs in data['max_sys_jobs']:
        for st in data['start_times']:
          for alg in data['algorithms']:
            output_file = wf + "_" + hashlib.md5(tf[1] + tf[0] + max_sys_jobs + st + alg).hexdigest() + ".json"
            j = Job("wrench_simulator")
            j.addProfile(Profile(Namespace.CONDOR, key="+SingularityImage", value=html_parser.unescape("&quot;/cvmfs/singularity.opensciencegrid.org/wrenchproject/task-clustering:latest&quot;")))
            j.addArguments(tf[1]) # num_compute_nodes
            j.addArguments(data['trace_file_dir'] + "/" + tf[0]) # job trace file
            j.addArguments(max_sys_jobs) # max jobs in system
            j.addArguments(data['workflow_type'] + ":" + data['workflow_dir'] + "/" + wf) # workflow specification
            j.addArguments(st) # workflow start time
            j.addArguments(alg) # algorithm
            j.addArguments("conservative_bf") # batch algorithm
            j.addArguments("--wrench-no-log")
            j.addArguments(output_file) # json file to write results to
            j.uses(File(output_file), link=Link.OUTPUT, transfer=True)
            subwf.addJob(j)
    
    # write subworkflow DAX file
    with open(subwf_dir + "/" + subwf_id + ".xml", "w") as subwf_out:
      subwf.writeXML(subwf_out)
    
    subwf_dax = File(subwf_id + ".xml")
    subwf_dax.addPFN(PFN("file://" + os.getcwd() + "/" + subwf_dir + "/" + subwf_id + ".xml", "local"))
    workflow.addFile(subwf_dax)

    subwf_job = DAX(subwf_id + ".xml", id=subwf_id)
    subwf_job.addProfile(Profile("dagman", "CATEGORY", "subwf"))
    subwf_job.uses(subwf_dax)
    subwf_job.addArguments("-Dpegasus.catalog.site.file=" + os.getcwd() + "/sites.xml",
                           "--sites", "condor_pool",
                           "--output-site", "local",
                           "--cluster", "horizontal",
                           "--cleanup", "inplace")
    workflow.addDAX(subwf_job)

  # Write the DAX to file
  f = open(options.daxfile, "w")
  workflow.writeXML(f)
  f.close()
