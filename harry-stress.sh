#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

if [ -z "$(ls -A cassandra)" ]; then
    echo -e "\nPlease build harry first using 'make package' before running stress\n"
    exit 1
fi

usage() {
    echo
    echo "usage: harry-stress.sh [args]"
    echo "  -f --flag           Flag last run as a failure (dir name + touch file indicating)"
    echo "  -t --title          Optional title to decorate the results directory with internally in 'title.txt'"
    echo "  -d --duration       Time in ms to run test (default 60000)"
    echo "  -m --minutes        Alternate duration in minutes"
    echo "  -r --read           # Read threads. default 2"
    echo "  -w --write          # Write threads. default 2"
    echo "  -s --seed           seed. Defaults to random in 0-1e13"
    echo "  -c --conf           .yaml config file. Defaults to conf/external.yaml"
    echo "  -v --validate       validation type. Defaults to validate-all"
    echo
    exit 0
}

# Sanity check; confirm harry's been built

title=""
duration=60000
read=2
write=2
seed=`awk -v min=1 -v max=9999999999999 'BEGIN{srand(); print int(min + rand() * (max - min+1))}'`
conf_file="conf/external.yaml"
validate="validate-all"


while test $# -gt 0
    do
    case "$1" in
        -h|--help)
            usage
            exit
            ;;
        -f|--flag)
            TO_FLAG=$(ls -td results/* | head -1)
            echo "Flagging $TO_FLAG as a failure"
            fail_dir=${TO_FLAG}_FAILED
            mv $TO_FLAG $fail_dir
            touch $fail_dir/failure.txt
            echo "See $fail_dir for detailed results"
            exit
            ;;
        -t|--title)
            title=$2
            shift
            ;;
        -d|--duration)
            duration=$2
            shift
            ;;
        -r|--read)
            read=$2
            shift
            ;;
        -w|--write)
            write=$2
            shift
            ;;
        -s|--seed)
            seed=$2
            shift
            ;;
        -c|--conf)
            conf=$2
            shift
            ;;
        -v|--validate)
            validate=$2
            shift
            ;;
        -m|--minutes)
            duration=$(($2 * 1000 * 60))
            shift
            ;;
        *)
            echo "Unknown arg $1"
            usage
            ;;
    esac
    shift
done

java_version=`java -version 2>&1 | grep version | cut -d "\"" -f2`
file_prefix="_s${seed}_r${read}_w${write}_${validate}_JDK${java_version}"
run_time="`date +%Y-%m-%d_%s`"

# Default to verbose run name if not provided by user; can't infer run intent
run_name=${run_time}_$file_prefix
results_dir="results/$run_name"

if [[ ! -z $title ]]; then
    results_dir="results/$title"
fi

if [ -d $results_dir ];
then
    echo "$results_dir already exists; please re-title your test with -t"
    exit -1
fi

mkdir -p $results_dir
touch $results_dir/$title.txt

echo "seed: $seed" > $results_dir/${file_prefix}_seed.txt
perl -pi -e "s/seed:*.*$/seed: $seed/g" $conf_file
echo "Confirming seed replacement in $conf_file" >> $results_dir/${file_prefix}_seed.txt
grep seed $conf_file >> $results_dir/${file_prefix}_seed.txt

export ARGS="$conf_file --read=$read --write=$write --duration=$duration --$validate"
cmd="make -o package stress"
echo Running command: [$cmd ARGS=\"$ARGS\"] >> $results_dir/${file_prefix}_cmd.txt

# TODO: Consider catching return code on stress command and flagging dir as FAILURE automatically
# Do we get a return code from ministress?
$cmd >> $results_dir/${file_prefix}_stress_results.txt 2>&1 &
sleep 1
tail -f $results_dir/*
