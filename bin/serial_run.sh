#!/bin/bash
#set -xv

##########
# This script executes 3 jobs in different clusters, 
# then aggregates the data to one cluster then execute one last time.
#
# The A is copied from C1 to C2, and C3, a job is executed in C1, C2. 
# At the end, the digests are are generated and compared. 
# If they are equal (which they will be because the tests don’t tamper the digests), C1 and C2 executed successfully.
# Similar scenario using the data B.
# Then, we copy the result of data B into C1, C2, C3 (maybe not all the copies are necessary 
# because there are clusters that already have the data), and aggregate the result by executing 
# job aggregation in C1, and C2. Compute the digests and compare the results.
##########


waiting () {
    # wait for 3 pids end
    wait "$1" || echo "first failed"
    wait "$2" || echo "second failed"
}

waiting3 () {
    # wait for 3 pids end
    wait "$1" || echo "first failed" 
    wait "$2" || echo "second failed" 
    wait "$3" || echo "third failed"
}

xxxdiff() {
    ((diff_sec=$2-$1))
    echo - | awk '{printf "%d:%d:%d","'"$diff_sec"'"/(60*60),"'"$diff_sec"'"%(60*60)/60,"'"$diff_sec"'"%60}'
    echo ""
    echo "$3: $diff_sec"
}


# launch() {
#     # ssh SSH_USER@$HOST1 "$SCRIPTS_DIR/distcp.sh $HOST1:3888/gutenberg-output/ $HOST2:3888/" &
#     $( ssh "$1" "$2" & )
#     echo "$!"
# }

validation() {
    # compare digests
    if [[ "$1" == $2 ]]; then
	echo "Matching sums"
    else
	echo  "WARNING: Digests are different." >&2; 
	exit 1;
    fi
}


# ssh host1 ls & pid1=$!; ssh host2 ls & pid2=$!; wait "$pid1" || echo
#	 first failed; wait "$pid2" || echo "second failed"

gdate1=$(date +"%s")
# Step 1
echo "Copying data to replicate"
date1=$(date +"%s")

ssh "$SSH_USER"@"$HOST1" "$SCRIPTS_DIR"/distcp.sh "$HOST1":3888"$INPUT1" "$HOST2":3888/ &
pid1="$!"
ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/distcp.sh "$HOST3":3888"$INPUT2" "$HOST4":3888/ &
pid2="$!"
ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/distcp.sh "$HOST4":3888"$INPUT3" "$HOST3":3888/ &
pid3="$!"

waiting3 "$pid1" "$pid2" "pid3"
date2=$(date +"%s")

xxxdiff $date1 $date2 "Copy data between namenodes time"


# Step 2
echo "Execution of jobs"
date1=$(date +"%s")
ssh "$SSH_USER"@"$HOST1" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT1" "$OUTPUT1" &
pid1=$!
ssh "$SSH_USER"@"$HOST2" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT1" "$OUTPUT1" &
pid2=$!
ssh "$SSH_USER"@"$HOST3" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT2" "$OUTPUT2" &
pid3=$!
ssh "$SSH_USER"@"$HOST4" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT2" "$OUTPUT2" &
pid4=$!
ssh "$SSH_USER"@"$HOST3" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT3" "$OUTPUT3" &
pid5="$!"
ssh "$SSH_USER"@"$HOST4" hadoop jar "$HADOOP_PATH"/countlines.jar "$INPUT3" "$OUTPUT3" &
pid6="$!"

waiting "$pid1" "$pid2"
waiting "$pid3" "$pid4"
waiting "$pid5" "$pid6"

date2=$(date +"%s")

xxxdiff $date1 $date2 "Job execution time"


# Step 3
echo "Generate digests"
date1=$(date +"%s")
digest1=$( ssh "$SSH_USER"@"$HOST1" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT1"/part* )
digest2=$( ssh "$SSH_USER"@"$HOST2" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT1"/part* )
digest3=$( ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT2"/part* )
digest4=$( ssh "$SSH_USER"@"$HOST4" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT2"/part* )
digest5=$( ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT3"/part* )
digest6=$( ssh "$SSH_USER"@"$HOST4" "$SCRIPTS_DIR"/generatedigests.sh "$OUTPUT3"/part* )

date2=$(date +"%s")

xxxdiff $date1 $date2 "Generate digests execution time"

echo ">> $digest1 = $digest2"
echo ">> $digest3 = $digest4"
echo ">> $digest5 = $digest6"

validation "$digest1" "$digest2"
validation "$digest3" "$digest4"
validation "$digest5" "$digest6"

# Step 4
echo "Execution of copying data to aggregate"
date1=$(date +"%s")
ssh "$SSH_USER"@"$HOST2" "$SCRIPTS_DIR"/distcp.sh "$HOST1":3888"$OUTPUT1" "$HOST3":3888/ &
pid1=$!
ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/distcp.sh "$HOST3":3888"$OUTPUT2" "$HOST1":3888/ &
pid2=$!
ssh "$SSH_USER"@"$HOST4" "$SCRIPTS_DIR"/distcp.sh "$HOST4":3888"$OUTPUT3" "$HOST1":3888/ &
pid3=$!

waiting3 "$pid1" "$pid2" "$pid3"

date2=$(date +"%s")
xxxdiff $date1 $date2 "Copy data between namenodes time"

# Step 5
echo "Execution of aggregation job"
date1=$(date +"%s")
ssh "$SSH_USER"@"$HOST1" hadoop jar "$HADOOP_PATH"/countlinesaggregator.jar "$OUTPUT1","$OUTPUT2","$OUTPUT3" "$AOUTPUT" &
pid1=$!
ssh "$SSH_USER"@"$HOST3" hadoop jar "$HADOOP_PATH"/countlinesaggregator.jar "$OUTPUT1","$OUTPUT2","$OUTPUT3" "$AOUTPUT" &
pid2=$!

waiting "$pid1" "$pid2"

date2=$(date +"%s")
xxxdiff $date1 $date2 "Aggregation execution time"


# Step 6
echo "Generate digests on aggregation jobs"
date1=$(date +"%s")
digest1=$( ssh "$SSH_USER"@"$HOST1" "$SCRIPTS_DIR"/generatedigests.sh "$AOUTPUT"/part* )
digest2=$( ssh "$SSH_USER"@"$HOST3" "$SCRIPTS_DIR"/generatedigests.sh "$AOUTPUT"/part* )
date2=$(date +"%s")

xxxdiff $date1 $date2 "Generate digests execution time"

echo ">> $digest1 = $digest2"

#validation "$digest1" "$digest2"

gdate2=$(date +"%s")

xxxdiff $gdate1 $gdate2 "Global execution time"
