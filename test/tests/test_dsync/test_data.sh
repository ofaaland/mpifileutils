#!/bin/bash

##############################################################################
# Description:
#
#   Verify dsync handles data
#     - file data is identical after a copy
#     - file with differing data is copied if --contents arg is used
#     - file with differing data is not copied if --contents arg is not used and metadata match
#
# Notes:
#   - does not test whether data copies are spread across nodes/tasks evenly
#
##############################################################################

# Turn on verbose output
#set -x

MFU_TEST_BIN=${MFU_TEST_BIN:-${1}}
DSYNC_SRC_DIR=${DSYNC_SRC_DIR:-${2}}
DSYNC_DEST_DIR=${DSYNC_DEST_DIR:-${3}}
DSYNC_TMP_FILE=${DSYNC_TMP_FILE:-${4}}

echo "Using MFU binaries at: $MFU_TEST_BIN"
echo "Using src directory at: $DSYNC_SRC_DIR"
echo "Using dest directory at: $DSYNC_DEST_DIR"
echo "Using directory tree: $DSYNC_TMP_FILE"

DSYNC_SRC_DIR=$(mktemp --directory ${DSYNC_SRC_BASE}/${DSYNC_TREE_NAME}.XXXXX)
DSYNC_DEST_DIR=$(mktemp --directory ${DSYNC_DEST_BASE}/${DSYNC_TREE_NAME}.XXXXX)

function fs_type()
{
	fname=$1
	df -T ${fname} | awk '$1 != "Filesystem" {print $2}'
}

function sum_all_files()
{
	pushd $1 >/dev/null
	find . -type f -print0 | xargs --no-run-if-empty -0 md5sum | sort -k2
	popd >/dev/null
}

function sync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local name=$3
	local expectation=$4

	local result=0
	local dest_type=""

	if [[ ! -d $destdir ]]; then
		echo "sync_and_verify: test assumes src $srcdir and dest $destdir both exist"
		exit 1
	fi

	src_sum=$(mktemp /tmp/sync_and_verify.src.XXXXX)
	sum_all_files $srcdir > $src_sum

	dest_sum=$(mktemp /tmp/sync_and_verify.dest.XXXXX)
	sum_all_files $destdir > $dest_sum

	dest_type=$(fs_type $destdir)

	quiet_opt="--quiet"
	delete_opt=""

	if [[ -n $mpirun ]]; then
		$mpirun $mpirun_opts $DSYNC_TEST_BIN $quiet_opt $delete_opt $srcdir $destdir
	else
		$DSYNC_TEST_BIN $quiet_opt $delete_opt $srcdir $destdir
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
	fi

	if [[ $result -eq 0 ]]; then
		after_sum=$(mktemp /tmp/sync_and_verify.after.XXXXX)
		sum_all_files $destdir > $after_sum

		expected_sum=$(mktemp /tmp/sync_and_verify.expected.XXXXX)

		case $expectation in
		  "union")
			cat $src_sum $dest_sum | sort -k2 | uniq > $expected_sum
			;;
		  "src_exactly")
			cat $src_sum > $expected_sum
			;;
		esac

		diff $after_sum $expected_sum
		result=$?
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $destdir type $dest_type"
	else
		echo "FAILED verify of option $name for $destdir type $dest_type - sets differ"
		echo =======================
		echo "before: src_sum"
		cat $src_sum
		echo
		echo "before: dest_sum"
		cat $dest_sum
		echo
		echo "after: after_sum:"
		cat $after_sum
		echo
		echo "expected:"
		cat $expected_sum
		echo =======================
	fi

	rm $src_sum $dest_sum $after_sum $expected_sum

	return $result
}

# file data is identical after a copy
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff

pushd $DSYNC_SRC_DIR/stuff

# args expected by dfilemaker (creates trees, files all different data)
$MFU_TEST_BIN/dfilemaker --items 5000-6000 --depth 5-6 --size 1MB-25MB
popd
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff new_files_checksum union

# file with differing data is copied if --contents arg is used
# not implemented

# file with differing data is not copied if --contents arg is not used and metadata match
# not implemented

# clean up
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff

exit 0
