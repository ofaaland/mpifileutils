#!/bin/bash

. utility/set_funcs.sh

##############################################################################
# Description:
#
#   Check sequential syncs for unexpected changes
#     - after dsync between src and dest, a second dsync copies nothing
#     - after rsync between src and dest, a dsync copies nothing
#     - after dsync between src and dest, a rsync copies nothing
#
# Notes:
#
##############################################################################

# Turn on verbose output
#set -x

MFU_TEST_BIN=${MFU_TEST_BIN:-${1}}
DSYNC_SRC_BASE=${DSYNC_SRC_BASE:-${2}}
DSYNC_DEST_BASE=${DSYNC_DEST_BASE:-${3}}
DSYNC_TREE_NAME=${DSYNC_TREE_NAME:-${4}}

rsync=$(which rsync 2>/dev/null)
if [[ -z $rsync ]]; then
	echo "test_rsync.sh: unable to find rsync in path, exiting"
	exit 1
fi

mpirun=$(which mpirun 2>/dev/null)
mpirun_opts=""
if [[ -n $mpirun ]]; then
	procs=$(( $(nproc ) / 8 ))
	if [[ $procs -gt 16 ]]; then
		procs=16
	fi
	mpirun_opts="-c $procs"

	echo "Using mpirun: $mpirun $mpirun_opts"
fi

echo "Using MFU binaries at: $MFU_TEST_BIN"
echo "Using src parent directory at: $DSYNC_SRC_BASE"
echo "Using dest parent directory at: $DSYNC_DEST_BASE"
echo "Using rsync at: $rsync"

DSYNC_SRC_DIR=$(mktemp --directory ${DSYNC_SRC_BASE}/${DSYNC_TREE_NAME}.XXXXX)
DSYNC_DEST_DIR=$(mktemp --directory ${DSYNC_DEST_BASE}/${DSYNC_TREE_NAME}.XXXXX)

function fs_type()
{
	fname=$1
	df -T ${fname} | awk '$1 != "Filesystem" {print $2}'
}

function list_all_files()
{
	find $1 -printf '%P\n' | sort | grep -v '^$'
}

function dsync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local expectation=$3

	local rc=0
	local result=0
	local dest_type=""

	dsync_output=$(mktemp /tmp/rsync_compare.dsync_output.XXXXX)
	if [[ -n $mpirun ]]; then
		$mpirun $mpirun_opts ${MFU_TEST_BIN}/dsync --delete $srcdir $destdir > $dsync_output 2>&1
	else
		${MFU_TEST_BIN}/dsync --delete $srcdir $destdir > $dsync_output 2>&1
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
	fi

	unexpected_changes=$(mktemp /tmp/rsync_compare.unexpected.XXXXX)
	if [[ $rc -eq 0 && $expectation = "no_change" ]]; then
		grep -E -e "Creating [0-9][0-9]* (files|directories)" -e "Copy data:" -e "Updated [0-9][0-9]* items" $dsync_output > $unexpected_changes
		if [[ $? -eq 0 ]]; then
			result=1
		fi
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $destdir type $dest_type"
	else
		echo "FAILED verify of option $name for $destdir type $dest_type"
		echo =======================
		echo "unexpected changes:"
		cat $unexpected_changes
		echo =======================
	fi

	rm $dsync_output $unexpected_changes

	return $result
}

function rsync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local expectation=$3

	local rc=0
	local result=0
	local dest_type=""

	rsync_output=$(mktemp /tmp/dsync_compare.rsync_output.XXXXX)
	$rsync -av -HAX $srcdir $destdir > $rsync_output 2>&1
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "rsync failed with rc $rc"
		result=1
	fi

	unexpected_changes=$(mktemp /tmp/dsync_compare.unexpected.XXXXX)
	if [[ $rc -eq 0 && $expectation = "no_change" ]]; then
		grep -v -e "^sending incremental" -e "^sent [1-9][0-9,]* bytes" -e "^total size is" -e "^[^0-9a-z]*" $rsync_output > $unexpected_changes
		if [[ $? -eq 0 ]]; then
			result=1
		fi
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $destdir type $dest_type"
	else
		echo "FAILED verify of option $name for $destdir type $dest_type"
		echo =======================
		echo "unexpected changes:"
		cat $unexpected_changes
		echo =======================
	fi

	rm $rsync_output $unexpected_changes

	return $result
}

# after dsync between src and dest, a second dsync copies nothing
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_SRC_DIR/stuff
dsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff initial_sync
dsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff no_change

# after rsync between src and dest, a dsync copies nothing
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_SRC_DIR/stuff
rsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff initial_sync
dsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff no_change

# after dsync between src and dest, a rsync copies nothing
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_SRC_DIR/stuff
dsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff initial_sync
rsync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff no_change

# clean up
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff

exit 0
