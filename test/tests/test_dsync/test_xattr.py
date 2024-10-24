#!/usr/bin/env python2
import subprocess

# change paths here for bash script as necessary
mpifu_path     = "/g/g0/faaland1/projects/mpifileutils/test/tests/test_dsync/test_xattr.sh"

# vars in bash script
dsync_test_bin   = "/g/g0/faaland1/projects/mfu-install/bin/dsync"
dsync_src_dir    = "/p/lflood/faaland1/scratch/mfutest/src"
dsync_dest_dir    = "/p/lflood/faaland1/scratch/mfutest/dest"
dsync_test_file  = "file_test_xattr_XXX"

def test_xattr():
        p = subprocess.Popen(["%s %s %s %s %s" % (mpifu_path, dsync_test_bin,
          dsync_src_dir, dsync_dest_dir, dsync_test_file)], shell=True,
          executable="/bin/bash").communicate()
        print(p)
