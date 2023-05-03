#!/bin/bash

mkdir install
installdir=`pwd`/install

if [[ $# -eq 0 ]]; then
	echo "$0: missing required arg [deps|mfu]"
	exit 1
fi

if [[ "$1" = "deps" ]]; then
	mkdir deps
	cd deps
	wget https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz
	wget https://github.com/llnl/lwgrp/releases/download/v1.0.4/lwgrp-1.0.4.tar.gz
	wget https://github.com/llnl/dtcmp/releases/download/v1.1.4/dtcmp-1.1.4.tar.gz
	wget https://github.com/libarchive/libarchive/releases/download/v3.5.1/libarchive-3.5.1.tar.gz

	tar -zxf libcircle-0.3.0.tar.gz
	cd libcircle-0.3.0
	./configure --prefix=$installdir
	make install
	cd ..

	tar -zxf lwgrp-1.0.4.tar.gz
	cd lwgrp-1.0.4
	./configure --prefix=$installdir
	make install
	cd ..

	tar -zxf dtcmp-1.1.4.tar.gz
	cd dtcmp-1.1.4
	./configure --prefix=$installdir --with-lwgrp=$installdir
	make install
	cd ..

	tar -zxf libarchive-3.5.1.tar.gz
	cd libarchive-3.5.1
	./configure --prefix=$installdir
	make install
	cd ..
	cd ..
fi


if [[ "$1" = "mfu" ]]; then
	mkdir build
	cd build

	mkdir build
	cd build
	cmake -DCMAKE_INSTALL_PREFIX=../install ..
	make -j install
fi

echo $?
