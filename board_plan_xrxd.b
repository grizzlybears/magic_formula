#!/bin/bash -e

MY_PATH=$0
MY_DIR=`dirname $MY_PATH`
TODAY=`date +%F`

pushd `pwd`
cd $MY_DIR

REPORT_YEAR=2018

DB=working_dir/mf.db 

OUT_CSV_BASE=working_dir/xrxd_research
OUT_CSV=${OUT_CSV_BASE}.csv

OUT_CSV_T_BASE=${OUT_CSV_BASE}.${TODAY}

OUT_CSV_T=${OUT_CSV_T_BASE}.csv
OUT_CSV_T_GB=${OUT_CSV_T_BASE}.gb.csv

#TO=grizzlybears@163.com
TO=phil_pan@hotmail.com
CC=WolfgangJohnson@outlook.com

rm -f ${DB}

./mf fetch_xrxd_b $REPORT_YEAR 

./mf sum_xrxd2

mv  $OUT_CSV $OUT_CSV_T 

iconv -t GBK $OUT_CSV_T > $OUT_CSV_T_GB 

rm  $OUT_CSV_T 


echo "This is board plan data of ${TODAY}." | mailx -s "Board plan of ${TODAY}" -a $OUT_CSV_T_GB -c $CC $TO

popd



