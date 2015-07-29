#!/bin/bash

# toft
svn co https://svn.baidu.com/myspace/st/public/qinan/toft

# pbrpc
git clone https://github.com/anqin/trident

# thirdparty
svn co https://svn.baidu.com/myspace/st/public/qinan/thirdparty
cd thirdparty
get_codes.sh
cd -
