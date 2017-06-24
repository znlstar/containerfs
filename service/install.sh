mkdir -p /usr/local/bin/
mkdir -p /usr/local/conf/
cp -rf ./cfs-*.service /usr/lib/systemd/system/
cp -rf ./cfs-*.ini /usr/local/conf/
cp -rf ./cfs-metanode /usr/local/bin/
cp -rf ./cfs-datanode /usr/local/bin/
cp -rf ./cfs-volmgr /usr/local/bin/