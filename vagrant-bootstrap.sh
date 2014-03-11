#! /bin/bash


apt-get update
apt-get install -y python-pip make


# Build Redis

vendor_dir=/vagrant/vendor

mkdir $vendor_dir
pushd $vendor_dir
wget  http://download.redis.io/redis-stable.tar.gz
tar xzf redis-stable.tar.gz
cd redis-stable
make

redis_dir=$vendor_dir/redis-stable

ln -sf $redis_dir/src/redis-server /usr/local/bin
ln -sf $redis_dir/src/redis-cli /usr/local/bin

popd


# Install Redis

mkdir /etc/redis
mkdir /var/redis
mkdir /var/redis/6379

cp $redis_dir/utils/redis_init_script /etc/init.d/redis_6379
sed <$redis_dir/redis.conf >/etc/redis/6379.conf \
    -e 's/^daemonize no/daemonize yes/' \
    -e 's,^pidfile /var/run/redis.pid,pidfile /var/run/redis_6379.pid,' \
    -e 's,^logfile "",logfile /var/log/redis_6379.log,' \
    -e 's,^dir ./,dir /var/redis/6379/,'

update-rc.d redis_6379 defaults

/etc/init.d/redis_6379 start


# Now install the Python dependencies

pip install -r requirements.txt
