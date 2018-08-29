sudo apt-get install software-properties-common
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xcbcb082a1bb943db
sudo add-apt-repository 'deb https://downloads.mariadb.com/MariaDB/mariadb-10.2.14/repo/ubuntu trusty main'
sudo apt-get update -q
sudo apt-get install -q -y --force-yes -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" mariadb-server
