sudo apt-get -y remove --purge \
             mysql-server-5.6 \
             mysql-server-core-5.6 \
             mysql-client-5.6 \
             mysql-client-core-5.6
sudo rm -rf /var/lib/mysql
sudo apt-get install software-properties-common
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xcbcb082a1bb943db
sudo add-apt-repository 'deb [arch=amd64,i386,ppc64el] http://download.nus.edu.sg/mirror/mariadb/repo/10.2/ubuntu trusty main'
sudo apt-get update
sudo apt-get install mariadb-server
