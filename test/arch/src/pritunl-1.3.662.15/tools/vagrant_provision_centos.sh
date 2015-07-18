#!/usr/bin/env bash
wget http://mirrors.rit.edu/fedora/epel/7/x86_64/e/epel-release-7-2.noarch.rpm
rpm -i epel-release-7-2.noarch.rpm

yum install -y gcc rpm-build redhat-rpm-config python python-virtualenv python-setuptools python2-devel libffi-devel

mkdir /usr/lib/pritunl
chown vagrant:vagrant /usr/lib/pritunl
