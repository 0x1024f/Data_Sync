#!/bin/sh
set -eu
/usr/sbin/haproxy -c -f /etc/haproxy/haproxy.cfg
systemctl reload haproxy
systemctl is-active --quiet haproxy
