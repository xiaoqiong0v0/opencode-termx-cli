#!/bin/sh
# 前台运行 sshd，容器生命周期即 sshd 生命周期。
# restart 策略只由 `docker run` 控制（测试用 `--restart=no`）。
set -eu

mkdir -p /run/sshd
exec /usr/sbin/sshd -D -e -p 2222
