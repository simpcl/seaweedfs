#!/bin/bash
time=$(date "+%Y-%m-%d")
current_git_branch_latest_short_id=`git rev-parse --short HEAD`
sudo docker build --network=host -t harbor-registry.inner.youdao.com/infra/minio/seaweedfs:prod-$time-$current_git_branch_latest_short_id .
sudo docker push harbor-registry.inner.youdao.com/infra/minio/seaweedfs:prod-$time-$current_git_branch_latest_short_id