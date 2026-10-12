#!/usr/bin/env bash
set -euo pipefail
NODE_IP=$(ip -4 -o addr show tailscale0 | awk '{split($4,a,"/"); print a[1]}')
[[ -n "$NODE_IP" ]] || { echo 'Login Tailscale before starting Spark.'; exit 1; }
export SPARK_LOCAL_IP="$NODE_IP"
export SPARK_DRIVER_HOST="$NODE_IP"
export SPARK_HDFS_DATANODE_HOSTNAME=true
case "${1:-worker}" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master --host "$NODE_IP" --port 7077 --webui-port 8080 ;;
  worker)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker --host "$NODE_IP" --port 7078 --webui-port 8081 --cores 2 --memory 4g "spark://${MASTER_IP:?Set MASTER_IP}:7077" ;;
  check|train)
    mode=$1; shift
    args=()
    [[ "$mode" == check ]] && args+=(--check-dependencies)
    if [[ "$mode" == train ]]; then
      export SPARK_MODEL_BASE_URI="hdfs://${MASTER_IP:?Set MASTER_IP}:9000/flight/models"
    fi
    exec python src/spark_ml/run_end_to_end_spark.py --master "spark://${MASTER_IP:?Set MASTER_IP}:7077" --driver-host "$NODE_IP" "${args[@]}" "$@" ;;
  *) exec "$@" ;;
esac
