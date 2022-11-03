# FDN-Monitor [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/Function-Delivery-Network/FDN-Monitor/blob/main/LICENSE)

FDN-Monitor is  a  client-based  python  tool  for  monitoring various serverless clusters deployed with serverless platforms. 
It’s main objective isto  act  as  a  sidecar with every virtual-kubelet node in FDN to collect various metrics from the clusters. However, it can also be independently used without deploying it as part of FDN.

## Currently following serverless frameworks are working as part of FDN-Monitor
- [x] OpenFaaS (Prometheus)
- [x] OpenWhisk (Prometheus)
- [x] Kubernetes Monitoring Data (Prometheus)
- [x] AWS Lambda
- [x] Google Cloud Functions

## Overall Architecture
Overall deployment architecture of FDN-Monitor:
<img src="./docs/diagrams/FDN-Monitor.png"/>

Each FDN-Moitor agent is attached as a contianer within virtual-kubelet node of each cluster. Based on which they pull the metrics from the underneath cluster every 30 seconds (```DEFAULT_LOGGING_PERIOD```) and aggreagte them into InfluxDB. Grafana queries the data from InfluxDB and showcase them in various dashboards. 

1. We deploy InfluxDB and Grafana as docker containers seperate from the FDN-K8s cluster so as to keep stability.
2. FDN-Monitor for private cluster deployment uses two Prometheus deployments (one for Serverless platform and other for K8s cluster) to get metrics.
3. All the dashboards for FDN are configured and present in grafana-provisioning.

> **_NOTE:_**  curl http://localhost:3005/stop to stop collecting data. 

## Getting Started

### 1. Modifying InfluxDB and Grafana credentials
> **_NOTE:_**  It is to be noted that we use InfluxDBv2.
Update ```.env``` file according to following parameters.  

```bash
INFLUXDB_USERNAME=admin
INFLUXDB_PASSWORD=secure_influx_admin_password_fdn
INFLUXDB_ORG=fdn_org
INFLUXDB_BUCKET=fdn_monitoring_bucket
INFLUXDB_ADMIN_TOKEN=a_secure_admin_token_for_admin_fdn
V1_DB_NAME=fdn_monitoring_db
V1_RP_NAME=v1-rp
V1_AUTH_USERNAME=admin
V1_AUTH_PASSWORD=secure_influx_admin_password_fdn

GRAFANA_USERNAME=admin
GRAFANA_PASSWORD=secure_influx_admin_password_fdn
```
### 2. Building FDN-Monitor
> **_NOTE:_**  It is to be noted that afunctiondeliverynetwork/fdn-monitor image is used when deploying.
1. Clone the repository and go to the root directory:

```bash
git clone https://github.com/Function-Delivery-Network/FDN-Monitor.git
cd FDN-Monitor
```

2. Make the changes in app folder 

3. Once changes are made, create a docker image and push it to Docker-Hub. 
```bash
cd FDN-Monitor/app
sudo docker build -t functiondeliverynetwork/fdn-monitor .
sudo docker push functiondeliverynetwork/fdn-monitor
```

### 3. Cluster Parameters (optional)
> **_NOTE:_**  These are specified within the virtual-kubelet when deploying, so update here only if you are deploying locally. 
Rename ```config_*.env``` to ```config.env``` file according to the cluster which you want to monitor.  

For example: 

```bash
CLUSTER_TYPE=OPENFAAS
CLUSTER_NAME=edge_cluster
CLUSTER_AUTH=
CLUSTER_HOST=
CLUSTER_USERNAME=admin
CLUSTER_API_GW_ACCESS_TOKEN=hello
CLUSTER_GATEWAY_PORT=31112
CLUSTER_SERVERLESS_PLATFROM_PROMETHEUS_PORT=30008
CLUSTER_KUBERNETES_PROMETHEUS_PORT=30009
DEFAULT_LOGGING_PERIOD=30
POWER_COLLECTION="True"

INFLUXDB_HOST=influxdb
INFLUXDB_PORT=8086
INFLUXDB_ORG=fdn_org
INFLUXDB_BUCKET=fdn_monitoring_bucket
INFLUXDB_ADMIN_TOKEN=a_secure_admin_token_for_admin_fdn
INFLUXDB_TABLE_INFRA=infra_usage_table
INFLUXDB_TABLE_FUNCTIONS=functions_usage_table
```


### 4. Deploying Locally (optional)

```bash
sudo docker-compose -f docker-compose-fdn.yml -d
```

### Collected Metrics Naming
<img src="./docs/diagrams/collected_metrics.png"/>


## Grafana Dashboard

- **Grafana Dashboard** (http://MASTER_PUBLIC_IP:3001)
### Overall System-Infra metrics dashboard
<img src="./docs/diagrams/grafana-system.png"/>

### Overall Functions metrics dashboard
<img src="./docs/diagrams/grafana-functions.png"/>

### Courier Based on HAProxy metrics dashboard
<img src="./docs/diagrams/courier-stats.png"/>


### Class Diagram of FDN-Monitor
<img src="./docs/diagrams/class_diagram.png"/>

## Help and Contribution

Please add issues if you have a question or found a problem. 

Pull requests are welcome too!

Contributions are most welcome. Please message me if you like the idea and want to contribute. 
