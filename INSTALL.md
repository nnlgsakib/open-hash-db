# OpenHashDB Installation and Deployment Guide

**Author:** Manus AI  
**Date:** July 28, 2025  
**Version:** 1.0.0

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation Methods](#installation-methods)
3. [Configuration](#configuration)
4. [Deployment Scenarios](#deployment-scenarios)
5. [Security Considerations](#security-considerations)
6. [Monitoring and Maintenance](#monitoring-and-maintenance)
7. [Troubleshooting](#troubleshooting)

## System Requirements

### Minimum Requirements

- **Operating System:** Linux (Ubuntu 20.04+, CentOS 8+, Debian 10+), macOS 10.15+, Windows 10+
- **CPU:** 2 cores, 2.0 GHz
- **Memory:** 4 GB RAM
- **Storage:** 10 GB available disk space
- **Network:** Stable internet connection for P2P networking

### Recommended Requirements

- **Operating System:** Linux (Ubuntu 22.04 LTS recommended)
- **CPU:** 4+ cores, 3.0 GHz
- **Memory:** 8+ GB RAM
- **Storage:** 100+ GB SSD storage
- **Network:** High-bandwidth connection with low latency

### Software Dependencies

- **Go:** Version 1.21 or later
- **Git:** For source code management
- **Build tools:** GCC or equivalent C compiler

## Installation Methods

### Method 1: Build from Source (Recommended)

This method provides the most control and ensures you have the latest version.

#### Step 1: Install Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y git build-essential
```

**CentOS/RHEL:**
```bash
sudo yum update
sudo yum groupinstall -y "Development Tools"
sudo yum install -y git
```

**macOS:**
```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Git
brew install git
```

#### Step 2: Install Go

**Linux:**
```bash
# Download and install Go 1.21
wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz

# Add Go to PATH
echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify installation
go version
```

**macOS:**
```bash
# Using Homebrew
brew install go

# Verify installation
go version
```

**Windows:**
1. Download Go installer from https://golang.org/dl/
2. Run the installer and follow the prompts
3. Verify installation in Command Prompt: `go version`

#### Step 3: Clone and Build OpenHashDB

```bash
# Clone the repository
git clone <repository-url>
cd openhash

# Download dependencies
go mod download

# Build the application
go build -o openhash .

# Verify build
./openhash --help
```

#### Step 4: Install System-wide (Optional)

**Linux/macOS:**
```bash
# Copy binary to system PATH
sudo cp openhash /usr/local/bin/

# Verify system installation
openhash --help
```

**Windows:**
1. Copy `openhash.exe` to a directory in your PATH
2. Or add the build directory to your system PATH

### Method 2: Docker Installation

Docker provides an isolated environment and simplified deployment.

#### Step 1: Create Dockerfile

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o openhash .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/openhash .
EXPOSE 8080 4001
CMD ["./openhash", "daemon"]
```

#### Step 2: Build and Run

```bash
# Build Docker image
docker build -t openhashdb .

# Run container
docker run -d \
  --name openhashdb \
  -p 8080:8080 \
  -p 4001:4001 \
  -v openhash-data:/root/data \
  openhashdb

# Check status
docker logs openhashdb
```

### Method 3: Docker Compose

For more complex deployments with multiple nodes.

#### docker-compose.yml

```yaml
version: '3.8'

services:
  openhashdb-node1:
    build: .
    ports:
      - "8080:8080"
      - "4001:4001"
    volumes:
      - node1-data:/root/data
    environment:
      - NODE_NAME=node1
    command: ["./openhash", "daemon", "--api-port", "8080"]

  openhashdb-node2:
    build: .
    ports:
      - "8081:8080"
      - "4002:4001"
    volumes:
      - node2-data:/root/data
    environment:
      - NODE_NAME=node2
    command: ["./openhash", "daemon", "--api-port", "8080"]

volumes:
  node1-data:
  node2-data:
```

#### Deploy with Docker Compose

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

## Configuration

### Command Line Configuration

OpenHashDB can be configured using command-line flags:

```bash
./openhash daemon \
  --db /path/to/database \
  --api-port 8080 \
  --p2p-port 4001 \
  --verbose
```

### Environment Variables

Set environment variables for containerized deployments:

```bash
export OPENHASH_DB_PATH="/data/openhash.db"
export OPENHASH_API_PORT="8080"
export OPENHASH_P2P_PORT="4001"
export OPENHASH_VERBOSE="true"
```

### Configuration File (Future Enhancement)

A configuration file format is planned for future releases:

```yaml
# openhash.yml
database:
  path: "/data/openhash.db"
  write_buffer_size: 67108864  # 64MB

api:
  port: 8080
  enable_cors: true
  max_upload_size: 104857600  # 100MB

network:
  p2p_port: 4001
  bootstrap_peers:
    - "/ip4/bootstrap1.example.com/tcp/4001/p2p/12D3KooW..."
    - "/ip4/bootstrap2.example.com/tcp/4001/p2p/12D3KooW..."

replication:
  factor: 3
  auto_pin_threshold: 10

logging:
  level: "info"
  format: "json"
```

## Deployment Scenarios

### Single Node Deployment

Suitable for development, testing, or small-scale usage.

```bash
# Start single node
./openhash daemon --db ./data/openhash.db --api-port 8080

# Or with Docker
docker run -d \
  --name openhashdb \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  openhashdb ./openhash daemon --db /data/openhash.db
```

### Multi-Node Local Cluster

For testing distributed functionality locally.

```bash
# Node 1
./openhash daemon --db ./node1.db --api-port 8080 --p2p-port 4001 &

# Node 2
./openhash daemon --db ./node2.db --api-port 8081 --p2p-port 4002 &

# Node 3
./openhash daemon --db ./node3.db --api-port 8082 --p2p-port 4003 &
```

### Production Cluster Deployment

#### Using systemd (Linux)

Create service file `/etc/systemd/system/openhashdb.service`:

```ini
[Unit]
Description=OpenHashDB Node
After=network.target

[Service]
Type=simple
User=openhash
Group=openhash
WorkingDirectory=/opt/openhashdb
ExecStart=/usr/local/bin/openhash daemon --db /var/lib/openhashdb/openhash.db --api-port 8080
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start service:

```bash
# Create user
sudo useradd -r -s /bin/false openhash

# Create directories
sudo mkdir -p /var/lib/openhashdb
sudo chown openhash:openhash /var/lib/openhashdb

# Enable and start service
sudo systemctl enable openhashdb
sudo systemctl start openhashdb

# Check status
sudo systemctl status openhashdb
```

#### Using Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: openhashdb
spec:
  replicas: 3
  selector:
    matchLabels:
      app: openhashdb
  template:
    metadata:
      labels:
        app: openhashdb
    spec:
      containers:
      - name: openhashdb
        image: openhashdb:latest
        ports:
        - containerPort: 8080
        - containerPort: 4001
        volumeMounts:
        - name: data
          mountPath: /data
        env:
        - name: OPENHASH_DB_PATH
          value: "/data/openhash.db"
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: openhashdb-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: openhashdb-service
spec:
  selector:
    app: openhashdb
  ports:
  - name: api
    port: 8080
    targetPort: 8080
  - name: p2p
    port: 4001
    targetPort: 4001
  type: LoadBalancer
```

### Cloud Deployment

#### AWS EC2

1. **Launch EC2 Instance:**
   - Choose Ubuntu 22.04 LTS AMI
   - Select appropriate instance type (t3.medium or larger)
   - Configure security groups (ports 8080, 4001)

2. **Install and Configure:**
   ```bash
   # Connect to instance
   ssh -i your-key.pem ubuntu@your-instance-ip
   
   # Install dependencies
   sudo apt update
   sudo apt install -y git build-essential
   
   # Install Go and build OpenHashDB
   # (follow build from source instructions)
   
   # Configure as systemd service
   # (follow systemd instructions above)
   ```

3. **Configure Load Balancer:**
   - Create Application Load Balancer
   - Configure target groups for port 8080
   - Set up health checks on `/health` endpoint

#### Google Cloud Platform

```bash
# Create VM instance
gcloud compute instances create openhashdb-node \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --machine-type=e2-medium \
  --tags=openhashdb \
  --metadata-from-file startup-script=startup.sh

# Create firewall rules
gcloud compute firewall-rules create openhashdb-api \
  --allow tcp:8080 \
  --source-ranges 0.0.0.0/0 \
  --target-tags openhashdb

gcloud compute firewall-rules create openhashdb-p2p \
  --allow tcp:4001 \
  --source-ranges 0.0.0.0/0 \
  --target-tags openhashdb
```

#### Azure

```bash
# Create resource group
az group create --name openhashdb-rg --location eastus

# Create VM
az vm create \
  --resource-group openhashdb-rg \
  --name openhashdb-vm \
  --image UbuntuLTS \
  --admin-username azureuser \
  --generate-ssh-keys \
  --custom-data startup.sh

# Open ports
az vm open-port --port 8080 --resource-group openhashdb-rg --name openhashdb-vm
az vm open-port --port 4001 --resource-group openhashdb-rg --name openhashdb-vm
```

## Security Considerations

### Network Security

1. **Firewall Configuration:**
   ```bash
   # Allow API access (restrict to trusted networks in production)
   sudo ufw allow 8080/tcp
   
   # Allow P2P networking
   sudo ufw allow 4001/tcp
   
   # Enable firewall
   sudo ufw enable
   ```

2. **Reverse Proxy Setup (Nginx):**
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;
       
       location / {
           proxy_pass http://localhost:8080;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```

3. **SSL/TLS Configuration:**
   ```bash
   # Install Certbot
   sudo apt install certbot python3-certbot-nginx
   
   # Obtain SSL certificate
   sudo certbot --nginx -d your-domain.com
   ```

### Access Control

Since OpenHashDB doesn't have built-in authentication, implement access control at the infrastructure level:

1. **API Gateway:** Use AWS API Gateway, Google Cloud Endpoints, or similar
2. **VPN Access:** Restrict access to VPN-connected clients
3. **IP Whitelisting:** Configure firewall rules for trusted IP ranges

### Data Security

1. **Encryption at Rest:** Use encrypted storage volumes
2. **Encryption in Transit:** Implement TLS for API access
3. **Backup Security:** Encrypt database backups

## Monitoring and Maintenance

### Health Monitoring

1. **Health Check Endpoint:**
   ```bash
   # Simple health check
   curl http://localhost:8080/health
   
   # Automated monitoring
   while true; do
     if ! curl -f http://localhost:8080/health > /dev/null 2>&1; then
       echo "$(date): Health check failed"
       # Send alert
     fi
     sleep 60
   done
   ```

2. **System Metrics:**
   ```bash
   # Monitor system resources
   htop
   iotop
   nethogs
   
   # Check disk usage
   df -h
   du -sh /var/lib/openhashdb/
   ```

### Log Management

1. **Log Rotation:**
   ```bash
   # Configure logrotate
   sudo tee /etc/logrotate.d/openhashdb << EOF
   /var/log/openhashdb/*.log {
       daily
       rotate 30
       compress
       delaycompress
       missingok
       notifempty
       create 644 openhash openhash
   }
   EOF
   ```

2. **Centralized Logging:**
   ```bash
   # Forward logs to syslog
   ./openhash daemon 2>&1 | logger -t openhashdb
   
   # Or use structured logging with journald
   sudo journalctl -u openhashdb -f
   ```

### Database Maintenance

1. **Backup Strategy:**
   ```bash
   #!/bin/bash
   # backup.sh
   
   DB_PATH="/var/lib/openhashdb/openhash.db"
   BACKUP_DIR="/backup/openhashdb"
   DATE=$(date +%Y%m%d_%H%M%S)
   
   # Stop service
   sudo systemctl stop openhashdb
   
   # Create backup
   mkdir -p $BACKUP_DIR
   cp -r $DB_PATH $BACKUP_DIR/openhash_$DATE.db
   
   # Compress backup
   gzip $BACKUP_DIR/openhash_$DATE.db
   
   # Start service
   sudo systemctl start openhashdb
   
   # Clean old backups (keep 30 days)
   find $BACKUP_DIR -name "*.gz" -mtime +30 -delete
   ```

2. **Database Optimization:**
   ```bash
   # Monitor database size
   du -sh /var/lib/openhashdb/
   
   # Check for corruption (if issues arise)
   # Note: LevelDB is generally self-healing
   ```

### Performance Tuning

1. **System Optimization:**
   ```bash
   # Increase file descriptor limits
   echo "openhash soft nofile 65536" >> /etc/security/limits.conf
   echo "openhash hard nofile 65536" >> /etc/security/limits.conf
   
   # Optimize network settings
   echo "net.core.rmem_max = 16777216" >> /etc/sysctl.conf
   echo "net.core.wmem_max = 16777216" >> /etc/sysctl.conf
   sysctl -p
   ```

2. **Storage Optimization:**
   ```bash
   # Use SSD storage for better performance
   # Mount with appropriate options
   /dev/sdb1 /var/lib/openhashdb ext4 defaults,noatime 0 2
   ```

## Troubleshooting

### Common Issues

#### 1. Port Already in Use

**Error:** `bind: address already in use`

**Solution:**
```bash
# Find process using the port
sudo lsof -i :8080
sudo netstat -tulpn | grep :8080

# Kill the process
sudo kill -9 <PID>

# Or use different port
./openhash daemon --api-port 8081
```

#### 2. Database Lock Error

**Error:** `database is locked`

**Solution:**
```bash
# Ensure only one instance is running
ps aux | grep openhash

# Kill any running instances
sudo pkill openhash

# Check file permissions
ls -la /var/lib/openhashdb/
sudo chown -R openhash:openhash /var/lib/openhashdb/
```

#### 3. Network Connectivity Issues

**Error:** P2P networking not working

**Solution:**
```bash
# Check firewall settings
sudo ufw status

# Test port connectivity
telnet <peer-ip> 4001

# Check network interfaces
ip addr show
```

#### 4. High Memory Usage

**Symptoms:** System running out of memory

**Solution:**
```bash
# Monitor memory usage
free -h
top -p $(pgrep openhash)

# Adjust LevelDB settings (requires code modification)
# Reduce write buffer size in storage configuration

# Add swap space (temporary solution)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### Diagnostic Commands

```bash
# Check service status
sudo systemctl status openhashdb

# View recent logs
sudo journalctl -u openhashdb -n 100

# Test API endpoints
curl -v http://localhost:8080/health
curl -v http://localhost:8080/stats

# Check network connectivity
ss -tulpn | grep openhash

# Monitor system resources
htop
iotop -o
```

### Getting Help

1. **Check Documentation:** Review README.md and API.md
2. **Search Issues:** Look for similar problems in the project repository
3. **Create Issue:** Provide detailed information including:
   - Operating system and version
   - OpenHashDB version
   - Configuration details
   - Error messages and logs
   - Steps to reproduce

### Recovery Procedures

#### Database Corruption

```bash
# Stop service
sudo systemctl stop openhashdb

# Backup current database
cp -r /var/lib/openhashdb/openhash.db /backup/corrupted_db_$(date +%Y%m%d)

# Restore from backup
cp /backup/openhashdb/openhash_YYYYMMDD.db.gz /tmp/
gunzip /tmp/openhash_YYYYMMDD.db.gz
cp -r /tmp/openhash_YYYYMMDD.db /var/lib/openhashdb/openhash.db

# Fix permissions
sudo chown -R openhash:openhash /var/lib/openhashdb/

# Start service
sudo systemctl start openhashdb
```

#### Complete System Recovery

```bash
# 1. Reinstall OpenHashDB
# 2. Restore database from backup
# 3. Restore configuration files
# 4. Start services
# 5. Verify functionality
```

This comprehensive installation and deployment guide provides the foundation for successfully running OpenHashDB in various environments, from development setups to production clusters. Regular maintenance and monitoring ensure optimal performance and reliability of your OpenHashDB deployment.


## Network Configuration

### Bootnode Setup

For production deployments, you should configure reliable bootnodes to ensure consistent network connectivity.

#### Setting Up a Bootnode

Any OpenHashDB node can serve as a bootnode. To set up a stable bootnode:

1. **Deploy on a stable server** with a public IP address
2. **Configure firewall** to allow incoming connections on the P2P port
3. **Start the daemon** with a fixed port:
   ```bash
   ./openhash daemon --p2p-port 4001 --api-port 8080
   ```
4. **Note the node's multiaddr** from the startup logs
5. **Share the multiaddr** with other network participants

#### Bootnode Best Practices

- **Use multiple bootnodes** for redundancy
- **Deploy geographically distributed** bootnodes for better connectivity
- **Monitor bootnode health** and replace failed nodes promptly
- **Use stable network addresses** (avoid dynamic IPs)
- **Configure appropriate resource limits** for high-traffic bootnodes

#### Example Production Configuration

```bash
# Start a production bootnode
./openhash daemon \
  --p2p-port 4001 \
  --api-port 8080 \
  --db /var/lib/openhash/data \
  --verbose

# Start a client node with multiple bootnodes
./openhash daemon \
  --bootnode /ip4/bootnode1.example.com/tcp/4001/p2p/12D3KooW...,/ip4/bootnode2.example.com/tcp/4001/p2p/12D3KooW... \
  --api-port 8080
```

### DHT Configuration

The DHT is automatically configured with sensible defaults, but you can optimize performance by:

#### Network Size Considerations

- **Small networks (< 10 nodes)**: DHT provides basic content discovery
- **Medium networks (10-100 nodes)**: DHT becomes highly effective for content routing
- **Large networks (> 100 nodes)**: DHT provides optimal performance with logarithmic lookup times

#### Monitoring DHT Health

Monitor DHT performance using the network statistics endpoint:

```bash
# Check DHT status
curl http://localhost:8080/network | jq '.dht'

# Monitor peer count over time
watch -n 5 'curl -s http://localhost:8080/network | jq ".dht.peer_count"'
```

### Firewall Configuration

Ensure your firewall allows the necessary ports:

```bash
# Allow P2P traffic (default random port, or specify with --p2p-port)
sudo ufw allow from any to any port 4001

# Allow REST API traffic
sudo ufw allow from any to any port 8080

# For Docker deployments
docker run -p 4001:4001 -p 8080:8080 openhashdb
```

### Network Troubleshooting

#### Common Issues

1. **No peers connecting**:
   - Check firewall settings
   - Verify bootnode addresses are correct
   - Ensure bootnodes are online and reachable

2. **DHT not finding content**:
   - Wait for DHT to bootstrap (can take 1-2 minutes)
   - Check that content providers are still online
   - Verify network connectivity between nodes

3. **Slow content retrieval**:
   - Check network bandwidth between nodes
   - Monitor DHT peer count - more peers = better performance
   - Consider adding more bootnodes for better connectivity

#### Debug Commands

```bash
# Check network connectivity
./openhash daemon --verbose

# Test content retrieval
./openhash get <hash> --verbose

# Monitor network statistics
curl http://localhost:8080/network
```


## CLI API Mode Configuration

The `--api` flag enables CLI commands to interact with running OpenHashDB daemon instances, resolving database lock conflicts and enabling remote operations.

### Basic Setup

#### Single Node Setup
```bash
# Terminal 1: Start daemon
./openhash daemon --api-port 8080

# Terminal 2: Use CLI with API mode
./openhash add myfile.txt --api http://localhost:8080
./openhash list --api http://localhost:8080
```

#### Multi-Node Setup
```bash
# Node 1 (192.168.1.100)
./openhash daemon --api-port 8080 --p2p-port 4001

# Node 2 (192.168.1.101)  
./openhash daemon --api-port 8080 --p2p-port 4001 \
  --bootnode /ip4/192.168.1.100/tcp/4001/p2p/12D3KooW...

# Client machine - can interact with either node
./openhash add file1.txt --api http://192.168.1.100:8080
./openhash add file2.txt --api http://192.168.1.101:8080
./openhash list --api http://192.168.1.100:8080
```

### Production Deployment

#### Systemd Service Configuration

Create `/etc/systemd/system/openhashdb.service`:
```ini
[Unit]
Description=OpenHashDB Daemon
After=network.target

[Service]
Type=simple
User=openhash
Group=openhash
WorkingDirectory=/opt/openhashdb
ExecStart=/opt/openhashdb/openhash daemon \
  --db /var/lib/openhashdb/data \
  --api-port 8080 \
  --p2p-port 4001 \
  --bootnode /ip4/bootnode1.example.com/tcp/4001/p2p/12D3KooW...,/ip4/bootnode2.example.com/tcp/4001/p2p/12D3KooW...
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable openhashdb
sudo systemctl start openhashdb
sudo systemctl status openhashdb
```

#### Nginx Reverse Proxy

Configure nginx for SSL termination and load balancing:

```nginx
upstream openhashdb {
    server 127.0.0.1:8080;
    # Add more backend servers for load balancing
    # server 127.0.0.1:8081;
}

server {
    listen 443 ssl http2;
    server_name openhash.example.com;
    
    ssl_certificate /path/to/ssl/cert.pem;
    ssl_certificate_key /path/to/ssl/key.pem;
    
    location / {
        proxy_pass http://openhashdb;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Enable CORS for web applications
        add_header Access-Control-Allow-Origin *;
        add_header Access-Control-Allow-Methods "GET, POST, PUT, DELETE, OPTIONS";
        add_header Access-Control-Allow-Headers "Content-Type, Authorization";
    }
}
```

Client usage with SSL:
```bash
./openhash add myfile.txt --api https://openhash.example.com
```

### Docker Deployment

#### Dockerfile
```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN go build -o openhash .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/openhash .

EXPOSE 8080 4001

CMD ["./openhash", "daemon", "--api-port", "8080", "--p2p-port", "4001"]
```

#### Docker Compose
```yaml
version: '3.8'

services:
  openhashdb-node1:
    build: .
    ports:
      - "8080:8080"
      - "4001:4001"
    volumes:
      - node1_data:/root/openhash.db
    environment:
      - OPENHASH_BOOTNODES=/ip4/openhashdb-node2/tcp/4001/p2p/12D3KooW...
    
  openhashdb-node2:
    build: .
    ports:
      - "8081:8080"
      - "4002:4001"
    volumes:
      - node2_data:/root/openhash.db
    environment:
      - OPENHASH_BOOTNODES=/ip4/openhashdb-node1/tcp/4001/p2p/12D3KooW...

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/ssl
    depends_on:
      - openhashdb-node1
      - openhashdb-node2

volumes:
  node1_data:
  node2_data:
```

Client usage with Docker:
```bash
# Upload to load-balanced cluster
./openhash add myfile.txt --api http://localhost

# Direct node access
./openhash list --api http://localhost:8080  # Node 1
./openhash list --api http://localhost:8081  # Node 2
```

### Monitoring and Logging

#### Health Checks
```bash
# Basic health check
curl http://localhost:8080/health

# Detailed status
curl http://localhost:8080/stats
curl http://localhost:8080/network
```

#### Log Monitoring
```bash
# Follow daemon logs
./openhash daemon --verbose 2>&1 | tee openhash.log

# Monitor API requests
tail -f /var/log/nginx/access.log | grep openhash

# System monitoring
journalctl -u openhashdb -f
```

#### Prometheus Metrics (Future Enhancement)
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'openhashdb'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Backup and Recovery

#### Database Backup
```bash
# Stop daemon
sudo systemctl stop openhashdb

# Backup database
tar -czf openhashdb-backup-$(date +%Y%m%d).tar.gz /var/lib/openhashdb/

# Restart daemon
sudo systemctl start openhashdb
```

#### Content Verification
```bash
# Verify all content integrity
./openhash list --api http://localhost:8080 | while read hash; do
    ./openhash get "$hash" --api http://localhost:8080 > /dev/null
    echo "Verified: $hash"
done
```

### Performance Tuning

#### Database Configuration
```bash
# Use SSD storage for better performance
./openhash daemon --db /mnt/ssd/openhashdb/data

# Increase file descriptor limits
ulimit -n 65536
```

#### Network Optimization
```bash
# Tune TCP settings for high throughput
echo 'net.core.rmem_max = 134217728' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 134217728' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 87380 134217728' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 65536 134217728' >> /etc/sysctl.conf
sysctl -p
```

#### API Rate Limiting
```nginx
# Add to nginx configuration
limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;

location / {
    limit_req zone=api burst=20 nodelay;
    proxy_pass http://openhashdb;
}
```

### Troubleshooting

#### Common Issues

1. **Database Lock Errors**
   ```bash
   Error: failed to open database: resource temporarily unavailable
   ```
   Solution: Use `--api` flag or stop daemon before direct access

2. **API Connection Refused**
   ```bash
   Error: dial tcp: connect: connection refused
   ```
   Solution: Verify daemon is running and port is correct

3. **Permission Denied**
   ```bash
   Error: failed to create database directory
   ```
   Solution: Check file permissions and user ownership

#### Debug Commands
```bash
# Check process status
ps aux | grep openhash

# Check port usage
netstat -tlnp | grep :8080

# Test API connectivity
curl -v http://localhost:8080/health

# Check firewall
sudo ufw status
sudo iptables -L
```

