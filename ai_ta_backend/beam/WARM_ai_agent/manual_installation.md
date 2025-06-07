# Manual Installation Guide

This guide provides detailed step-by-step instructions for installing the WARM AI Agent and its dependencies.

## Prerequisites

### 1. System Requirements
- Ubuntu/Debian-based Linux system
- Python 3.10 or higher
- Internet connection
- Sudo privileges

### 2. Install Conda
```bash
# Download Miniconda installer
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
# Install Miniconda
bash miniconda.sh -b -p $HOME/miniconda
# Add conda to path
export PATH="$HOME/miniconda/bin:$PATH"
# Initialize conda
source $HOME/miniconda/bin/activate
# Verify installation
conda --version
```

### 3. Install Kerberos
```bash
# Install Kerberos client
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user
# Verify installation
klist --version
```

### 4. Install ODBC Driver
```bash
# Import Microsoft GPG key
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
# Add Microsoft repository
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | \
sudo tee /etc/apt/sources.list.d/mssql-release.list
# Update package list
sudo apt-get update
# Install ODBC Driver
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
sudo apt-get install -y unixodbc-dev
# Verify installation
odbcinst -j
```

## WARM AI Agent Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd warm_ai_agent
```
### 2. Create Conda Environment
```bash
# Create environment from yml file
conda env create -f environment.yml
# Activate environment
conda activate warm_ai_agent
# Verify environment
python --version # Should show Python 3.10.x
```
### 3. Configure Environment Variables
Create a `.env` file in the project root:
```bash
cat > .env << EOL
DB_DRIVER="{ODBC Driver 18 for SQL Server}"
DB_SERVER=PRI-SQLP.ad.uillinois.edu,1433
DB_NAME=warm
DB_TRUSTED_CONNECTION=yes
DB_TRUST_SERVER_CERT=yes
OPENAI_API_KEY="your_key_here"
EOL
```
Replace `your_key_here` with your actual OpenAI API key.

## Verification Steps

### 1. Verify Kerberos Authentication

```bash
# Initialize Kerberos ticket
kinit {netid}@AD.UILLINOIS.EDU
# Verify ticket
klist
```

### 2. Verify ODBC Configuration
```bash
# Check ODBC drivers
odbcinst -q -d
# Should show "ODBC Driver 18 for SQL Server" in the list
```
### 3. Verify Python Environment

```bash
# Activate environment
conda activate warm_ai_agent
# Check installed packages
pip list | grep -E "langchain|openai|sqlalchemy"
```

## Running the Agent

1. Ensure Kerberos ticket is valid:
```bash
kinit {netid}@AD.UILLINOIS.EDU
```
2. Activate conda environment:
```bash
conda activate warm_ai_agent
```
3. Run the agent:
```bash
python -m warm_ai.main
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Conda Environment Issues

If environment creation fails, try:

```bash
conda clean --all
conda env create -f environment.yml --force
```
#### 2. ODBC Driver Issues

If driver not found, verify installation:
```bash
sudo dpkg -l | grep msodbcsql
```
Reinstall if needed:
```bash
sudo ACCEPT_EULA=Y apt-get install --reinstall msodbcsql18
```
#### 3. Kerberos Issues

Check Kerberos configuration
```bash
cat /etc/krb5.conf
```
Verify ticket status
```bash
klist
```
If ticket expired, renew:
```bash
kinit {netid}@AD.UILLINOIS.EDU
```

#### 4. Database Connection Issues
- Verify environment variables in `.env` file
- Check network connectivity to database server
- Ensure Kerberos ticket is valid
- Verify ODBC driver configuration

## Maintenance

### Updating the Environment

Update conda environment
```bash
conda env update -f environment.yml --prune
```
Update ODBC driver
```bash
sudo apt-get update
sudo apt-get upgrade msodbcsql18
```

### Cleaning Up
Remove conda environment if needed
```bash
conda deactivate
conda env remove -n warm_ai_agent
```
Remove ODBC driver if needed
```bash
sudo apt-get remove msodbcsql18
````tr -=bvcxz Cx`

## Further Reading & Resources

### System Setup
- [Ubuntu Installation Guide](https://ubuntu.com/tutorials/install-ubuntu-desktop) - Official Ubuntu installation guide
- [Conda Installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html) - Conda setup on Linux. This project uses miniconda.
- [Python Version Management](https://docs.python.org/3/using/index.html) - Python documentation

### Authentication & Security
- [UIUC VPN Documentation](https://cybersecurity.illinois.edu/change-to-campus-vpn-login-process-coming-march-12/) - Official VPN setup guide
- [OpenConnect VPN Client](https://www.infradead.org/openconnect/) - OpenConnect documentation
- [Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/) - MIT Kerberos documentation
- [UIUC Identity and Access Management](https://answers.uillinois.edu/illinois/page.php?id=47575) - UIUC authentication services

### Database Connectivity
- [Microsoft ODBC Installation](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) - Official ODBC driver setup
- [SQL Server on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-overview) - Microsoft's Linux guide
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/en/20/) - Python SQL toolkit documentation

### Environment Management
- [Conda Environment Management](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) - Conda env guide

### Troubleshooting
- [ODBC Troubleshooting](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/known-issues-in-this-version-of-the-driver) - Microsoft ODBC issues guide
- [Kerberos Debug Guide](https://web.mit.edu/kerberos/krb5-latest/doc/admin/troubleshoot.html) - MIT Kerberos troubleshooting
- [VPN Connection Issues](https://answers.uillinois.edu/illinois/98773) - UIUC VPN troubleshooting