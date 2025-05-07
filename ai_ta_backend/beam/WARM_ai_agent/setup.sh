#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting WARM AI Agent setup...${NC}"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check and install conda if not present
if ! command_exists conda; then
    echo -e "${RED}Conda not found. Installing Miniconda...${NC}"
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    bash miniconda.sh -b -p $HOME/miniconda
    rm miniconda.sh
    export PATH="$HOME/miniconda/bin:$PATH"
    source $HOME/miniconda/bin/activate
fi

# Create and activate conda environment
echo -e "${GREEN}Creating conda environment...${NC}"
conda env create -f environment.yml
source $(conda info --base)/etc/profile.d/conda.sh
conda activate warm_ai_agent

# Install system dependencies (ODBC Driver and Kerberos)
echo -e "${GREEN}Installing system dependencies...${NC}"
sudo apt-get update

# Install Kerberos
echo -e "${GREEN}Installing Kerberos...${NC}"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user

# Install ODBC Driver
echo -e "${GREEN}Installing ODBC Driver...${NC}"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
sudo apt-get install -y unixodbc-dev

# Install OpenConnect VPN client
echo -e "${GREEN}Installing VPN client...${NC}"
sudo apt-get update
sudo apt-get install -y network-manager-openconnect-gnome openconnect

echo -e "${GREEN}VPN client installed. To connect:${NC}"
echo -e "1. GUI Method: Use Network Settings > VPN > Add VPN"
echo -e "2. Command Line: sudo openconnect --useragent=AnyConnect -qb vpn.illinois.edu"
echo -e "   Select 'OpenConnect2 (All)' when prompted for GROUP"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${GREEN}Creating .env file...${NC}"
    cat > .env << EOL
DB_DRIVER="{ODBC Driver 18 for SQL Server}"
DB_SERVER=PRI-SQLP.ad.uillinois.edu,1433
DB_NAME=warm
DB_TRUSTED_CONNECTION=yes
DB_TRUST_SERVER_CERT=yes
OPENAI_API_KEY="your_key_here"
EOL
    echo -e "${RED}Please update the OPENAI_API_KEY in .env file${NC}"
fi

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${GREEN}Next steps:${NC}"
echo -e "1. Update OPENAI_API_KEY in .env file if you haven't already"
echo -e "2. Run: kinit {netid}@AD.UILLINOIS.EDU"
echo -e "3. Start the agent with: python -m warm_ai.main"