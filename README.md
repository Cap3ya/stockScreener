# stockScreener

Steps to set up a working environment:

```bash
# Update package lists and install dependencies
sudo apt update
sudo apt install -y python3 python3-pip python3-venv git

# Create and enter a Python virtual environment
python3 -m venv python && cd ./python

# Clone the repository
git clone https://github.com/Cap3ya/stockScreener.git

# Install Python dependencies
python3 -m pip install -r stockScreener/requirements.txt

# Activate the virtual environment
source bin/activate

# Run the main script
python3 stockScreener/main.py
