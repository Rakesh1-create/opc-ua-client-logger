OPC UA Client Logger

How to run:
1) Install dependencies:
   pip install opcua pandas

2) Start your OPC UA server (Unified Automation Demo Server).

3) Run the logger:
   python opc_logger.py

The script reads 10 tags every 1 minute and creates hourly CSV log files.
