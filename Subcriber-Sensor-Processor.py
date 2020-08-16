# Nats
import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
# Hostname
import socket 
# Json
import json
import os
# Time
from datetime import datetime
# Mkdir
from shutil import rmtree 
from os import makedirs
from os import remove
import shutil 

#///////////////////////////////////////////////////////////////////////
# Create folder for Message backup
#///////////////////////////////////////////////////////////////////////
try:
    FolderTest = "C:/File-Nats/Kiosk-Message/"
    makedirs(FolderTest)
    print (" - Creating Folder Kiosk-Message")
except FileExistsError:
    print (" - Folder Exists")

#///////////////////////////////////////////////////////////////////////
# Get Hostname
#///////////////////////////////////////////////////////////////////////
Hostname = socket.gethostname()
print ('*********************************************************************')
print('Runing... Nats Subscriber [VSBLTY-DATA-TEMP]')
print('   - Host: ', Hostname)

#///////////////////////////////////////////////////////////////////////
# Define Channel where you want to register
# You can check the Channel within the setting file of the nats Server
#///////////////////////////////////////////////////////////////////////
ChannelNatsSubscriber = 'VSBLTY-DATA-TEMP-SENSOR-4212e5fa-35a5-489e-a404-e52535780f9f'

#///////////////////////////////////////////////////////////////////////
# Client name or cliendID (This must be Unique within the Nats Server)
#///////////////////////////////////////////////////////////////////////
SubscriberId = "Client-Python-SENSOR"

#///////////////////////////////////////////////////////////////////////
# Function to capture possible Connection Errors
#///////////////////////////////////////////////////////////////////////
async def error_cb(e):
    print("Error:", e)

#///////////////////////////////////////////////////////////////////////
# Main Function for Nats Server connection and Message handling
#///////////////////////////////////////////////////////////////////////
async def run(loop):
    nc = NATS()
    sc = STAN()

    options = {
        #///////////////////////////////////////
        # Remote Server IP Address
        #///////////////////////////////////////
        "servers": ["nats://192.168.100.14:4288"],
        "io_loop": loop,
        "error_cb": error_cb
    }
    print ("   - Sever: ", options["servers"])
    print ('   - Channel-Subscriber: ', ChannelNatsSubscriber)
    print ('*********************************************************************')
    print ('')

    # Initialize Connection Options
    await nc.connect(**options)

    # Start session with NATS Streaming cluster using
    # the established NATS connection.
    await sc.connect("vsblty-cluster", SubscriberId, nats=nc)

    async def cb(msg):

        # Decode UTF-8 bytes (message received)
        # to double quotes to make it valid JSON
        JsonNats = msg.data.decode('utf8').replace("'", '"')

        # Message is JSON formatted
        mensaje = json.loads(JsonNats)

        # Get the FrameTimestamp, which come inside the message
        MessageId = str(mensaje['FrameTimestamp'])

        # replace (:) with (.) and be able to save file
        MessageId = MessageId.replace(':', '.') 

        # File path where the Json files will be saved
        dir = 'C:/File-Nats/Kiosk-Message/'  

        # Current Date, Only to show in console the Date the message was received
        now = datetime.now()
        timestampStr = now.strftime("%d-%m-%Y %H%M%S.%f")

        # Show message on console, next to Msg Nats Sequence
        print ('- [Subscriber] - [Sensor-Processor] - Msg:' + str(msg.seq) + ' - ' + str(timestampStr))

        #///////////////////////////////////////////////////////////////////////
        # Save Message in Json format and defined folder
        #///////////////////////////////////////////////////////////////////////

        # Defining File Name
        file_name =  MessageId + " - [Sensor-Processor] - Msg - [" + str(msg.seq) + "] - Kiosk.json"
        
        # Save File
        # For each File received, create and save a json file with the information received in the message
        with open(os.path.join(dir, file_name), 'w') as file:
            json.dump(mensaje, file)

    # Subscribe to get all messages.
    await sc.subscribe(ChannelNatsSubscriber, start_at='first', cb=cb)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()