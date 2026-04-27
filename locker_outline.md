# General Information

This locker system is powered by a raspberry pi that is in read only mode. The system is expandable up to 24 lockers. A i2c io expander from DFRobot called the CH423 is being used for this purpose. Each board can power up to 8 lockers. These boards have 16 output only pins and 8 input/output pins. Each locker uses 2 output only pins to power its light and latch. Each locker uses 1 input/output pin to read a sensor. The python script has a the pin assignments for each locker saved locally, even those that are not yet wired up. Each board uses 1 i2c bus on the raspberry pi, of which there are 3. Each board also has its own i2c address, 0x20, 0x21, and 0x22. This means that the 1st, 9th, and 17th locker will have different bus numbers and addresses, but the same pin numbers. Here is an example of the local hardware dictionary (see https://github.com/DFRobot/DFRobot_CH423 for more information):

locker_hardware.dict = {
    "Locker 1": {
        "Bus": 1
        "Address": 0x20
        "Light Pin": "eGPO0"
        "Latch Pin": "eGPO1"
        "Sensor Pin": "eGPIO0"
    }
}

There is a seperate website used to modify the values in the locker's database. Each registered user has their name, email, 16-digit card swipe id, and registration date and time saved within the database. Additionally, The state of each locker is stored within the database. There is also a locker cache, which includes all unassigned, active lockers and their previous users and their entry and exit times. This cache is initialized on the pi. Each time a locker is assigned, it is removed from the cache. Each time a locker is unassigned, it is added to the end of the cache. The first element in the cache is assigned to the next registered user who requests a locker. Example dictionaries:

locker_status_dict = {
    "Locker 1": {
        "Admin Unlocked": False
        "Door Shut": True
        "Active": True
        "Occupied": True
        "Occupant": {
            "Name": John Smith
            "Card ID": 1234567812345678
            "Email": "johnsmith@uindy.edu"
            "Entry Date": 2026-01-01 00:00:00
        }
    }
}

registered_user_dict = {
    "John Smith": {
        "Card ID": 1234567812345678
        "Email": johnsmith@uindy.edu
        "Registration Date": 2026-01-01 00:00:00
    }
}

locker_cache_dict = {
    "Locker 4":{
        "Previous User": "Billy Bob"
        "Previous User Card ID": 1234567812345678
        "Previous User Email": "billybob@uindy.edu"
    }
    "Locker 3": {
        ...
    }
    "Locker 2": {
        ...
    }
}

Because the pi is read only, the registered user dictionary is initially blank in the code, and all lockers 24 in the locker status dictionary are saved as:

locker_status_dict = {
    "Locker 1": {
        "Admin Unlocked": False
        "Door Shut": True
        "Active": False
        "Occupied": False
        "Occupant": {
            "Name": None
            "Card ID": None
            "Email": None
            "Entry Date": None
        }
    }
}

Likewise, the registered user dictionary is saved locally as:

registered_user_dict = {}

When a locker marked false for active, it cannot be assigned by the code to any user. additionally, the code can never change a locker's activation status. This is done by the admin on the website. If the code sees a new locker is active, it can then intereact with it.

# Functions

Here are some important functions

## Locker Control Functions:
def unlock_locker: opens the latch of a given locker
def locker_locker: releases the latch of a given locker
def blink_light: blinks the light every second of a given locker for a given time
def is_door_closed: returns true if door is closed, false if it is open

## User Check Functions
def is_card_registered: returns user name if the card id is found in the registered    
    user dictionary, returns false if not
def is_card_assigned: returns locker id if card id corresponds to a locker in locker 
    status dictionary, false if there is no assigned locker
def is_locker_availible: returns first locker in cache, false if there are no locker 
    in cache

# Initializeation

## Database Sync
1.  The code saves all registered users in the database to the registered_users_dict
2.  The code saves status of all lockers to locker_status_dict

# Main Loop
1.  The code checks for updates to locker_status_dict. If there are any updates, the 
    pin states of the latch and light are updated using the CH423 library. The latch is activated if the "Admin Unlocked" element is set to True.
    If lockers that were occupied are now unoccupied, save them and their previous user, card id, and entry and exit time to the cache. 
    If lockers that were unoccupied are now occupied, remove them from the cache
    If there are duplicate emails within the registered user database, prioritize the newest card id that is registered to that email
2.  The code checks for updates to registered_user_dict. If there are any updates, 
    they are saved to the local registered_user_dict (which is technically temporary 
    because it is wiped if the pi loses power)
3.  The code checks if a card swipe has occured:
    If one has:
    1.  The card id is saved to a variable
    2.  The code checks if the card id corresonds to a registered user
        If it does:
        1.  The code saves the user name to a variable
        2.  The code checks if that user has an assigned locker in locker_status_dict
            If they do:
            1.  The code unlocks that locker and flashes the light   
                every 1 second until either 15 seconds has passed (This is saved as a constant at the beginning of the code) or the sensor of that locker reads that the door has opened.
            2.  If the door opens within 15 seconds:
                1.  Unassign the user from the locker
                2.  Add locker, user, and entry and exit time to end of the locker 
                    cache
                3.  Hold the latch for 30 seconds or until the door shuts
                    If the door does not shut in 30 seconds:
                    1.  Release the latch
                    2.  Save that locker's "Door Shut" variable in the 
                        locker_status_dict as False
                Otherwise, don't unassign the locker, leave
            If they do not have an assigned locker in locker_status_dict (This is the same unlocking procedure as above):
            1.  The code unlocks that locker and flashes the light   
                every 1 second until either 15 seconds has passed (This is saved as a constant at the beginning of the code) or the sensor of that locker reads that the door has opened.
            2.  If the door opens within 15 seconds:
                1.  Assigns the user from the locker
                2.  Remove the locker, user, and entry and exit time from the cache
                3.  Hold the latch for 30 seconds or until the door shuts
                    If the door does not shut in 30 seconds:
                    1.  Release the latch
                    2.  Save that locker's "Door Shut" variable in the 
                        locker_status_dict as False
        If it doesn't, send the card id to the database as UNREGISTERED_USER_CARD_ID
