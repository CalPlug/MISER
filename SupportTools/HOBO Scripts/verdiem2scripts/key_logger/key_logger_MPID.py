#################################
#       Simple Illustration     #
#################################
# Read the contents of a key logger devices which records time of pwr key was pressed and parse into Verdiem 2 format then upload to a mySQL table

#################################
#       Usage Illustration      #
#################################
# "Change"      means   Change the global variable in the code

# "table_name"          Change the desire output table in MySQL DB
# "desktop_type"        Change the desire current desktop type
# "subject_ID"          Change the desire current subject_identifier
# "MPID"                Change it to switch scope of time in each 15 mins period
#                               ex. MPID = 1, each block will be 15 mins value
#                               ex. MPID = 15, each block will be 15 consecutive 1 min value
#                               ex. MPID = 900, each block will be 900 consecutive 1 sec value
# "input_file_name"     Change to File name of key logger which records times [pwr] key was pressed

###############################
#       Data Constraints      #
###############################
# If the entry has all periods with all 0, the entry will be ignored.

########################
#       Producers      #
########################
# California Plug Load Research Center, 2018
# Original Produced by Zihan "Bronco" Chen
# Tested and Modified by Liangze Yu

# Library setup:
import numpy
from datetime import timedelta, datetime, date
import time
import mysql.connector
import pytz  # handle timzone issues
import re

# Operational Settings
table_name = "KeyLoggerExampleDATA"  # Name of database table to write to
desktop_type = "PC"  # User supplied field that identifies the OS that the parsed file was run on.
subject_ID = "120"  # User supplied field that identifies the subject - to be pushed as a string to the DB
MPID = 900  # User supplied field that identifies the subject - to be pushed as a string to the DBName
int_record = 1
input_file_name = "input_key_logger.txt"
unknown_for_fringes = 1

auto_timezone_adjust = True
auto_timezone = pytz.timezone(
    'America/Los_Angeles')  # Set timezone for subject, assume pacific for California for CalPlug studies
manual_offset = timedelta(hours=7)

# Constants
min_in_day = 1440  # Total minutes in a day, used as a constant, placed here for clarity in code
sec_in_day = 60 * min_in_day
time_parser = "%Y/%m/%d %H:%M:%S"

resolution = 900 // MPID
period_in_day = sec_in_day // resolution


# Functions:
def timeconverter(datetime_currentvalue):
    ts = int(datetime_currentvalue)  # use integer value in seconds
    if auto_timezone_adjust:
        datetime_newvalue = pytz.utc.localize(datetime.utcfromtimestamp(ts)).astimezone(auto_timezone)
    else:
        datetime_newvalue = datetime.utcfromtimestamp(ts) + manual_offset

    # Continue building out this function
    # return datetime object, "pretty date, i.e. 2014-04-30" as string, and Day of the week as a string - each of the return arguments
    return datetime_newvalue


def statusArrayToMinutesDict(statusArray):
    # [timestamp]
    timeRange = statusArray[-1] - statusArray[0]
    dayRange = timeRange // 86400 + 2
    periodCount = dayRange * period_in_day
    secondsRange = int(dayRange * sec_in_day)

    secondsState = numpy.zeros((secondsRange,), numpy.int8)

    firstSlotTime = timeconverter(statusArray[0])
    firstSlotTimestamp = int(firstSlotTime.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    for timestamp in statusArray:
        secondsState[(timestamp - firstSlotTimestamp)] = 1

    reshaped = numpy.reshape(secondsState, (periodCount, resolution)).sum(axis=1)
    return reshaped


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        try:
            yield tuple(l[i:i + n])
        except IndexError:
            yield tuple(l[i:])
            return


def dateToWeekdays(adate):
    dowValueDict = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday"
    }
    return dowValueDict.get(adate.weekday())


# ************************************************
# Program Operation:
# open PUMI database file:
# open file and read in contents
with open(input_file_name, 'r', encoding='utf-8') as data_file:
    data = data_file.readlines()
    data = data[:-1]  # remove lat line
# Alternatively, use json.dumps(json_value) to convert your json object(python object) in a json string that you can insert in a text field in mysql
# read in JSON to Python dictionary
# example on this:  https://stackoverflow.com/questions/4251124/inserting-json-into-mysql-using-python

eventList = []
regexParse = re.compile(r"^\[(.*)\]\[Pwr\]$")
for i in data:
    if regexParse.match(i):
        timeStr = regexParse.match(i).group(1)
        try:
            if auto_timezone_adjust:
                eventList.append(int(
                    auto_timezone.localize(datetime(*(time.strptime(timeStr, "%Y/%m/%d %H:%M:%S")[0:6]))).timestamp()))
            else:
                eventList.append(int((datetime.strptime(timeStr, "%Y/%m/%d %H:%M:%S") + manual_offset).timestamp()))

        except:
            continue

eventList.sort()
# create string "p1, p2 ,p3 ..... p96"
p_series_in_query = ", ".join(["p" + str(i) for i in range(1, 97)])

# Open Database connection
db = mysql.connector.connect(host="XXXXXXX.calit2.uci.edu",  # host
                             user="XXXXXXXXX",  # username
                             passwd="XXXXXXXX",  # password
                             db="XXXXXXXX")  # DBName

cursor = db.cursor()  # Cursor object for database query

slotArray = statusArrayToMinutesDict(eventList)
firstDate = timeconverter(eventList[0])
querys = []


currentDate = firstDate
device = "User"
status = "Pwr"
for chunk in chunks(slotArray, period_in_day):
    if chunk == tuple(len(chunk) * [0]):
        currentDate += timedelta(days=1)
        continue
    # if len(list(filter(lambda x: x != 0, chunk))) == 0:
    #     continue
    query = "INSERT INTO " \
            + table_name \
            + "(subject_identifier,desktop_type,MPID,device,status,int_record,date,day_of_week," \
            + p_series_in_query \
            + ') VALUES (' \
            + str(subject_ID) \
            + ',' \
            + repr(desktop_type) \
            + ',' \
            + str(MPID) \
            + ',' \
            + repr(device) \
            + ',' \
            + repr(status) \
            + ',' \
            + str(int_record) \
            + ',' \
            + repr(currentDate.strftime('%Y-%m-%d')) \
            + ',' \
            + repr(dateToWeekdays(currentDate)) \
            + ',' \
            + ",".join(map(lambda x: repr(",".join([str(i) for i in x])), chunks(chunk, len(chunk) // 95))) \
            + ");"

    querys.append((currentDate, (device, status), query))
    currentDate += timedelta(days=1)

#list(map(lambda x: print(x[2]), querys))
list(map(lambda x: cursor.execute(x[2]), querys))
db.commit()
db.close()  # close DB connection



#########################################
#       Sample Table Structure Code     #
#########################################
# TEXT type of each period is for MPID = 900

'''

CREATE TABLE `KeyLoggerExampleDATA` (
  `record_id` smallint(6) NOT NULL AUTO_INCREMENT,
  `subject_identifier` tinyint(4) DEFAULT NULL,
  `desktop_type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `MPID` smallint(6) DEFAULT NULL,
  `device` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `status` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `int_record` mediumint(9) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `day_of_week` varchar(9) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `p1` text COLLATE utf8mb4_unicode_ci,
  `p2` text COLLATE utf8mb4_unicode_ci,
  `p3` text COLLATE utf8mb4_unicode_ci,
  `p4` text COLLATE utf8mb4_unicode_ci,
  `p5` text COLLATE utf8mb4_unicode_ci,
  `p6` text COLLATE utf8mb4_unicode_ci,
  `p7` text COLLATE utf8mb4_unicode_ci,
  `p8` text COLLATE utf8mb4_unicode_ci,
  `p9` text COLLATE utf8mb4_unicode_ci,
  `p10` text COLLATE utf8mb4_unicode_ci,
  `p11` text COLLATE utf8mb4_unicode_ci,
  `p12` text COLLATE utf8mb4_unicode_ci,
  `p13` text COLLATE utf8mb4_unicode_ci,
  `p14` text COLLATE utf8mb4_unicode_ci,
  `p15` text COLLATE utf8mb4_unicode_ci,
  `p16` text COLLATE utf8mb4_unicode_ci,
  `p17` text COLLATE utf8mb4_unicode_ci,
  `p18` text COLLATE utf8mb4_unicode_ci,
  `p19` text COLLATE utf8mb4_unicode_ci,
  `p20` text COLLATE utf8mb4_unicode_ci,
  `p21` text COLLATE utf8mb4_unicode_ci,
  `p22` text COLLATE utf8mb4_unicode_ci,
  `p23` text COLLATE utf8mb4_unicode_ci,
  `p24` text COLLATE utf8mb4_unicode_ci,
  `p25` text COLLATE utf8mb4_unicode_ci,
  `p26` text COLLATE utf8mb4_unicode_ci,
  `p27` text COLLATE utf8mb4_unicode_ci,
  `p28` text COLLATE utf8mb4_unicode_ci,
  `p29` text COLLATE utf8mb4_unicode_ci,
  `p30` text COLLATE utf8mb4_unicode_ci,
  `p31` text COLLATE utf8mb4_unicode_ci,
  `p32` text COLLATE utf8mb4_unicode_ci,
  `p33` text COLLATE utf8mb4_unicode_ci,
  `p34` text COLLATE utf8mb4_unicode_ci,
  `p35` text COLLATE utf8mb4_unicode_ci,
  `p36` text COLLATE utf8mb4_unicode_ci,
  `p37` text COLLATE utf8mb4_unicode_ci,
  `p38` text COLLATE utf8mb4_unicode_ci,
  `p39` text COLLATE utf8mb4_unicode_ci,
  `p40` text COLLATE utf8mb4_unicode_ci,
  `p41` text COLLATE utf8mb4_unicode_ci,
  `p42` text COLLATE utf8mb4_unicode_ci,
  `p43` text COLLATE utf8mb4_unicode_ci,
  `p44` text COLLATE utf8mb4_unicode_ci,
  `p45` text COLLATE utf8mb4_unicode_ci,
  `p46` text COLLATE utf8mb4_unicode_ci,
  `p47` text COLLATE utf8mb4_unicode_ci,
  `p48` text COLLATE utf8mb4_unicode_ci,
  `p49` text COLLATE utf8mb4_unicode_ci,
  `p50` text COLLATE utf8mb4_unicode_ci,
  `p51` text COLLATE utf8mb4_unicode_ci,
  `p52` text COLLATE utf8mb4_unicode_ci,
  `p53` text COLLATE utf8mb4_unicode_ci,
  `p54` text COLLATE utf8mb4_unicode_ci,
  `p55` text COLLATE utf8mb4_unicode_ci,
  `p56` text COLLATE utf8mb4_unicode_ci,
  `p57` text COLLATE utf8mb4_unicode_ci,
  `p58` text COLLATE utf8mb4_unicode_ci,
  `p59` text COLLATE utf8mb4_unicode_ci,
  `p60` text COLLATE utf8mb4_unicode_ci,
  `p61` text COLLATE utf8mb4_unicode_ci,
  `p62` text COLLATE utf8mb4_unicode_ci,
  `p63` text COLLATE utf8mb4_unicode_ci,
  `p64` text COLLATE utf8mb4_unicode_ci,
  `p65` text COLLATE utf8mb4_unicode_ci,
  `p66` text COLLATE utf8mb4_unicode_ci,
  `p67` text COLLATE utf8mb4_unicode_ci,
  `p68` text COLLATE utf8mb4_unicode_ci,
  `p69` text COLLATE utf8mb4_unicode_ci,
  `p70` text COLLATE utf8mb4_unicode_ci,
  `p71` text COLLATE utf8mb4_unicode_ci,
  `p72` text COLLATE utf8mb4_unicode_ci,
  `p73` text COLLATE utf8mb4_unicode_ci,
  `p74` text COLLATE utf8mb4_unicode_ci,
  `p75` text COLLATE utf8mb4_unicode_ci,
  `p76` text COLLATE utf8mb4_unicode_ci,
  `p77` text COLLATE utf8mb4_unicode_ci,
  `p78` text COLLATE utf8mb4_unicode_ci,
  `p79` text COLLATE utf8mb4_unicode_ci,
  `p80` text COLLATE utf8mb4_unicode_ci,
  `p81` text COLLATE utf8mb4_unicode_ci,
  `p82` text COLLATE utf8mb4_unicode_ci,
  `p83` text COLLATE utf8mb4_unicode_ci,
  `p84` text COLLATE utf8mb4_unicode_ci,
  `p85` text COLLATE utf8mb4_unicode_ci,
  `p86` text COLLATE utf8mb4_unicode_ci,
  `p87` text COLLATE utf8mb4_unicode_ci,
  `p88` text COLLATE utf8mb4_unicode_ci,
  `p89` text COLLATE utf8mb4_unicode_ci,
  `p90` text COLLATE utf8mb4_unicode_ci,
  `p91` text COLLATE utf8mb4_unicode_ci,
  `p92` text COLLATE utf8mb4_unicode_ci,
  `p93` text COLLATE utf8mb4_unicode_ci,
  `p94` text COLLATE utf8mb4_unicode_ci,
  `p95` text COLLATE utf8mb4_unicode_ci,
  `p96` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`record_id`)
) ENGINE=InnoDB AUTO_INCREMENT=461 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci

'''
