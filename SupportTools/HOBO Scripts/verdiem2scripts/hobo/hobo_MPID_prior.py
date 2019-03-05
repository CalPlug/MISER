# Read the contents of an Onset HOBO Plug Load and Ligt/Motion monitor and parse into Verdiem format then upload to a mySQL table
# Reads in DB, and for all days with known starts and ends, it will push a record to the database with explicit state values
# California Plug Load Research Center, 2018
# by Zihan "Bronco" Chen
# The OnSET HOBO logger must be read in (Uing HOBOWare) then the data exported as a .CSV to use with this script.

# Library setup:
from collections import defaultdict
import scipy.interpolate
import numpy as np
from datetime import timedelta, datetime
import mysql.connector
import pytz
import time

# Operational Settings
table_name = "ExampleDATAfloat"  # Name of database table to write to
desktop_type = "PC"  # User supplied field that identifies the OS that the parsed file was run on.
subject_ID = "120"  # User supplied field that identifies the subject - to be pushed as a string to the DB
MPID = 5 # User supplied field that identifies the subject - to be pushed as a string to the DBName
int_record = 1

#Example Table Creation SQL:
'''CREATE TABLE IF NOT EXISTS `HoboExampleDATA` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;'''


# Define which input file type to process, one or both:  WARNING:Do not set both to False
hobo_1_enabled = True
hobo_2_enabled = True
hobo_1_filename = "mikeofficepc_power_10.24.18.csv"  # File name of Hobo Plug Load Logger exported file
hobo_2_filename = "mikeofficepc_motionandlight_10.24.18.csv"  # File name of Hobo Motion/Light Logger exported file
total_period = 96  # number of columns in DB corresponds to minute resolution (96 is default for 15 minute blocks)
unknown_for_fringes = 1
use_unknown_for_everyday = 1

auto_timezone_adjust = True
auto_timezone = pytz.timezone(
    'America/Los_Angeles')  # Set timezone for subject, assume pacific for California for CalPlug studies
manual_offset = timedelta(hours=7)

# Constants
min_in_day = 1440  # Total minutes in a day, used as a constant, placed here for clarity in code
sec_in_day = 60 * min_in_day

resolution = 900 // MPID
period_in_day = sec_in_day // resolution

time_parse = "%x %I:%M:%S %p"

hobo_1_meaning = [
    ("CPU_Power", "RMS Voltage"),  # row 2
    ("CPU_Power", "RMS Current"),
    ("CPU_Power", "Active Power"),
    ("CPU_Power", "Active Energy"),
    ("CPU_Power", "Apparent Power"),
    ("CPU_Power", "Power Factor"),
    # ("CPU_Power", "Unknown")
]

hobo_2_meaning = [
    ("User", "Light"),  # row 2
    ("User", "Occupancy"),
]

state_list = [
    ("CPU_Power", "RMS Voltage"),  # row 2
    ("CPU_Power", "RMS Current"),
    ("CPU_Power", "Active Power"),
    ("CPU_Power", "Active Energy"),
    ("CPU_Power", "Apparent Power"),
    ("CPU_Power", "Power Factor"),
    ("CPU_Power", "Unknown"),
    ("User_Light", "0"),  # row 2
    ("User_Light", "1"),
    ("User_Light", "Unknown"),
    ("User_Occupancy", "0"),
    ("User_Occupancy", "1"),
    ("User_Occupancy", "Unknown")] \
    if unknown_for_fringes else [
    ("CPU_Power", "RMS Voltage"),  # row 2
    ("CPU_Power", "RMS Current"),
    ("CPU_Power", "Active Power"),
    ("CPU_Power", "Active Energy"),
    ("CPU_Power", "Apparent Power"),
    ("CPU_Power", "Power Factor"),
    ("User_Light", "0"),  # row 2
    ("User_Light", "1"),
    ("User_Occupancy", "0"),
    ("User_Occupancy", "1"),
]


# Functions:

def timeconverter(datetime_currentvalue):
    # datetime_currentvalue /= 1000  # remove from ms and take into seconds
    ts = int(datetime_currentvalue)  # use integer value in seconds
    if auto_timezone_adjust:
        datetime_newvalue = pytz.utc.localize(datetime.utcfromtimestamp(ts)).astimezone(auto_timezone)
    else:
        datetime_newvalue = datetime.utcfromtimestamp(ts) + manual_offset

    # Continue building out this function
    # return datetime object, "pretty date, i.e. 2014-04-30" as string, and Day of the week as a string - each of the return arguments
    return datetime_newvalue


def file_preprocess(hobo_1_filename, hobo_2_filename):
    hobo_1_np = {}
    hobo_2_np = {}
    if hobo_1_enabled:
        with open(hobo_1_filename) as hobo_1:
            hobo_1_lines = hobo_1.readlines()
            hobo_1_data = defaultdict(list)
        for line in hobo_1_lines:
            try:
                seperated = line.split(",")
                if len(list(filter(lambda x: x == "", seperated[1:8]))) != 0:
                    continue
            except:
                continue
            for i in range(2, 8):
                try:
                    if auto_timezone_adjust:
                        hobo_1_data[hobo_1_meaning[i - 2]].append(
                            # (time.strptime(seperated[1], time_parse).timestamp()
                            (
                                auto_timezone.localize(
                                    datetime(*(time.strptime(seperated[1], time_parse)[0:6]))).timestamp()
                                , float(seperated[i])))
                    else:
                        hobo_1_data[hobo_1_meaning[i - 2]].append(
                            (
                                (datetime.strptime(seperated[1], time_parse) + manual_offset).timestamp(),
                                float(seperated[i])))
                except:
                    pass
        for i in hobo_1_meaning:
            hobo_1_data[i].sort()
            hobo_1_np[i] = np.array(hobo_1_data[i], dtype=[("timestamp", np.uint32), ("value", np.float16)])
    if hobo_2_enabled:
        with open(hobo_2_filename) as hobo_2:
            hobo_2_lines = hobo_2.readlines()
            hobo_2_data = defaultdict(list)
        for line in hobo_2_lines:
            try:
                seperated = line.split(",")
                if seperated[2] == "" and seperated[3] == "":
                    continue
            except:
                continue
            try:
                if auto_timezone_adjust:
                    if seperated[2] != "":
                        hobo_2_data[hobo_2_meaning[0]].append(
                            (
                                int(
                                    auto_timezone.localize(
                                        datetime(*(time.strptime(seperated[1], time_parse)[0:6]))).timestamp()
                                )
                                , float(seperated[2])))
                    if seperated[3] != "":
                        hobo_2_data[hobo_2_meaning[1]].append(
                            (int(auto_timezone.localize(
                                datetime(*(time.strptime(seperated[1], time_parse)[0:6]))).timestamp()
                                 ), float(seperated[3])))
                else:
                    if seperated[2] != "":
                        hobo_2_data[hobo_2_meaning[0]].append(
                            (int((datetime.strptime(seperated[1], time_parse) + manual_offset).timestamp()),
                             float(seperated[2])))
                    if seperated[3] != "":
                        hobo_2_data[hobo_2_meaning[1]].append(
                            (int((datetime.strptime(seperated[1], time_parse) + manual_offset).timestamp()),
                             float(seperated[3])))
            except:
                pass
        for i in hobo_2_meaning:
            hobo_2_data[i].sort()
            hobo_2_np[i] = np.array(hobo_2_data[i], dtype=[("timestamp", np.uint32), ("value", np.float16)])
    return hobo_1_np, hobo_2_np


firstDate = None


def hobo_process(hobo_1_np: dict, hobo_2_np: dict):  # output will be 15 minutes chunks
    global firstDate
    if hobo_1_enabled:
        dayRange = (datetime.fromtimestamp(next(iter(hobo_1_np.values()))[-1][0]).date() - datetime.fromtimestamp(
            next(iter(hobo_1_np.values()))[0][0]).date()).days + 1
        firstDate = datetime.fromtimestamp(next(iter(hobo_1_np.values()))[0][0])
    else:
        dayRange = (datetime.fromtimestamp(next(iter(hobo_2_np.values()))[-1][0]).date() - datetime.fromtimestamp(
            next(iter(hobo_2_np.values()))[0][0]).date()).days + 1
        firstDate = datetime.fromtimestamp(next(iter(hobo_2_np.values()))[0][0])

    sectionRange = dayRange * period_in_day
    result = np.zeros((len(hobo_1_meaning) + (1 if unknown_for_fringes else 0) + len(hobo_2_meaning) * (
        3 if unknown_for_fringes else 2), sectionRange),
                      np.float16)
    if hobo_1_enabled:
        firstSlot_min = datetime(3000, 1, 1, 0, 0, 0).timestamp()
        knownSlotCount_max = 0
        for (device, state), values in hobo_1_np.items():
            firstSlotTimestamp = int(
                timeconverter(values[0][0]).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            # firstSlotTimestamp = values[0][0] - (values[0][0] % (min_in_day * 60))
            interpFunc = scipy.interpolate.interp1d(values["timestamp"], values["value"], fill_value="extrapolate")
            firstSlot = (values[0][0] - firstSlotTimestamp) // (86400 // period_in_day)
            if firstSlot < firstSlot_min:
                firstSlot_min = firstSlot
            knownSlotCount = (values[-1][0] - values[0][0]) // (86400 // period_in_day)
            if knownSlotCount > knownSlotCount_max:
                knownSlotCount_max = knownSlotCount
            for i in range(firstSlot, firstSlot + knownSlotCount):
                result[hobo_1_meaning.index((device, state)), i] = interpFunc(
                    firstSlotTimestamp + i * (86400 // period_in_day))
        result[len(hobo_1_meaning), :firstSlot_min] = 1
        result[len(hobo_1_meaning), firstSlot_min + knownSlotCount_max:] = 1

    if hobo_2_enabled:
        minutesRange = dayRange * 24 * 60
        secondsRange=minutesRange*60
        possible_states = 3 if unknown_for_fringes else 2

        for (user, device), values in hobo_2_np.items():
            firstSlotTimestamp = int(
                timeconverter(values[0][0]).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            minutesState = np.zeros((possible_states, secondsRange), np.int8)
            # sequence: 0, 1, unknown (for axis 0 of minuteState)
            if unknown_for_fringes:
                minutesState[2, :] = 1
            for timestamp, value in values:
                if int(value) == 1:
                    minutesState[1, (timestamp - firstSlotTimestamp) ::] = 1
                    minutesState[0, (timestamp - firstSlotTimestamp) ::] = 0
                else:
                    minutesState[0, (timestamp - firstSlotTimestamp) ::] = 1
                    minutesState[1, (timestamp - firstSlotTimestamp) ::] = 0
                if unknown_for_fringes:
                    minutesState[2, (timestamp - firstSlotTimestamp) ::] = 0
            minutesState[(0, 1), (values[-1][0] - firstSlotTimestamp) + 1::] = 0
            if unknown_for_fringes:
                minutesState[2, (values[-1][0] - firstSlotTimestamp)  + 1::] = 1
            reshaped = np.reshape(minutesState, (
                possible_states, dayRange * period_in_day, sec_in_day // period_in_day)).sum(
                axis=2)
            result[len(hobo_1_meaning) + (1 if unknown_for_fringes else 0) + hobo_2_meaning.index((user, device)) * possible_states:
                   len(hobo_1_meaning) + (1 if unknown_for_fringes else 0) + hobo_2_meaning.index((user, device)) * possible_states + possible_states,::] = reshaped

    return result


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield tuple(l[i:i + n])


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

# print out loaded JSON for testing
# for x in data:
#     print("%s: %d" % (x, data[x]))  # adjust fields in this example
# subjectIdDict = defaultdict(list)
# subjectIdCounter = 1

# create string "p1, p2 ,p3 ..... p96"
p_series_in_query = ", ".join(["p" + str(i) for i in range(1, 97)])

# Open Database connection
db = mysql.connector.connect(host="XXXXXXX.calit2.uci.edu",  # host
                             user="XXXXXXXX",  # username
                             passwd="XXXXXXX",  # password
                             db="XXXXXXXX")  # DBName
# db=None
cursor = db.cursor()  # Cursor object for database query

result = hobo_process(*file_preprocess(hobo_1_filename, hobo_2_filename))
# print(device)


# datesCountained = len(list(slotDict.values())[0])

querys = []

if hobo_1_enabled and hobo_2_enabled:
    validStateList = state_list
elif hobo_1_enabled:
    validStateList = state_list[0:6]
else:
    validStateList = state_list[6::]

for device, status in validStateList:
    currentDate = firstDate

    for chunk in chunks(result[state_list.index((device, status)), :], period_in_day):
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
        # + ",".join(map(str, chunk)) \

        querys.append((currentDate, (device, status), query))
        currentDate += timedelta(days=1)
querys.sort(key=lambda x: (x[0], state_list.index(x[1])))
all_zeroes = ",".join(total_period * ["0"])
list(map(lambda x: print(x[2]), filter(lambda x: all_zeroes not in x[2], querys)))
list(map(lambda x: cursor.execute(x[2]), filter(lambda x: all_zeroes not in x[2], querys)))
db.commit()
db.close()  # close DB connection