#Analysis script for Verdiem data from CalPlug 2014 study - Idle Time Reporter using XOR
#Developed by M. Klopfer Sept 11, 2018 - V1.0 
#Ver 2.1, Oct 15,2018 - major restructure in function by MJ Klopfer
#Ver 2.2, 12/18/18  - Zihan "Bronco" Chen (streamline function and operation)
#Ver 2.5, 01/17/2019 - Liangze Yu (Updated block analysis algorithm)
#Operation:  This script is a stand-alone processor that takes the Verdiem data and formats it into a style used as a .CSV input into the PLSin program.  This script will not actually output a .CSV file its current state, just format the text in a way that can be quickly formatted into the specific PLSim format.   
            #The script reads from a database/table with the following entries:  record_id    subject_identifier    desktop_type    MPID    device    status    int_record    date    day_of_week PM_Setting (in new version)  P1  P2...[There are 96 entries that correspond to 15 minute periods across the day]

#Dependencies and setup considerations:  This program uses Miniconda/Eclipse in development and is within an Eclipse workshop - it shares identical dependencies as PLSim:  https://github.com/CalPlug/PlugLoadSimulator-PLSim, in final form it can be run also from Python in a console window.  This can be helpful to run multiple variants of this script at once to save processing time.
#Note, if the console is not large enough in Eclipse to display the return, consider the following:  https://stackoverflow.com/questions/2600653/adjusting-eclipse-console-size
#Warning, on a big enough data set this script can take DAYS to run.  Plan accordingly!

#Library imports
import sys
import calendar
from datetime import timedelta, datetime, date, time
from time import mktime
#Cython accelerator used to expedite Python processing operation
import pyximport; pyximport.install() #pyximport cython accelerator, partially implemented to accelerate processing
import mysql.connector
from distutils.core import setup #Processing speedup
from Cython.Build import cythonize  #Processing speedup, not fully implemented
import numpy as np
import itertools

#Parameters for data entry
periodlength=15  #assume a standard 15 min period length
totalperiods = 96  #total number of columns devoted to the periods
periodstartcolumn = 10 #column which the period info starts in
record_idrow = 0 #position original identifier is in
subjectrow = 1 #Row the subject info is in
desktop_typerow = 2 #position for the desktop type info
MPIDrow = 3 #row MPID info is placed in
devicerow = 4 #Identify row for the device (CPU or USER) that is being reported on 
stateposition = 5 #position of the state/status identifier column
int_recordrow = 6
daterow = 7 #row that date information is placedin
day_of_weekrow = 8 #Day of the Week identifier
powermanagementsetting = 9 #Column holding the PM setting value for the computers
minutebaseinday = 1440 #Time base for min in day, Verdiem based periods= 1425 (minus one period), alternatively 1440

#Run Options for Output Formatting
elapsedminortime=True #Print headers as time (False) or minutes since 00:00 (True)
writetosummarydb = True #Option allows summary to be written to a summary table in the database
modifyoutputfordayanalysis = True
countidleactive = False  #used to turn on the counting of 0 states in Idle only days as Active, may cause double Active counts or unknowns to count as active versus removal

# IDLE INTERPOLATION SETTINGS
IDLE_INTERPOLATION = True #Set to FALSE disable idle block interpolation, in this case, the raw idle blocks per period will be analyzed without any combination.  Note: idle blocks are split by the 15 min period often, this feature helps to mitigate this effect!  
INTERPOLATION_SETTING = 1  #If enabled, two options are possible, option 1 combines blocks based on largest nearest neighbor (forst order).  Option 2 performs the operation as in option 1 and then performs a second pass to verify block edges are combined in largest grouping (layer check), checks for known artifact of monotonic block sizes being discontinious 

#define global variables
dbrecordpost = 0  #counter for posted records, this is a null initial value, this displays in the console output
dayanalysis = -1  #Default value for this counter - set the scope of this outside the for loop for each run, this is an invalid initial value to be reset when used


 

#Run Option Parameters - Iteration Parameters
#subjectlist = [3, 10, 21, 29, 30, 35, 43, 46, 48, 51, 60, 82, 85, 89, 100, 105]

#other subjects subsets that can be used for "subjectlist"
#subjectlist = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115]
#ComputerswithoutPM = [1, 2, 4, 5, 6 ,7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 84, 85, 87, 88, 89, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 110, 111, 112, 113, 114, 115]
#ComputerswithPM = [3, 10, 21, 29, 30, 35, 43, 46, 48, 51, 60, 82, 85, 89, 100, 105]
#ComputerPMValues= [60, 10, 30, 180, 240, 30, 10, 120, 25, 10, 180, 10, 60, 30, 30, 60] 

#Joy's R4 Monitoring study values, cross-confirmed
#subjectlist = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118]
#ComputerswithoutPM = [1, 2, 4, 5, 6 ,7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 44, 45, 47, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 65, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 81, 82, 84, 85, 87, 88, 89, 91, 92, 93, 94, 95, 96, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118]
subjectlist = [3, 10, 21, 29, 30, 35, 43, 46, 48, 52, 61, 83, 86, 90, 101, 106]
#ComputerPMValues= [25,60, 60, 10, 30, 180, 10, 240, 60, 10, 30, 10, 120, 180, 30, 30]  


sensorsettingvalues = [5,10,15,20,25,30,35,40,45,50,55,60,120,180,240,300]  #must be above 0 for savings to make sense
#deltaWcomputerpower = [20, 30, 40, 50, 60, 80, 100, 120, 150] #must be above 0
deltaWcomputerpower = [0.000000001, 20, 50, 100] #must be above 0,  (0.000000001) simulates a near zero (0W) load for Controlled Load Only calculation for APS usage
standbycomputerwatt = [0] #energy used by the computer in standby, assumed 0 for pure delta calculations (you need to add back in the factor to subtract based on runtime later for the new state that the save action is forcing to enter)

deltaWaccessoriespower = [5, 10, 20, 50, 100, 500] #auto include power req =0 (null case separate)
#other subsets/parameters to use:
#deltaWaccessoriespower = [5, 10, 20]
#deltaWaccessoriespower = [1, 1.46, 5, 10]
#pmSettings = [5, 10, 15, 20, 30, 45, 60, 120]  #auto include PM=0/PM OFF
pmSettings = [5,10,15,30,40,60,120,180,240,300]

#Settings for formatting the output to be entered
dowsetting = [0, 1, 2]  #0: All Days, 1: Weekdays only, 2: Weekends only  all 3 outputs the Simulation for each period, calculated one after the next for all subjects/


##Program Functions   
def discontinuity_solver(idle_array): #Idle block combination interpolation engine

    # 1,04/10/14 typical case to look at
    new_idle = np.array(idle_array)
    new_idle = np.reshape(new_idle,(int(len(new_idle)/15),15))

    # Flip for Largest
    for num,block in enumerate(new_idle):
        # TEST IF 0, 15 OR LAST 15 BLOCK
        if np.sum(new_idle[num, :]) != 0 and np.sum(new_idle[num, :]) != 15 and num != 0:
            # The code written at 500-700 lines always fills in 0s in front of 1s in IDLE CUZ ACTIVE is 1s in front of 0s
            if num == (len(new_idle)-1):
                new_idle[num,:] = block[::-1]
            else:
                left = np.sum(new_idle[num-1,:])
                right = np.sum(new_idle[num+1,:])
                if left > right:
                    new_idle[num, :] = block[::-1]

    if INTERPOLATION_SETTING == 1:
        # ONLY LARGEST
        return list(new_idle.reshape((1,len(idle_array)))[0])

    # Flip block idle order for continuity
    for num, block in enumerate(new_idle):
        if np.sum(new_idle[num, :]) != 0 and np.sum(new_idle[num, :]) != 15 and num != (len(new_idle) - 1) and num != 0:
            if new_idle[num,0] != new_idle[num-1,-1] and new_idle[num,-1] != new_idle[num+1,0]:
                new_idle[num, :] = block[::-1]

    return list(new_idle.reshape((1,len(idle_array)))[0])
         
def query_updatevalue(key_to_find, definition):
    for key in query_modifications.keys():
        if key == key_to_find:
           query_modifications[key] = definition
           
def safe_div(x,y):  #Totaling float division that will give 0 versus "div. by 0" error if no state is present for a certain day when calculating ratios
    if y == 0:
        return 0
    return x / y
           
#Used to search for transitions in the dateset to identify timing:
def transitionsearch(inputarray, mask, noforcedaytransition):  #input array used for comparison and a mask of always
    equallength=1
    if ((len(inputarray)!=len(mask))):
        sys.stdout.write("Warning: The input mask is not the same length as the input array! The input / mask lengths are: ")
        sys.stdout.write(str(len(inputarray)))
        sys.stdout.write(" / ")
        print(len(mask))
        mask=inputarray #to allow it to calculate
        equallength=0
    results = []
    resultswithstart= []
    resultswithstartandstop = []
    option1start = ((0,0))
    option2start = ((0,1))   
    updatedoptionend = []  #needs to be a list, converted to tuple later upon insert
    endval = 0 #Initialize this variable
    startval = 0
    emptyreturn = 0
    
    deltastate = []
    deltanotstate = []
    deltacombined = []
    deltaactiveOn = []
    deltaactiveOff = []

    results = [(n+1, b) for (n, (a,b)) in enumerate(zip(inputarray,inputarray[1:])) if a!=b]  #Return the point of transition (to the new value) and the prior value to the transition as the function return  
    resultswithstart.extend(results)
    if (inputarray[0]==inputarray[1]):  #take care of the 0 case and add it to the array "resultswithstart"
        if (inputarray[0]==0):
            resultswithstart.insert(0, tuple(option1start))
            startval = 1  #reversed order to update the endval for the no-transition case
            if (noforcedaytransition == True):  #feature not fully implemented - intent to make it so the day doesn't break if the run continues over a day transition
                startval = 0
        if (inputarray[0]==1):
            resultswithstart.insert(0, tuple(option2start))
            startval = 0 #reversed order to update the endval for the no-transition case
            if (noforcedaytransition == True):
                startval = 1
                
    resultswithstartandstop.extend(resultswithstart)
    
    if (len(results) == 0):
        emptyreturn = 1
        updatedoptionend.append(len(inputarray)) #rem-1
        updatedoptionend.append(startval)
        t= tuple(updatedoptionend)
        resultswithstartandstop.insert((len(inputarray)+1), t)   #rem-1
    
    else:
            
        if (inputarray[len(inputarray)-2]==inputarray[len(inputarray)-1]):  #take care of the 0 case, change the last value to force an end of day transition 
            if (inputarray[len(inputarray)-1]==1):
                endval=0
            if (inputarray[len(inputarray)-1]==0):
                endval=1
                
            updatedoptionend.append(len(inputarray)) #rem-1
            updatedoptionend.append(endval)
            if (len(results)!= 0):
                t= tuple(updatedoptionend)
                resultswithstartandstop.insert((len(inputarray)), t)   #rem-1
           
    for x in range(0, len(resultswithstartandstop)-1):     
        deltacombined.append(resultswithstartandstop[x+1][0] - resultswithstartandstop[x][0])
        if (resultswithstartandstop[x][1] == 1): #count transition to idle and period until activity comes back
            deltastate.append(resultswithstartandstop[x+1][0] - resultswithstartandstop[x][0])
        if (resultswithstartandstop[x][1] == 0):    
            deltanotstate.append(resultswithstartandstop[x+1][0] - resultswithstartandstop[x][0])
        #mask based analysis
        #Mask 1      
        if (resultswithstartandstop[x][1] == 0 and mask[resultswithstartandstop[x][0]] == 1):    
            deltaactiveOn.append(resultswithstartandstop[x+1][0] - resultswithstartandstop[x][0])
        #Mask 0
        if (resultswithstartandstop[x][1] == 0 and mask[resultswithstartandstop[x][0]] == 0):
            deltaactiveOff.append(resultswithstartandstop[x+1][0] - resultswithstartandstop[x][0])
      
    #add in zero values if there is no return
    if (len(deltacombined)==0):
           deltacombined.append(0)   
    if (len(deltastate)==0):
           deltastate.append(0)  
    if (len(deltanotstate)==0):
           deltanotstate.append(0)    
    if (len(deltaactiveOn)==0):
           deltaactiveOn.append(0)  
    if (len(deltaactiveOff)==0):
           deltaactiveOff.append(0)     
           
    #switch to return -1 if states are not matched for the masks as a warning
    if (equallength==0):
        del deltaactiveOn[:]
        del deltaactiveOff [:]
        deltaactiveOn.append(-1)  
        deltaactiveOff.append(-1)    
           
    return [resultswithstartandstop, deltastate, deltanotstate, deltacombined, deltaactiveOff, deltaactiveOn, results, resultswithstart]

def savingsevaluation(inputarray, timersetting, simulatedPMSetting, PMSimulationOn):  #Simulation savings calculator
    savingsarray=[]
    newlist=[int(x) for xs in inputarray for x in xs]
    sys.stdout.write(",,Raw Idle Times List:, ")
    print(str(newlist)) #To print entry values
    for index in range(0, (len(newlist))):
        if ((PMSimulationOn == True) and (simulatedPMSetting>0)):  #no negative savings, for PMSimulation - Preconditioner and savings application - this applies a simulated power management to the data.
            if (newlist[index] > simulatedPMSetting): #initial verification of values to prevent negative savings
                newlist[index] = simulatedPMSetting #if the PM works, now only the time to this PM value is used
                if (newlist[index] < timersetting):  #with PMSim On, verify check for no negative savings
                        savingsval=0
                        savingsarray.append(int(savingsval))
            
                else:
                    savingsval = (newlist[index] - timersetting) #if not negative, with PM on, apply savings
                    savingsarray.append(int(savingsval))
            else:  #where PM setting is larger than savings value
                savingsval=0  #no savings possible, PM catches the idle already
                savingsarray.append(int(savingsval))
                
                
        else:  # for PMSimulation in OFF State
            if (newlist[index] < timersetting):  #check to make sure no negative savings
                savingsval=0
                savingsarray.append(int(savingsval))
            
            else:
                savingsval = (newlist[index] - timersetting) #apply savings
                savingsarray.append(int(savingsval))
    sys.stdout.write(",,Savings Period List:, ")
    print(str(savingsarray)) #To print entry values
    return [savingsarray]
            
def pushsummarytodb(generatetable, table_name, subject_identifier, desktop_type, MPID, delta_computer_W, delta_acessories_W, external_pm_control_min, invervention_setting_min, reporting_type, summary_idle_percent, summary_total_time_idle_percent, runtime_saved_per_day_min, runtime_eval_period_saved_hr_per_year, projected_per_day_savings_kWh, total_days_estimate_kwhperyear_kWh):
    entryData = []
    if (generatetable == True):
        # Sample table create sequence 
        #cursor.execute("CREATE TABLE IF NOT EXISTS `summary` (  `subject_identifier` tinyint(4) DEFAULT NULL, `desktop_type` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT '', `MPID` tinyint(4) DEFAULT NULL, `summary_idle_percent` float DEFAULT NULL, `delta_computer_W` int(11) DEFAULT NULL, `delta_acessories_W` int(11) DEFAULT NULL, `external_pm_control_min` float DEFAULT NULL, `invervention_setting_min` float DEFAULT NULL, `runtime_saved_per_day_min` float DEFAULT NULL,  'runtime_saved_per_day_Hrperyear' float DEFAULT NULL, `projected_per_day_savings_kWh` float DEFAULT NULL, `weekday_only_estimate_kwhperyear` float DEFAULT NULL, `all_days_even_estimate_kwhperyear` float DEFAULT NULL, `weekday_and_weekends_estimate_kwhperyear` float DEFAULT NULL, `min_max_estimate_ratio` float DEFAULT NULL, `update_time` datetime DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        print("Not Fully implemented: nothing to see here.  Use the comments in the code to manually generate the table")
        '''
        Example table used by this script:
            CREATE TABLE IF NOT EXISTS `summary12` (
              `subject_identifier` tinyint(4) DEFAULT NULL,
              `desktop_type` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '',
              `MPID` tinyint(4) DEFAULT NULL,
              `delta_computer_W` float DEFAULT NULL,
              `delta_acessories_W` float DEFAULT NULL,
              `external_pm_control_min` int(10) DEFAULT NULL,
              `invervention_setting_min` int(10) DEFAULT NULL,
              `reporting_type` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '',
              `summary_idle_percent` float DEFAULT NULL,
              `summary_total_time_idle_percent` float DEFAULT NULL,
              `runtime_saved_per_day_min` float DEFAULT NULL,
              `runtime_eval_period_saved_hr_per_year` float NOT NULL,
              `projected_per_day_savings_kWh` float DEFAULT NULL,
              `total_days_estimate_kwhperyear_kWh` float DEFAULT NULL,
              `update_time` datetime DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        '''
      
    #Assemble array to insert into DB
    entryData.append (subject_identifier)
    entryData.append (desktop_type)
    entryData.append (MPID)
    entryData.append (delta_computer_W)
    entryData.append (delta_acessories_W)
    entryData.append (external_pm_control_min)
    entryData.append (invervention_setting_min)
    entryData.append (reporting_type)
    entryData.append (summary_idle_percent)
    entryData.append (summary_total_time_idle_percent)
    entryData.append (runtime_saved_per_day_min)
    entryData.append (runtime_eval_period_saved_hr_per_year)
    entryData.append (projected_per_day_savings_kWh)
    entryData.append (total_days_estimate_kwhperyear_kWh)
    entryData.append (str(datetime.now()))
    print (str(entryData)) #print out contents of array to enter
    
    #Insert the array by element into the DB
    cursor.execute('''INSERT IGNORE INTO %s (subject_identifier, desktop_type, MPID, delta_computer_W, delta_acessories_W, external_pm_control_min, invervention_setting_min, reporting_type, summary_idle_percent, summary_total_time_idle_percent, runtime_saved_per_day_min, runtime_eval_period_saved_hr_per_year, projected_per_day_savings_kWh, total_days_estimate_kwhperyear_kWh, update_time) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)''' % table_name, (entryData))
    db.commit()

    print ("Records posted to DB - Total this session:")
    global dbrecordpost  #need to make a reference to this global variable
    dbrecordpost=dbrecordpost+1
    print(dbrecordpost)
    
def savingsreporting(inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdayswitch):
    deltawatt = activecomputerwatt - standbycomputerwatt
    deltawattaccessories = activeaccessorieswatt - standbyaccessorieswatt 
    reportingtype = "Default"  #Initialize the reporting type return
    reportedval = 0.5 #Initialize the reporting type return
    if (accessorycontrol==False):
        deltawattaccessories =  0
        
    #this needs to be implemented fully - to be finished!  This accounts for the break between days that breaks up contiguous idle periods crossing day boundaries
    '''
    if (breakdayswitch ==True): #This combines elements that are full days to combine with the next day - assume the computer was left on overnight
        for indq, deltaval in enumerate(inputrange):
            currentlength=0 #set the current length value as 0
            while (currentlength == len(inputrange)):
                if ((inputrange[indq] == minutebaseinday) #if there is a 100% idle day
                    inputrange[indq]=(inputrange[indq] + inputrange[indq+1])
                    del inputrange[indq+1]  #after the value has been added, remove the combined value in the array and reorder so this element is removed (collapse array into that removed value)
                 #consider array length change to make sure this does not run off the array
    '''
    print()
    print()
    print(",,++++++++++++++++++++++++++++++++++++++++++++++++++++")
    for x in analysisvalues:
        print()
        savingsarrayreturn = savingsevaluation (inputrange, x, simPMsavingsval,simPMsavingsOn)     
        if (simPMsavingsOn == False):
            simPMsavingsval = 0 #set to 0 if not enabled for reporting  
        sys.stdout.write(",,Applied savings for intervention at (min):,")
        sys.stdout.write(str(x))
        print()
        if (simPMsavingsOn ==True):
            sys.stdout.write(",,Simulated Pre-PM Savings Applied at (min):,")
            sys.stdout.write(str(simPMsavingsval))
            print()
        else:
            sys.stdout.write(",,No Simulated Pre-PM Savings Applied, accordingly the setting is (min):,")
            sys.stdout.write(str(0))
            print()
            
        savingssum = 0 #Initialize the sum counter
        for index in range(0, len(savingsarrayreturn)):
            if (len(savingsarrayreturn) == 1):
                savingssum = 0    #Bypass if no records exist
            else:
                savingssum = savingssum + int(savingsarrayreturn[0][index])
        sys.stdout.write(",,Total (min) Runtime Saved:,")
        print(str(sum(savingsarrayreturn[0])))
        sys.stdout.write(",,Calculation day baseline used (evaluated period of study days):,")
        print(str(averagebase))
        perday= (safe_div(sum(savingsarrayreturn[0]),(averagebase)))  #avoid a div by 0
        sys.stdout.write(",,Per study day (evaluation period) day (min/day) Runtime Saved:,")
        print(str(perday))
        sys.stdout.write(",,Per study day (evaluation period) day (Hr) Runtime Saved:,")
        rawruntimehr=(perday/60) #per study day average
        print(str(rawruntimehr))
        sys.stdout.write(",,Idle Periods per days with Idle for all days in selected evaluation scope (AVG):")
        sys.stdout.write(',')
        sys.stdout.write('{0:.2f}'.format(totalidleperiods/resultsreviewcount))
        print()
        
        if(dayanalysis == 0):
            sys.stdout.write(",,Per AllDays (Hr/year) Runtime Saved:,")
            runtimehr = (perday/60)*365
           
        elif (dayanalysis == 1):
            sys.stdout.write(",,Per WeekDays (Hr/year) Runtime Saved:,")
            runtimehr = (perday/60)*261
           
        elif (dayanalysis == 2):
            sys.stdout.write(",,Per Weekend Days (Hr/year) Runtime Saved:,")
            runtimehr = (perday/60)*104
        
        print(str(runtimehr))
        
        
        if (energyreport == True):
            sys.stdout.write(",,Projected per day Energy Saved considering a delta of ")
            #calculations
            sys.stdout.write(str(deltawatt))
            Wcontacess = (((deltawatt+deltawattaccessories)/1000)*rawruntimehr)
            sch1= (((deltawatt+deltawattaccessories)/1000)*rawruntimehr)*261
            sch2 = (((deltawatt+deltawattaccessories)/1000)*rawruntimehr)*365
            sch3 = ((((deltawatt+deltawattaccessories)/1000)*rawruntimehr)*261)+(104*rawruntimehr*(deltawatt/1000))  #treating all days as even - this you need all days to calculate
            sch4 = (((deltawatt+deltawattaccessories)/1000)*rawruntimehr)*104 #this is a weekends only deriv. schedule used with modified output

            #This is a tip-off indicator 
            if (dayanalysis == 0):  #All Days
               #sch 2 passes - all days even avg
               sch1 = -1
               sch3 = -1
               minMaxRatio = 0
               minMidRatio = 0
               midMaxRatio = 0
               reportingtype = "AllDays"
               reportedval = sch2
            if (dayanalysis == 1):  #Week Days
               #sch 1 passes - weekdays only avg
               sch2 = -1
               sch3 = -1
               minMaxRatio = 0
               minMidRatio = 0
               midMaxRatio = 0
               reportedval = sch1
               reportingtype = "WeekDays"
            if (dayanalysis == 2):  #All Days
               #sch4 passes as sch3 - weekend days only average
               sch3 = sch4 #sch3 is artificial average, use 4 instead
               sch2 = -1
               sch1 = -1
               minMaxRatio = 0
               minMidRatio = 0
               midMaxRatio = 0
               reportedval = sch4
               reportingtype = "Weekends"
                                    
            sys.stdout.write(" W and ")
            sys.stdout.write(str(deltawattaccessories))
            sys.stdout.write(" W controlled accessories ")
            sys.stdout.write("-- presented in (kWh/day):,")
            sys.stdout.write(str(Wcontacess))
            print()
            sys.stdout.write(",,Reported value Projected per Year Energy Saved -- presented in (kWh/year):,")
            sys.stdout.write(str(reportedval))
            print()
            sys.stdout.write(",,Reporting Period:,")
            sys.stdout.write(reportingtype)
            print()
            sys.stdout.write(",,Current Analysis Subject:,")
            sys.stdout.write(str(valuesubject)) #print out the current subject being processed
            print()
            
        print (",,______________________________________________")
        if (writetosummarydb == True):
            pushsummarytodb(False, tablename, str(row[1]), str(row[2]), str(row[3]), str(deltawatt), str(deltawattaccessories), str(simPMsavingsval), str(x), reportingtype, str(safe_div(sum(directidlevsonaverage),((len(directidlevsonaverage))))), str(safe_div(sum(sumallidleperiodsaverage),(len(sumallidleperiodsaverage)))), str(perday), str(runtimehr), str(Wcontacess), str(reportedval)) #Push to DB, do not re-generate table
        
    print(",,**********************************************")

def recursive_len(item): # determines the number of elements in an arb. sized array
    if type(item) == list:
        return sum(recursive_len(subitem) for subitem in item)
    else:
        return 1
      
#Begin main program
# Open database connection
if __name__=="__main__":
    db = mysql.connector.connect(host="cplamp.calit2.uci.edu",    # host
                         user="calplug",         # username
                         passwd="123456!",  # password
                         db="VerdiemStudy")        # DBName
    cursor = db.cursor() # Cursor object for database query
    tablename = "summary14" #Used as the DB table

    query = ("SELECT * FROM DATAUPDATED3 "
             "WHERE subject_identifier = %(s_ID)s AND (date BETWEEN %(start_DATE)s AND %(end_DATE)s)") #base query

    #Default Query for device states - change subject number to view other subjects
    query_modifications= {'s_ID': 1,'start_DATE': "2014-01-01",'end_DATE': "2014-12-31"} #query records updated by defined variables in dictionary, for device, start and end dates - alternatively use this style for datetime hire_start = datetime.date(1999, 1, 1), for date time printout: #for (first_name, last_name, hire_date) in cursor: print("{}, {} was hired on {:%d %b %Y}".format(last_name, first_name, hire_date))
    #Leave this default alone in most use cases if the general query is unchanged, update the values by the function provided to perform a replacement on this dictionary object

    #Iterate thru parameters to build out analysis

    for valuek in (dowsetting):
        sys.stdout.write("Run with Day of Week Analysis Setting (0:All Days, 1:Weekdays, 2:Weekends): ")
        print(valuek)
        dayanalysis = valuek #set the day of week analysis switch as a global variable.
            #with no PM settings, no accessory control

        for q, valuesubject in enumerate(subjectlist):
            directidleaverage = []
            directidlevsonaverage = []
            directonlyidleaverage = []
            sumallidleperiodsaverage = []
            finaldeltalist =[] #holder for the total collected delta values across all days
            resultsreviewcount = 0 #keep track of counted records for the basis for each subject
            rowreviewcountUSERUnknown = 0 #keep track of unknown records
            rowreviewcountCPUUnknown = 0  #keep track of unknown records
            idleaverage = 0  #set the global variable to this for each subject run
            perdayidleaverage = 0 #set the global variable to this for each subject run
            perdayidleaverageonly = 0
            idleaveragealldays = 0
            idleaveragealldaysthree = 0
            idleaveragealldaystwo = 0

            query_updatevalue('s_ID', (valuesubject))  #add 1 because q starts at index 0 add 1 because q starts at index 0 - this is driven by the list and automatically entered by the for loop
            print(query_modifications)

            cursor.execute(query, query_modifications) #Process query with variable modifications
            queryreturn = cursor.fetchall() #Fetch all rows with defined query for first state

            #collect and display unique states in the query
            subjecttallylist = [] #total list of states found
            subjecttallylist_unique_list = []  # intitalize a null list
            statetallylist = [] #total list of states found
            statetallylist_unique_list = [] #total list of states found
            datetallylist = []  #total list of dates found
            datetallylist_unique_list = []  #total list of dates found

            for rowindex, row in enumerate(queryreturn): #go thru query and generate a full list of states from the dataset
                statetallylist.append(row[stateposition])
                subjecttallylist.append(row[subjectrow])
                #check to see if there is a day of week limiter in use
                if (dayanalysis==0):
                    if (('Sunday'in row[day_of_weekrow]) or ('Monday'in row[day_of_weekrow]) or ('Tuesday'in row[day_of_weekrow]) or ('Wednesday'in row[day_of_weekrow]) or ('Thursday'in row[day_of_weekrow]) or ('Friday'in row[day_of_weekrow]) or ('Saturday'in row[day_of_weekrow]) or ('Sunday'in row[day_of_weekrow])):   #all days
                        datetallylist.append(row[daterow])
                elif (dayanalysis==1):
                    if (('Monday'in row[day_of_weekrow]) or ('Tuesday'in row[day_of_weekrow]) or ('Wednesday'in row[day_of_weekrow]) or ('Thursday'in row[day_of_weekrow]) or ('Friday'in row[day_of_weekrow])):     #Weekdays
                        datetallylist.append(row[daterow])
                elif (dayanalysis==2):
                    if (('Sunday'in row[day_of_weekrow]) or ('Saturday'in row[day_of_weekrow])):  #Weekends
                        datetallylist.append(row[daterow])

            for x in statetallylist:
                # check if exists in unique_list or not
                if x not in statetallylist_unique_list:
                    statetallylist_unique_list.append(x)

            for x in subjecttallylist:
                # check if exists in unique_list or not
                if x not in subjecttallylist_unique_list:
                    subjecttallylist_unique_list.append(x)

            for x in datetallylist:
                # check if exists in unique_list or not
                if x not in datetallylist_unique_list:
                    datetallylist_unique_list.append(x)


            #Print out Headers for CSV (Console output can be used as a CSV for further analysis)
            sys.stdout.write("Subject,Date,State/Info,")
            for x in range(0, (totalperiods*periodlength)): #96 sets of 15 minute periods for all minutes in a 24 hour period
                timeholder = datetime.today() #initialize datetimeobject
                timeholder = (datetime.combine(date.today(), time(0,0,0)) + timedelta(minutes=1*x))
                if(elapsedminortime==False):
                    sys.stdout.write(datetime.strftime(timeholder, '%H:%M:%S'))
                else:
                    sys.stdout.write(str(x))
                if ((x<(totalperiods*periodlength)-1)): #suppress final comma
                    sys.stdout.write(",")
            print () #print newline after the header row is finished

            for listeddate in datetallylist_unique_list: # display entries for a single date
                ontimelist = []
                offtimelist = []
                sleeptimelist = []
                activetimelist = []
                intermediatedirectidletimelist = []
                directidletimelist = []
                sumactivevsidleperiodsdaily = 0
                sumallidleperiodsdaily = 0
                sumactiveperiodsdaily = 0
                sumoffperiodsdaily = 0
                sumsleepperiodsdaily = 0
                activeperiods=[]
                idlethisday=0
                directoffperiods=[]
                directsleepperiods = []
                offperiods=[]
                directidleperiods = []
                logicalstate = []
                returnresultsXORWaste=[]
                returnresultsdirectidleperiods =[]
                datecounted=0 #reset the single option check
                CPUUnknowndetectedday=0  #reset the single option check for the unknown
                UserUnknowndetectedday=0  #reset the single option check for the unknown
                UserdirectIdledetectedday = 0
                # total the max time in unknown state per day and use this basis - warning!  This is a shortcut that may need to be adjusted later
                # set the total minutes as the basis for counting
                rowreviewcountCPUUnknown= minutebaseinday
                # set the total minutes as the basis for counting
                rowreviewcountUSERUnknown= minutebaseinday
                # page thru all returned rows from query, also return an index number related to the row
                for rowindex, row in enumerate(queryreturn):
                    # reset the single option check for the unknown
                    CPUUnknowndetectedrow=0
                    # reset the single option check for the unknown
                    UserUnknowndetectedrow=0
                    rowreviewcountCPUUnknownintercount = minutebaseinday
                    rowreviewcountUSERUnknownintercount = minutebaseinday


                    if ((('On') in row[stateposition]) and (row[daterow] == listeddate)):
                        # (INSERT "("%B %d, %Y")" after %d to have commas in name.  Print out the datetime for each record
                        # If record line date is not intended to be displayed, turn of this line also
                        # If record line date is not intended to be displayed, turn of this line also - add space so text aligns up in console
                        print(f'{str(subjecttallylist_unique_list[0])},{row[daterow].strftime("%m/%d/%y")},{str(row[stateposition])}    ,',end='')  #python 3.6 print formatter

                        # page thru each of the data columns per the defined start and total number of these
                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods):
                            # This is used to read the value at the index each period: total the time active in the column
                            lengthinstate = int(row[x])
                            # This is used to read the value at the index each period: assuming a known total, subtract to find the time not in the state - there would be multiple check under this for more defined states
                            lengthnotinstate = (periodlength-int(row[x]))
                            #Identify Active/On state by string comparison - for CPU this is ON, for User this is Active
                            #need to identify this is an active state
                            
                            for a in range(lengthinstate):
                                # print out all rows for inspection
                                # Used when formatting a non-compliant PLSim CSV file
                                print('1,',end='')
                                ontimelist.append(1)

                            for b in range(lengthnotinstate):
                                # print out all rows for inspection
                                # Used when formatting a non-compliant PLSim CSV file
                                print('0,', end='')
                                ontimelist.append(0)
                        print()


                    #If no Active state exists, add in Idle - this is to get around a no entry for Active on totally Idle days
                    # takes care of days where no idle happens and there is no entry
                    if ((('Idle') in row[stateposition]) and (row[daterow] == listeddate)):
                        # flag tripped for the counting for this with active
                        datecounted = row[daterow]

                        sys.stdout.write(str(subjecttallylist_unique_list[0]))
                        sys.stdout.write(',')
                        sys.stdout.write(row[daterow].strftime("%m/%d/%y")) # (INSERT "("%B %d, %Y")" after %d to have commas in name.  Print out the datetime for each record
                        sys.stdout.write(",") #If record line date is not intended to be displayed, turn of this line also
                        sys.stdout.write(str(row[stateposition]))
                        sys.stdout.write("  ,") #If record line date is not intended to be displayed, turn of this line also

                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            #reverse order to take care of Idle Only entries
                            lengthnotinstate = int(row[x])  # - in reference to ON state - This is used to read the value at the index each period: total the time active in the column
                            lengthinstate = (periodlength-int(row[x])) # - in reference to ON state - This is used to read the value at the index each period: assuming a known total, subtract to find the time not in the state - there would be multiple check under this for more defined states
                            #Identify Active/On state by string comparison - for CPU this is ON, for User this is Active
                            #need to identify this is an active state

                            for a in range(lengthinstate): #length in Active state in an Idle block

                                sys.stdout.write('0') #print out all rows for inspection, double negative as printing for idle but referring to active
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                directidletimelist.append(0)

                            for b in range(lengthnotinstate): #length not in Active state in an Idle block

                                sys.stdout.write('1') #print out all rows for inspection, double negative as printing for idle but referring to active
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                directidletimelist.append(1)
                                UserdirectIdledetectedday = 1

                        print()


                    #check for total Active state values
                    if ((('Active') in row[stateposition]) and (row[daterow] == listeddate) and (datecounted != row[daterow])):

                        sys.stdout.write(str(subjecttallylist_unique_list[0]))
                        sys.stdout.write(',')
                        sys.stdout.write(row[daterow].strftime("%m/%d/%y")) # (INSERT "("%B %d, %Y")" after %d to have commas in name.  Print out the datetime for each record
                        sys.stdout.write(",") #If record line date is not intended to be displayed, turn of this line also
                        sys.stdout.write(str(row[stateposition]))
                        sys.stdout.write(",") #If record line date is not intended to be displayed, turn of this line also

                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            lengthinstate = int(row[x])  #This is used to read the value at the index each period: total the time active in the column
                            lengthnotinstate = (periodlength-int(row[x])) #This is used to read the value at the index each period: assuming a known total, subtract to find the time not in the state - there would be multiple check under this for more defined states
                            #Identify Active/On state by string comparison - for CPU this is ON, for User this is Active
                                        #need to identify this is an active state
                            for a in range(lengthinstate):

                                sys.stdout.write('1') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                activetimelist.append(1)


                            for b in range(lengthnotinstate):

                                sys.stdout.write('0') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                activetimelist.append(0)


                            for posi, valzs in enumerate(directidletimelist):  #remove reported active or idle states when the computer is not on
                                if (ontimelist[posi] == 0):
                                    directidletimelist [posi] = 0
                        print()


                    if ((('Off') in row[stateposition]) and (row[daterow] == listeddate)):

                        sys.stdout.write(str(subjecttallylist_unique_list[0]))
                        sys.stdout.write(',')
                        sys.stdout.write(row[daterow].strftime("%m/%d/%y")) # (INSERT "("%B %d, %Y")" after %d to have commas in name.  Print out the datetime for each record
                        sys.stdout.write(",") #If record line date is not intended to be displayed, turn of this line also
                        sys.stdout.write(str(row[stateposition]))
                        sys.stdout.write("    ,") #If record line date is not intended to be displayed, turn of this line also - add space so text aligns up in console

                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            lengthinstate = int(row[x])  #This is used to read the value at the index each period: total the time active in the column
                            lengthnotinstate = (periodlength-int(row[x])) #This is used to read the value at the index each period: assuming a known total, subtract to find the time not in the state - there would be multiple check under this for more defined states
                            #Identify Active/On state by string comparison - for CPU this is ON, for User this is Active
                            #need to identify this is an active state
                            for a in range(lengthinstate):

                                sys.stdout.write('1') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                offtimelist.append(1)

                            for b in range(lengthnotinstate):

                                sys.stdout.write('0') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                offtimelist.append(0)
                        print()


                    if ((('Sleep') in row[stateposition]) and (row[daterow] == listeddate)):

                        sys.stdout.write(str(subjecttallylist_unique_list[0]))
                        sys.stdout.write(',')
                        sys.stdout.write(row[daterow].strftime("%m/%d/%y")) # (INSERT "("%B %d, %Y")" after %d to have commas in name.  Print out the datetime for each record
                        sys.stdout.write(",") #If record line date is not intended to be displayed, turn of this line also
                        sys.stdout.write(str(row[stateposition]))
                        sys.stdout.write("    ,") #If record line date is not intended to be displayed, turn of this line also - add space so text aligns up in console

                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            lengthinstate = int(row[x])  #This is used to read the value at the index each period: total the time active in the column
                            lengthnotinstate = (periodlength-int(row[x])) #This is used to read the value at the index each period: assuming a known total, subtract to find the time not in the state - there would be multiple check under this for more defined states
                            #Identify Active/On state by string comparison - for CPU this is ON, for User this is Active
                                        #need to identify this is an active state
                            for a in range(lengthinstate):

                                sys.stdout.write('1') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                sleeptimelist.append(1)

                            for b in range(lengthnotinstate):

                                sys.stdout.write('0') #print out all rows for inspection
                                sys.stdout.write(',') #Used when formatting a non-compliant PLSim CSV file

                                sleeptimelist.append(0)
                        print()


                    if ((('Unknown') in row[stateposition]) and (row[daterow] == listeddate) and (('CPU') in row[devicerow])): #count number of times in CPU unknown state, remove from consideration
                        CPUUnknowndetectedrow = 1 #Trigger the Flag for detection
                        CPUUnknowndetectedday = 1 #Trigger the Flag for detection for the day
                        rowtotal = 0
                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            lengthinstate = int(row[x])  #This is used to read the value at the index each period: total the time active in the column
                            rowtotal = rowtotal+lengthinstate #increase total unknown count
                        rowreviewcountUSERUnknownintercount = rowreviewcountUSERUnknownintercount-rowtotal
                        print()


                    if ((('Unknown') in row[stateposition]) and (row[daterow] == listeddate) and (('User') in row[devicerow])): #count number of times in User unknown state, remove from consideration
                        UserUnknowndetectedrow = 1 #Trigger the Flag for detection
                        UserUnknowndetectedday = 1 #Trigger the Flag for detection for the day
                        rowtotal = 0
                        for x in range(periodstartcolumn, periodstartcolumn+totalperiods): #page thru each of the data columns per the defined start and total number of these
                            lengthinstate = int(row[x])  #This is used to read the value at the index each period: total the time active in the column
                            rowtotal = rowtotal+lengthinstate #increase total unknown count
                        rowreviewcountCPUUnknownintercount = rowreviewcountCPUUnknownintercount-rowtotal
                        print()


                    if (CPUUnknowndetectedrow == 1):
                        if (rowreviewcountCPUUnknownintercount < rowreviewcountCPUUnknown):
                            rowreviewcountCPUUnknown = rowreviewcountCPUUnknownintercount
                        #print(rowreviewcountCPUUnknown)

                    if (UserUnknowndetectedrow == 1):
                        if (rowreviewcountUSERUnknownintercount < rowreviewcountUSERUnknown ):
                            rowreviewcountUSERUnknown = rowreviewcountUSERUnknownintercount
                        #print (rowreviewcountCPUUnknown)


                # If INTERPOLATION OF DATA PER DAY IS USED, Print Status of this operation in the console
                if IDLE_INTERPOLATION and UserdirectIdledetectedday == 1:
                    print(f'Option: {INTERPOLATION_SETTING}, Original Idle Array:     {directidletimelist}')
                    directidletimelist = discontinuity_solver(directidletimelist)
                    print(f'Option: {INTERPOLATION_SETTING}, Interpolated Idle Array: {directidletimelist}')


                #Daily Summary
                print()
                print("&&&&&&&&&&&&&&&&&&&") #Newline between rows - makes it formatted properly when there is final readout
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of all ON Time:, ")
                sys.stdout.write(str(sum (ontimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of Explicit OFF Times:, ")
                sys.stdout.write(str(sum (offtimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of Implicit OFF Times:, ")
                sys.stdout.write(str(minutebaseinday - sum (ontimelist) - abs(minutebaseinday-rowreviewcountCPUUnknown)-sum(sleeptimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of Sleep Times:, ")
                sys.stdout.write(str(sum (sleeptimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of Active-On Times:, ")
                sys.stdout.write(str(sum (activetimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of Idle-On Times:, ")
                sys.stdout.write(str(sum (directidletimelist)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of User Unknown Times:, ")
                sys.stdout.write(str(abs(minutebaseinday-rowreviewcountUSERUnknown)))
                print()
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily Sum of CPU Unknown Times:, ")
                sys.stdout.write(str(abs(minutebaseinday-rowreviewcountCPUUnknown)))
                print()
                sumallidleperiodsdaily = ((sum(directidletimelist))/(minutebaseinday))
                sumallidleperiodsaverage.append(sumallidleperiodsdaily)
                if(sum(ontimelist)>0):  #Avoid days with no on time
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Daily % Idle versus On:, ")
                    sumactivevsidleperiodsdaily =((sum (directidletimelist))/(sum(ontimelist)))
                    sys.stdout.write(str(sumactivevsidleperiodsdaily))
                    directidlevsonaverage.append(sumactivevsidleperiodsdaily)

                print() #Newline
                sys.stdout.write(str(subjecttallylist_unique_list[0]))
                sys.stdout.write(',')
                sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                sys.stdout.write(',')
                sys.stdout.write("Daily % Idle versus Total Day:, ")
                sys.stdout.write(str(sumallidleperiodsdaily))
                print()
                print("&&&&&&&&&&&&&&&&&&&")
                print()



                if ((sum(directidletimelist)>0) and sum(ontimelist)>0): #check to see if there is a list for the same day for both active and ON states #check to see if there is a list for the same day for both active and ON states
                    idlethisday = 1  #flag the day as a potential for idle savings
                    print("$$$$$$$$$$$$$$$$$$$$$$$$$$") #Newline between rows - makes it formatted properly when there is final readout
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    print ("**Idle time detected - Day Valid for Idle Analysis**")
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Direct Off Periods (Mask):")
                    sys.stdout.write(',')
                    for positionindex in range(0, len(offtimelist)):
                            directoffperiods.append(int(offtimelist[positionindex] == 1))  #Calculate Raw XOR State
                            sys.stdout.write(str(directoffperiods[positionindex]))
                            sys.stdout.write(',')
                    print()
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Direct Idle Time Periods (Mask):")
                    sys.stdout.write(',')
                    for positionindex in range(0, len(directidletimelist)):
                            directidleperiods.append(int(directidletimelist[positionindex] != 0))  #Calculate mask for direct measured idle times
                            sys.stdout.write(str(directidleperiods[positionindex]))
                            sys.stdout.write(',')
                    print()
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Direct Sleep Time Periods (Mask):")
                    sys.stdout.write(',')
                    for positionindex in range(0, len(sleeptimelist)):
                            directsleepperiods.append(int(sleeptimelist[positionindex] != 0))  #Calculate mask for direct measured idle times
                            sys.stdout.write(str(directsleepperiods[positionindex]))
                            sys.stdout.write(',')
                    print()
                    returnresultsdirectidleperiods = transitionsearch(directidleperiods, ontimelist, False) #read back in transition points, send in null mask array for testing
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Transition Idle Points [Date Summary]:,")
                    print(returnresultsdirectidleperiods[0], sep=", ")
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Transition Deltas (Idle)[Date Summary]:,")
                    print(returnresultsdirectidleperiods[1], sep=", ")
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Transition Deltas (Combined)[Date Summary]:,")
                    print(returnresultsdirectidleperiods[3], sep=", ")
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Total Daily Idle Periods:,")
                    print(str(len(returnresultsdirectidleperiods[1])))

                    #append final list with totals for each evaluated day
                    finaldeltalist.append(returnresultsdirectidleperiods[1]) #append to final delta list
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Transition Deltas (Not Idle)[Date Summary]:,")
                    print(returnresultsdirectidleperiods[2], sep=", ")
                    sys.stdout.write(str(subjecttallylist_unique_list[0]))
                    sys.stdout.write(',')
                    sys.stdout.write(listeddate.strftime("%m/%d/%y"))
                    sys.stdout.write(',')
                    sys.stdout.write("Daily Idle Average:,")
                    sys.stdout.write(str(sum (directidletimelist)/minutebaseinday))
                    directidleaverage.append((sum (directidletimelist))/minutebaseinday)
                    directonlyidleaverage.append((sum (directidletimelist))/minutebaseinday) #only total days where idle savings is possible
                    print()
                    print("$$$$$$$$$$$$$$$$$$$$$$$$$$")
                    print() #Newline between rows - makes it formatted properly when there is final readout
                    print() #Newline between rows - makes it formatted properly when there is final readout
                    print() #Newline between rows - makes it formatted properly when there is final readout

                else:
                    directidleaverage.append(0)  #placeholder - nothing added to sum



                resultsreviewcount+=1  #Update the day counter for analysis

            print()
            print()
            print()
            print()
            print(",,===================================================")
            print(",,Subject Run Summary Analysis:")
            sys.stdout.write(",,Delta (Idle) Summary across all days: ")
            sys.stdout.write(str(subjecttallylist_unique_list[0]))
            sys.stdout.write(',')
            sys.stdout.write("All Days")
            sys.stdout.write(',')
            sys.stdout.write("Idle Delta Summary:")
            sys.stdout.write(',')
            sys.stdout.write(str(finaldeltalist))
            print()
            sys.stdout.write(",,Total days in the Run:")
            sys.stdout.write(',')
            finaldeltalisttemp=finaldeltalist
            dayswithidle = len(finaldeltalisttemp)
            sys.stdout.write(str(resultsreviewcount))
            print()
            sys.stdout.write(",,Total days in period with Idle periods in Run:")
            sys.stdout.write(',')
            finaldeltalisttemp=finaldeltalist
            dayswithidle = len(finaldeltalisttemp)
            sys.stdout.write(str(dayswithidle))
            sys.stdout.write(',or,') #multiple metrics should result in the same value, both displayed for debugging sake
            sys.stdout.write(str(len(directidleaverage)))
            print()
            sys.stdout.write(",,Total number of Idle periods in Run:")
            sys.stdout.write(',')
            totalidleperiods = recursive_len(finaldeltalisttemp)
            sys.stdout.write(str(totalidleperiods))
            print()
            sys.stdout.write(",,Idle Periods per day for days with Idle (AVG):")
            sys.stdout.write(',')
            sys.stdout.write('{0:.2f}'.format(safe_div(totalidleperiods,dayswithidle))) #use safediv for days without idle
            print()
            sys.stdout.write(",,Idle Periods per days with Idle for all days (AVG):")
            sys.stdout.write(',')
            sys.stdout.write('{0:.2f}'.format(safe_div(totalidleperiods,resultsreviewcount)))
                        
            print()
            sys.stdout.write(",,Idle time average across days with idles present:,")
            perdayidleaverageonly = safe_div((sum(directonlyidleaverage)),(len(directonlyidleaverage)))
            sys.stdout.write('{:.2%}'.format((perdayidleaverageonly)))
            print() #newline
            sys.stdout.write(",,Idle time average across all days with idle or not:,")
            idleaveragealldays = safe_div((sum(directidleaverage)),(len(directidleaverage)))
            sys.stdout.write('{:.2%}'.format(idleaveragealldays))
            print() #newline
            sys.stdout.write(",,Idle time average across all days:,")
            idleaveragealldaystwo = safe_div(sum((sumallidleperiodsaverage)),(len(sumallidleperiodsaverage)))
            sys.stdout.write('{:.2%}'.format(idleaveragealldaystwo))
            print() #newline
            sys.stdout.write(",,Idle time average across days with idle where On State is confirmed:,")
            idleaveragealldaysthree = safe_div(sum((directidlevsonaverage)),(len(directidlevsonaverage)))
            sys.stdout.write('{:.2%}'.format(idleaveragealldaysthree))
            print() #newline
            print("Run Parameters: ")
            sys.stdout.write("Run with Day of Week Analysis Setting (All-Days[0], Weekdays[1], Weekends[2]): ")
            print(valuek)
            print(",,===================================================")


            #Run Specific Analyses with Specified Parameters
            #with no PM, no accessory control
            for i, value1 in enumerate(standbycomputerwatt):
                for j, value2 in enumerate(deltaWcomputerpower):
                    savingsreporting(finaldeltalist,sensorsettingvalues,resultsreviewcount,standbycomputerwatt[i],deltaWcomputerpower[j],0,0,False,0,False,True,False) #Arguments are: (inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdays):

            #with no PM settings, accessory control
            #approach using itertools library
            for (i,j,l) in itertools.product(range(len(standbycomputerwatt)),range(len(deltaWcomputerpower)),range(len(deltaWaccessoriespower))):
                savingsreporting(finaldeltalist, sensorsettingvalues, resultsreviewcount, standbycomputerwatt[i],
                                 deltaWcomputerpower[j], 0, deltaWaccessoriespower[l], True, 0, False, True,
                                 False)  # Arguments are: (inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdays):

            #Equiv.approach as previous without using itertools, provided as reference, this also works the same way
            #for i, value1 in enumerate(standbycomputerwatt):
            #    for j, value2 in enumerate(deltaWcomputerpower):
            #        for l, value3 in enumerate(deltaWaccessoriespower):
            #            savingsreporting(finaldeltalist,sensorsettingvalues,resultsreviewcount,standbycomputerwatt[i],deltaWcomputerpower[j],0,deltaWaccessoriespower[l],True,0,False,True,False) #Arguments are: (inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdays):


            #with PM settings, no accessory control
            for i, value1 in enumerate(standbycomputerwatt):
                for j, value2 in enumerate(deltaWcomputerpower):
                    for k, value3 in enumerate(pmSettings):
                        savingsreporting(finaldeltalist,sensorsettingvalues,resultsreviewcount,standbycomputerwatt[i],deltaWcomputerpower[j],0,0,False,pmSettings[k],True,True,False) #Arguments are: (inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdays):


            #with PM settings, accessory control
            for i, value1 in enumerate(standbycomputerwatt):
                for j, value2 in enumerate(deltaWcomputerpower):
                    for k, value3 in enumerate(pmSettings):
                            for l, value4 in enumerate(deltaWaccessoriespower):
                                    savingsreporting(finaldeltalist,sensorsettingvalues,resultsreviewcount,standbycomputerwatt[i],deltaWcomputerpower[j],0,deltaWaccessoriespower[l],True,pmSettings[k],True,True,False) #Arguments are: (inputrange, analysisvalues, averagebase, standbycomputerwatt, activecomputerwatt, standbyaccessorieswatt, activeaccessorieswatt, accessorycontrol, simPMsavingsval, simPMsavingsOn, energyreport, breakdays):


            print("Single Subject - Analysis Run Complete")
        print()
        print("Run Segment Complete")
        print()
    print("Full Run Complete")

    cursor.close()
    db.close()  #close DB connection