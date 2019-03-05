#Read the mySQL table summary results for power analysis and print contents of the .CSV file summary to the console
#California Plug Load Research Center, 2018
#Michael Klopfer, Ph.D, & Prof. G.P. Li with coding support by Liangze Yu and Zihan "Bronco" Chen
#Ver 2.2 - first usable release in current format -  12/18/18
#Ver 2.7 - Minor updates for usability (1/29/2019)



#Library setup:
import sys
from datetime import timedelta, datetime, date, time
from time import mktime
import mysql.connector

#return index in the DB return (column for this value) for Output DB:
idlepercent = 0 
idlepercentSTDEV = 1 
totaldayidlepercent = 2
totaldayidlepercentSTDEV = 3
runtimesavedperday = 4  #min savings per day
runtimesavedperdaySTDEV = 5  #stdev of min savings per day
runtime_eval_period_saved_hr_per_yearAVG = 6
runtime_eval_period_saved_hr_per_yearSTDEV = 7
perdaysavings = 8
perdaysavingsSTDEV = 9
totaldays = 10
totaldaysSTDEV = 11
returncount = 12


#Functions
def query_updatevalue(dictionaryname, key_to_find, definition):  #used to update a sample SQL query with a new value - easier than parsing strings manually
    #remember, if a key is missed, this will skip, be careful!!
    for index, key in enumerate(dictionaryname.keys()):
        if(len(key_to_find) == len(definition)): #check same number of entries to valid process
            if (index<len(key_to_find)):  #prevent an out of range error if not all key vaues are replaced (for some reason)
                if key == key_to_find[index]: #fix the index versus element issue with start as 0 vs 1
                    dictionaryname[key] = definition[index]
                    #print (index)  #--Debug printout
                    #print (key)    #--Debug printout
    return [dictionaryname] #returns as array, you only need/want the first [0] element, make sure this is called on the function return!



#************************************************
#Program Operation

#Open Database connection
db = mysql.connector.connect(host="XXXXXXXXXX.calit2.uci.edu",    # host
                     user="XXXXXXXXXXX",         # username
                     passwd="XXXXXXXXXXX",  # password
                     db="XXXXXXXXXXXX")        # DBName

cursor = db.cursor() # Cursor object for database query

#Queries against the summary and summary 2 Database tables
 
##example SELECT AVG(`summary_idle_percent`), AVG(`summary_total_time_idle_percent`), AVG(`runtime_saved_per_day_min`), AVG(`projected_per_day_savings_kWh`), AVG(`total_days_estimate_kwhperyear_kWh`), count(*) FROM summary6 WHERE (delta_computer_W = 20 AND delta_acessories_W = 0 AND external_pm_control_min = 0 AND invervention_setting_min = 10 AND reporting_type = 'Weekdays') 
#eliminate records where the idlepercent=0, these are ones with no valid entries as all days are averaged
#query_1 = ("SELECT AVG(`summary_idle_percent`), STDDEV(`summary_idle_percent`), AVG(`summary_total_time_idle_percent`), STDDEV(`summary_total_time_idle_percent`), AVG(`runtime_saved_per_day_min`), STDDEV(`runtime_saved_per_day_min`), AVG(`projected_per_day_savings_kWh`), STDDEV(`projected_per_day_savings_kWh`), AVG(`total_days_estimate_kwhperyear_kWh`), STDDEV(`total_days_estimate_kwhperyear_kWh`), count(*) FROM summary10 WHERE (delta_computer_W = %(delta_computer_W)s AND delta_acessories_W = %(delta_acessories_W)s AND external_pm_control_min = %(external_pm_control_min)s AND invervention_setting_min = %(invervention_setting_min)s AND reporting_type = %(reporting_type)s)")
#NOTE, for Floating point searches, use LIKE versus equals per this example:
#for MAC subset Only:  query_1 = ("SELECT AVG(`summary_idle_percent`), STDDEV(`summary_idle_percent`), AVG(`summary_total_time_idle_percent`), STDDEV(`summary_total_time_idle_percent`), AVG(`runtime_saved_per_day_min`), STDDEV(`runtime_saved_per_day_min`), AVG(`projected_per_day_savings_kWh`), STDDEV(`projected_per_day_savings_kWh`), AVG(`total_days_estimate_kwhperyear_kWh`), STDDEV(`total_days_estimate_kwhperyear_kWh`), count(*) FROM summary8 WHERE (delta_computer_W = %(delta_computer_W)s AND delta_acessories_W = %(delta_acessories_W)s AND external_pm_control_min = %(external_pm_control_min)s AND invervention_setting_min = %(invervention_setting_min)s AND reporting_type = %(reporting_type)s) AND desktop_type ='MAC'")
#for PC subset Only:  query_1 = ("SELECT AVG(`summary_idle_percent`), STDDEV(`summary_idle_percent`), AVG(`summary_total_time_idle_percent`), STDDEV(`summary_total_time_idle_percent`), AVG(`runtime_saved_per_day_min`), STDDEV(`runtime_saved_per_day_min`), AVG(`projected_per_day_savings_kWh`), STDDEV(`projected_per_day_savings_kWh`), AVG(`total_days_estimate_kwhperyear_kWh`), STDDEV(`total_days_estimate_kwhperyear_kWh`), count(*) FROM summary8 WHERE (delta_computer_W = %(delta_computer_W)s AND delta_acessories_W = %(delta_acessories_W)s AND external_pm_control_min = %(external_pm_control_min)s AND invervention_setting_min = %(invervention_setting_min)s AND reporting_type = %(reporting_type)s) AND desktop_type ='PC'")

#To change database table to run summary tool on, modify with template query in the line below in addition to any query modifications in the notes above:
query_1 = ("SELECT AVG(`summary_idle_percent`), STDDEV(`summary_idle_percent`), AVG(`summary_total_time_idle_percent`), STDDEV(`summary_total_time_idle_percent`), AVG(`runtime_saved_per_day_min`), STDDEV(`runtime_saved_per_day_min`), AVG(`runtime_eval_period_saved_hr_per_year`), STDDEV(`runtime_eval_period_saved_hr_per_year`), AVG(`projected_per_day_savings_kWh`), STDDEV(`projected_per_day_savings_kWh`), AVG(`total_days_estimate_kwhperyear_kWh`), STDDEV(`total_days_estimate_kwhperyear_kWh`), count(*) FROM summaryNoPMOPT2 WHERE (delta_computer_W LIKE %(delta_computer_W)s AND delta_acessories_W LIKE %(delta_acessories_W)s AND external_pm_control_min = %(external_pm_control_min)s AND invervention_setting_min = %(invervention_setting_min)s AND reporting_type = %(reporting_type)s) AND desktop_type ='PC'")
#In the query, return averages and std.devs. for all specified records in the return


#Default Query for replacing elements of return with dictionary - change subject number to view other subjects
query_modifications_1 = {'delta_computer_W': 1,'delta_acessories_W': 0,'external_pm_control_min': 0, 'invervention_setting_min': 5, 'reporting_type': "Weekdays"} #query records default case


#values to use for the summary evaluation report - match these values to the ones in your summarizer to output all data - All machines that match query are analyzed.  Only parameters in the sim placed here are outputted in the summary.  Reducing these simplifies the output cases presented.  Remember that "NONE"'s will be presented if values are put in here that are not present in your input table!  This is just a filter, it will not create new values!
delta_acessories_W_report_vals = [0, 5, 10, 20, 50, 100, 500]
#delta_acessories_W_report_vals = [0, 1.46, 1, 5, 10]
#examples of other subsets:
#delta_acessories_W_report_vals = [0, 5, 10, 20, 50, 100, 500] 
#or 
#delta_acessories_W_report_vals =[20, 30, 40, 50, 60, 80, 100, 120, 150]

delta_computer_W_report_vals = [0.000000001, 20, 50, 100]
#delta_computer_W_report_vals = [0.000000001, 20, 50, 100] #As in the simulation, use 0.000000001 as the 0 W power use case to avoid div by 0 errors, this case can be used to assess power contribution by load controls by removing the computer's contribution
#examples of other subsets:
#delta_computer_W_report_vals = [20, 30, 40, 50, 60, 80, 100, 120, 150]


external_pm_control_min_report_vals = [0,5,10,15,30,40,60,120,180,240,300] #Specifically include 0 in this list, this is not required by the simulator, but required here if this summary value is requested
#examples of other subsets:
#external_pm_control_min_report_vals = [0, 5, 10, 15, 20, 30, 45, 60, 120]

invervention_setting_min_report_vals = [5,10,15,20,25,30,35,40,45,50,55,60,120,180,240,300] #There typically is no 0 value for intervention - this delta value makes no sense to report
reporting_type_setting = ["AllDays","Weekdays","Weekends"] #Show for all days as well as the contribution of weekdays and weekends per year, this is noted in the SIM as 0, 1, 2 (which correspond to ["AllDays","Weekdays","Weekends"] )
#reporting_type_setting = ["AllDays"] #Example to omit weekends and weekday results

#FYI (not used in calculation directly) -  Provided for reference for the 2014 Monitoring study - Computers with PM enabled and the values for the PM timer settings for computer standby:
#subjectlist = [1, 2, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 44, 45, 47,  49, 50, 52, 53, 54, 55, 56, 57, 58, 59, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 83, 84, 86, 87, 88, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 101, 102, 103, 104, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116]
#ComputerswithoutPM = [1, 2, 4, 5, 6 ,7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 84, 85, 87, 88, 89, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116]
#ComputerswithPM = [3, 10, 21, 29, 30, 35, 43, 46, 48, 51, 60, 82, 85, 89, 100, 105]
#ComputerPMValues= [60, 10, 30, 180, 240, 30, 10, 120, 25, 10, 180, 10, 60, 30, 30, 60] 

#Print CSV Headers:
print("Controlled_devices(W),Scope(reporting_type),PM_Setting(min)[or Wildtype (for 0)],ComputerDeltaPower(W),Intervention_Setting (min),AVG_Idle_Percent,STDEV_Idle_Percent,AVG_total_day_idle_percent,STDEV_total_day_idle_)percent,AVG_runtimesavedperday[min],STDEV_runtimesavedperday[min],AVG_TotalYearHrsSaved[yearlyhoursforscope],STDEV_TotalYearHrsSaved[yearlyhoursforscope],AVG_dayEstimateKWh[dailykWh],STDEV_dayEstimateKWh[dailykWh],AVG_totaldaysEstimateKWh[yearlykWhforScope],STDEV_totaldaysEstimateKWh[yearlykWhforScope],TotalRecordsProcessed")

#Page thru parameters to extract summary for and print into a CSV style format in the console
for value0 in reporting_type_setting:
    for value1 in delta_acessories_W_report_vals:
        if (value1 == 0): #Case where no devices are controlled in the simulation
            print("No Controlled Devices")
        else:
            sys.stdout.write("Controlled Devices (W): ")
            print(value1)
        for value2 in delta_computer_W_report_vals:
            for value3 in external_pm_control_min_report_vals:
                for value4 in invervention_setting_min_report_vals:
                    ## Modify each of the queries
                    #Identify the run type, what is the scope of the averages returned
                    sys.stdout.write(str(value1))
                    sys.stdout.write(",") #separator
                    sys.stdout.write(str(value0))
                    sys.stdout.write(",") #separator
                    #update query with run values
                    updated_querymodifications_1 = query_updatevalue(query_modifications_1,['delta_computer_W', 'delta_acessories_W','external_pm_control_min','invervention_setting_min', 'reporting_type' ],[value2, value1, value3, value4, value0])  #present values in the order to match the keys
                     #Debug for query:
                    #sys.stdout.write("Query 1, post modification: ") # - Use in debug
                    #print(updated_querymodifications_1[0]) # - Use in debug, returns as array, you only want the first element.

                    #Denote separately if the PM setting is not modified (no PM applied over computer settings) - Denote Wildtype in output record to highlight this case
                    if (value3 == 0):
                        sys.stdout.write("Wildtype") #Case where the original PM settings are used (wildtype/O case)
                    else:
                        sys.stdout.write(str(value3)) #if not a wildtype, note the PM setting in use
                    
                    sys.stdout.write(",") #separator
                    sys.stdout.write(str(value2))
                    sys.stdout.write(",") #separator
                    sys.stdout.write(str(value4))
                    sys.stdout.write(",") #separator
                    
                    #sys.stdout.write("******") #Debug test separator
                    #put the parts together and query the DB:
                    #process each query:
                    cursor.execute(query_1, updated_querymodifications_1[0]) #Process query with variable modifications
                    queryreturn_1 = cursor.fetchone() #Fetch only one row with defined query for first state
                   
                
                    #write out the return values - Query 1 (Averages)
                    #calculated average value from the return
                    sys.stdout.write(str(queryreturn_1[idlepercent]))
                    sys.stdout.write(",") #separator
                    
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[idlepercentSTDEV]))
                    sys.stdout.write(",") #separator
                    
                    #calculated average value from the return
                    sys.stdout.write(str(queryreturn_1[totaldayidlepercent]))
                    sys.stdout.write(",") #separator
                    
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[totaldayidlepercentSTDEV]))
                    sys.stdout.write(",") #separator
                    
                    #calculated average value from the return
                    sys.stdout.write(str(queryreturn_1[runtimesavedperday]))
                    sys.stdout.write(",") #separator
                    
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[runtimesavedperdaySTDEV]))
                    sys.stdout.write(",") #separator
                    
                    #calculated average value from the return
                    sys.stdout.write(str(queryreturn_1[runtime_eval_period_saved_hr_per_yearAVG]))
                    sys.stdout.write(",") #separator
                    
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[runtime_eval_period_saved_hr_per_yearSTDEV]))
                    sys.stdout.write(",") #separator
                    
                    sys.stdout.write(str(queryreturn_1[perdaysavings]))
                    sys.stdout.write(",") #separator
                    
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[perdaysavingsSTDEV]))
                    sys.stdout.write(",") #separator

                    #calculated average value from the return - yearly scoped component saved
                    sys.stdout.write(str(queryreturn_1[totaldays])) #total days average runtime yearly AVG for scoped period in evaluation
                    sys.stdout.write(",") #separator
                    #Calculated STDEV value from the return
                    sys.stdout.write(str(queryreturn_1[totaldaysSTDEV]))  #total days average runtime yearly STDEV for scoped period in evaluation

                    #sys.stdout.write(";") #separator (a semicolon is common, use a comma to easily import into excel, behind the semicolon is diag data to pull
                    sys.stdout.write(",") # temporary separator, typically use semicolon as this is not used for analysis just data quality to make sure the number of records averaged is what was expected.  The ":" denotes this as a non analysis record, but if importing to Excel, just use a ",", then delete the column, it's easier.
                    
                    #Write out total records/subjects - Used in debugging to verify all expected records were totaled
                    sys.stdout.write(str(queryreturn_1[returncount]))
            
                    print("") #send newline with each full return set
        
cursor.close()
db.close()  #close DB connection
print()
print("Analysis Summary Operation Complete! - Delete this and other extraneous lines in Output CSV File before Processing!")


  