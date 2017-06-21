#######################################################################
#######################################################################
###################     Code developed by:     ########################
###################       Unai Saralegui       ########################
#######################################################################
#######################################################################
#########         Scraping data from timeanddate.com          #########
####    In this script historic data about the weather is scraped  ####
####    from timeanddate.com. The data scraped are the             ####
####    "Date","Temperature (C)","Weather","Wind (km/h)",          ####
####    "Humidity (%)","Barometer (mbar)","Visibility (km)" values ####
####    for the years 2015 and 2016 in Berkeley, USA.              ####
#######################################################################
#######################################################################

from bs4 import BeautifulSoup
import webkit_server
import pandas as pd
import numpy as np
import re

def getDate(kk):
    aa = re.split(pattern='[A-z]+', string=kk, maxsplit=1)
    aa[1] = aa[1].split(maxsplit=1)[1]
    return aa

def html2DF(soup):
    """This function receives a BeautifulSoap object and returns a pandas data frame 
    with a table containing dates and different values by web scraping"""
    
    #the values in the tables are defined by td
    temp = soup.find_all('td')
    kk=[] #a list to save each of the rows with td
    for i in temp:
        kk.append(i.text) #we just save the text
    kk.pop() #don't need the last value, and it can be problematic to handle so we erase it
    kk.append('') #add an empty character string to perform for iterations
    byrow=list() #a new list for saving only the desired values
    ini=list() #initialize a list that will be appended to byrow
    for i in range(len(kk)):
        if (kk[i]==''):
            #when we find an empty character string append the current ini and start a new one
            byrow.append(ini)
            ini=list()
        else:
            #append current value to ini
            ini.append(kk[i])
    
    #the first item in byrow is from a table we don't need so we remove it
    byrow=byrow[1:len(byrow)]
    
    #we now remove items that do not fit with the required size
    torem=list()
    for i in range(len(byrow)):
        if len(byrow[i])!=7: #the table has 7 elements
            torem.append(i)
    for i in torem:
        #remove items that don't fullfil the requirements
        del byrow[i]
    #the values in the rows are not numeric and some have some characters we do not need
    #so we process the strings to get the desired result
    for i in range(len(byrow)):
        test=byrow[i]
        temper=int(test[0].split('\xa0°C')[0])
        weather=test[1]
        if test[2]=="No wind":
            wind=0
        elif test[2]=="N/A":
            wind="NA"
        else:
            wind=int(test[2].split()[0])
        if test[4]=="N/A":
            hum="NA"
        else:
            hum=int(test[4].split('%')[0])
        barom=int(test[5].split()[0])
        visib=int(test[6].split('\xa0km')[0])
        byrow[i] = [temper,weather,wind,hum,barom,visib]
    
    #we define the header for the data frame by hand 
    header=["Date","Temperature (C)","Weather","Wind (km/h)","Humidity (%)","Barometer (mbar)","Visibility (km)"]
    
    #we now pass to find the time of each of the measurements, which are in a th object
    temp = soup.find_all('th')
    #the first 21 elements are always from other values so we do not consider them
    temp=temp[21:len(temp)]

    #we create an empty list for saving the hours
    times=list()
    for i in temp:
        times.append(i.text)
    #the first element is a bit different as t contains the hour and the date
    #so we call getDate to take tha time and the date
    date = getDate(times[0])
    times[0]=date[0] #hour
    date=date[1] #day
    date=date.split()  #the day will be something like '21 Aug' or '21 de ago' if it is in spanish
    #so we split it to get the values
    if len(date)==3:   #Depending if the page is opened in english or spanish its different
        #in english it will be something like [1,'Aug'] and in spanish [1,'de','ago']
        #so in the case it is in spanish we need to remove 'de'
        del date[1]
    
    #declare number corresponding to month in both english and spanish
    months = {'ene':1 , 'feb':2 , 'mar':3 , 'abr':4 , 'may':5, 'jun':6, 'jul':7 ,'ago':8,'sep':9,'oct':10,'nov':11,'dic':12,
             'Jan':1 , 'Feb':2 , 'Mar':3 , 'Apr':4 , 'May':5, 'Jun':6, 'Jul':7 ,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    date[1]=str(months[date[1]]) #pass month name to number
    
    #we now pass to receive the year of the data
    #which can be found in a h2 object
    temp = soup.find_all('h2')
    yy=temp[0].text.split()[1] #obtain the year
    date.append(yy) #now date is [day,month,year]
    #reorder date to obtain [year,month,day] to use join and obtain YYYY-MM-DD
    date[0],date[2]=date[2],date[0]
    fec="-".join(date) #YYYY-MM-DD

    #we now merge the date with the hours to obtain 'YYYY-MM-DD hh:mm:ss'
    for i in range(len(times)):
        times[i]=fec+" "+times[i]

    #all the data has been obtained, now we create a pandas data frame to save all such data
    df = pd.DataFrame(np.random.randn(len(times), (len(header))),columns=header)
    df['Date']=times
    df[header[1:len(header)]]=byrow
    return df





a=webkit_server.Client()


header="https://www.timeanddate.com/weather/usa/berkeley/historic?"
dfall = pd.DataFrame(columns=["Date","Temperature (C)","Weather","Wind (km/h)","Humidity (%)",
    "Barometer (mbar)","Visibility (km)"])

try:
    for year in [2015,2016]: #years 2015 and 2016
        print(year)
        for month in range(1,13):  #all the months in the year
            print(month)
            url=[header,"month=",str(month),"&year=",str(year)]
            URL="".join(url) # this URL is valid for month in year
            a.visit(URL)
            #once the month page is opened we can made the requests
            response=a.body()
            soup = BeautifulSoup(response,"html.parser")
            #we first obtain the javascript commands we have to run
            temp = soup.find_all('a',rel="nofollow") #commands to run to obtain data for different days
            for i in temp:
                a.eval_script(str(i).split("return ")[1].split("\"")[0]) #execute the command
                #once the command is executed a False value will be returned and the new data will be charged in the page
                response=a.body() #get the new body of the html code
                soup = BeautifulSoup(response,"html.parser") #initialize the BeautifulSoup object
                df1=html2DF(soup)  ##obtain the data frame for year and month
                dfall=pd.concat([dfall,df1]) #concat the new values with the previous ones
                
                
    #Once the requests have ended save the resulting data frame to a csv file
    print("Data obtained, saving...")
    dfall.to_csv("scrapedData.csv",index=False)
    
except:
    #Once the requests have ended save the resulting data frame to a csv file
    print("An error happened, saving the obtained data...")
    dfall.to_csv("scrapedData.csv",index=False)