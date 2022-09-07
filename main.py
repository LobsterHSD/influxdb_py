from influxdb_client import InfluxDBClient, Dialect
import csv, os, datetime, glob, tarfile, sys
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

if not os.path.exists("client_secrets.json"):
    while True:
        userChoice = input("client_secrets.json not found. No GDrive upload will occur! Would you like to continue anyway? (y/n) ")
        if userChoice != "y" and userChoice != "n":
            print("Invalid input. Please enter y or n.")
            continue
        if userChoice == "y":
            print("Continuing without Drive upload...")
            break
        else:
            sys.exit("Exiting...")

for fname in os.listdir("output"):
    if fname.endswith(".csv"):
        sys.exit("Please remove all CSV files from output folder before running this script.")

for fname in os.listdir("output"):
    if fname.endswith(str(datetime.date.today().year) + "_" + str(datetime.date.today().month).zfill(2) + "_" + str(datetime.date.today().day).zfill(2) + ".tar.gz"):
        sys.exit("tar.gz file for today already exists in output folder. Please remove before running this script.")

client = InfluxDBClient.from_config_file("config.ini")

query_api = client.query_api()

# search directory for influx export files
mypath = os.getcwd() + "/output" # <-- change this!
files = os.listdir(mypath)
exportList = []
for word in files:
    year = ""
    month = ""
    day = ""
    if (word[0:7] == 'lobster' and word[-1:-7:-1][::-1] == 'tar.gz'):
        for i in range(31, len(word)):
            if (i>=31 and i<=34):
                year += word[i]
            if (i>=36 and i<=37):
                month += word[i]
            if (i>=39 and i<=40):
                day += word[i]
        exportList.append([year,month,day])

# convert to datetime.date format
LogDates = []
for date in exportList:
    intDate = [int(x) for x in date] # convert current date to list of integers
    LogDate = datetime.date(intDate[0], intDate[1], intDate[2])    
    LogDates.append(LogDate)

# get time delta between today and latest log
today = datetime.date.today()
latestLogDate = max(LogDates) # find latest log date
latestLogDate1 = latestLogDate + datetime.timedelta(days=1)
deltaTime = (today-latestLogDate)

# gzip filename
comp_filename = "lobster_influx_" + str(latestLogDate1.year) + "_" + str(latestLogDate1.month).zfill(2) + "_" + str(latestLogDate1.day).zfill(2) + "_TILL_" + str(today.year) + "_" + str(today.month).zfill(2) + "_" + str(today.day).zfill(2) + ".tar.gz"

# fill a list with the start/stop strings for the querys
queryList = []
if deltaTime.days > 0:
    for i in range(deltaTime.days):
        date = latestLogDate + datetime.timedelta(days=i+1)
        query_start = "start: " + str(date.year) + "-" + str(date.month).zfill(2) + "-" + str(date.day).zfill(2) + "T00:00:00Z"
        query_stop = "stop: " + str(date.year) + "-" + str(date.month).zfill(2) + "-" + str(date.day).zfill(2) + "T23:59:59Z"
        queryList.append(query_start + ", " + query_stop)
    print(queryList)
    print(comp_filename)



# using csv library with queryList
for query in queryList:
    flux_query = 'from(bucket:"cifxdata") |> range(' + query + ')'
    csv_result = query_api.query_csv(flux_query, dialect=Dialect(header=True, delimiter=',', annotations=[], date_time_format="RFC3339"))
    with open("output/" + query[7:17] + ".csv", "w", newline='') as csv_file:
        writer = csv.writer(csv_file, dialect='unix')
        writer.writerows(csv_result)

client.close()

csvlist = glob.glob("output/*.csv")
print(csvlist)

with tarfile.open(os.getcwd() + "/output/" + comp_filename, "w:gz") as tar:
    for file in csvlist:
        tar.add(file)
    tar.close()

for file in csvlist:
    os.remove(file)

# Upload to Google Drive
if os.path.exists("client_secrets.json"):
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile("lobster_creds.txt")
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile("lobster_creds.txt")
    # create GoogleDrive instance
    drive = GoogleDrive(gauth)
    # upload file to Lobster Folder
    drivefile = drive.CreateFile({'title': comp_filename,'parents': [{'id': '1SCQWiExyhl0Qn8kgxEyTVIYitIMjXwv-'}]})
    drivefile.SetContentFile(os.getcwd() + "/output/" + comp_filename)
    drivefile.Upload()
else:
    print("client_secrets.json not found. Please get it from Google Dev Console Website. For automated drive upload.")

#csv_result = query_api.query_csv('from(bucket:"cifxdata") |> range(start: 2022-08-22T00:00:00Z, stop: 2022-08-22T23:59:59Z)', dialect=Dialect(header=True, delimiter=',', annotations=[], date_time_format="RFC3339"))

""" val_count = 0
for row in csv_result:
    #print(row)
    for cell in row:
        val_count += 1
print(val_count) """


# with open('output.csv', 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile, delimiter=',', dialect='unix')
#     writer.writerows(csv_result)
    
# csvfile.close()
# client.close()