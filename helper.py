import csv, os, json, math

def default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

def writeCsvFile(filename, dataDict, header = []):
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, 'wb') as f:
        w = csv.writer(f)
        if len(header) > 0:
            w.writerow(header)
        w.writerows(dataDict.items())

def writeJsonFile(fileName, content):
    with open(fileName, 'w') as fp:
        json.dump(content, fp,  indent=4)

def convertBytesSize(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])
