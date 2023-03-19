import boto3
import os, argparse, time, math
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract Files in Specified Directory and prefix")
parser.add_argument("-d",
                    "--directory",
                    help="Target Directory.",
                    required=True)
parser.add_argument("-p",
                    "--prefix",
                    help="File prefixes in Directories.",
                    required=False)
parser.add_argument("-a",
                    "--agefilter",
                    help="Filter file age | e.g. part of the metadata of the object.",
                    required=False)

# Read the arguments from the command line
args         = parser.parse_args()
directory    = args.directory
prefix       = "" if args.prefix == None else args.prefix
agefilter    = "" if args.agefilter == None else args.agefilter

def contextSettings(args):
    print(args)

def archiveFile(fileObj, fileAttr):
    print(fileAttr)

def scanDirectory(directory, prefix, agefilter):
    path = os.path.abspath(directory)
    scan = os.scandir(path)
    print("Files/Directories at '% s':" % path)
    for entry in scan:
        if not entry.name.startswith('.') and entry.is_file(): ## set file only scans ##
            absFilePath = "{}/{}".format(path, entry.name)
            daysAge     = math.floor((time.time() - os.stat(absFilePath).st_mtime) / 86400)
            fileAttr    = {"file": absFilePath,"stat": os.stat(absFilePath).st_mtime,"now": time.time(),"daysAge": daysAge}
            
            if int(agefilter) <= daysAge: ## age filter
                archiveFile(absFilePath, fileAttr)
              
    scan.close()

## ITERATE DIRECTORY OBJ ##
contextSettings(args)
scanDirectory(directory, prefix, agefilter)