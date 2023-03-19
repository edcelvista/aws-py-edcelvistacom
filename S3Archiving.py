import boto3
from botocore.exceptions import ClientError
import os, argparse, time, math, pathlib

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Archive Files from Source Local File System and Upload to S3 | $python3 S3Archiving.py --directory tmp --s3bucket edcelvistadotcom --awsprofile default --agefilter 1 --extension csv --prefix file --dryrun False --ispurge True")
parser.add_argument("-d",
                    "--directory",
                    help="Source Directory.",
                    required=True)
parser.add_argument("-b",
                    "--s3bucket",
                    help="Target S3 Bucket.",
                    required=True)
parser.add_argument("-s",
                    "--awsprofile",
                    help="AWS Profile.",
                    required=False)
parser.add_argument("-p",
                    "--prefix",
                    help="File prefixes in Directories.",
                    required=False)
parser.add_argument("-e",
                    "--extension",
                    help="File extension in Directories.",
                    required=False)
parser.add_argument("-a",
                    "--agefilter",
                    help="File age in Directories.",
                    required=False)
parser.add_argument("-x",
                    "--ispurge",
                    help="If Purge after upload.",
                    required=False)
parser.add_argument("-r",
                    "--dryrun",
                    help="Dry Run no upload or purge.",
                    required=False)

# Read the arguments from the command line
args         = parser.parse_args()
directory    = args.directory
s3bucket     = args.s3bucket
awsprofile   = "default" if args.awsprofile == None else args.awsprofile
prefix       = "" if args.prefix == None else args.prefix
extension    = "" if args.extension == None else args.extension
agefilter    = "" if args.agefilter == None else args.agefilter
ispurge      = "False" if args.ispurge == None else args.ispurge
dryrun       = "True" if args.dryrun == None else args.dryrun

def contextSettings(args):
    print(args)

def archiveFile(S3Client, fileObj, s3bucket, dryrun, daysAge):
    try:
        if dryrun == "False":
            S3Client.upload_file(fileObj, s3bucket, "EFS-ARCHIVE{}".format(fileObj))
        print("Archived: {} - age({})".format("EFS-ARCHIVE{}".format(fileObj),daysAge))
    except ClientError as e:
        print(e)
        print("S3 PUT - An exception occurred...")

def filterPrefix(fileAttr, prefix):
    if fileAttr['entryName'].startswith(prefix):
        return True
    else:
        return False

def filterExtension(fileAttr, extension):
    if fileAttr['entryName'].endswith(extension):
        return True
    else:
        return False

def filterAge(fileAttr, agefilter):
    if int(agefilter) <= fileAttr['daysAge']:
        return True
    else:
        return False

def scanDirectory(directory, prefix, extension, agefilter):
    path = os.path.abspath(directory)
    scan = os.scandir(path)
    print("Files at '% s':" % path)

    session  = boto3.Session(profile_name="{}".format(awsprofile))
    S3Client = session.client('s3')

    for entry in scan:
        if not entry.name.startswith(".") and entry.is_file(): ## file only scans ##

            absFilePath = "{}/{}".format(path, entry.name)
            daysAge     = math.floor((time.time() - os.stat(absFilePath).st_mtime) / 86400)
            fileAttr    = {"entryName": entry.name, "file": absFilePath,"stat": os.stat(absFilePath).st_mtime,"now": time.time(),"daysAge": daysAge, "ext": pathlib.Path(absFilePath).suffix}

            if prefix:
                if filterPrefix(fileAttr, prefix) == False:
                    continue
                
            if extension:
                if filterExtension(fileAttr, extension) == False:
                    continue

            if agefilter:
                if filterAge(fileAttr, agefilter) == False:
                    continue

            archiveFile(S3Client, absFilePath, s3bucket, dryrun, fileAttr['daysAge'])

            if dryrun == "False":
                if ispurge == "True":
                    os.remove(absFilePath)
    scan.close()

## ITERATE DIRECTORY OBJ ##
contextSettings(args)
scanDirectory(directory, prefix, extension, agefilter)

if dryrun != "False":
    print("Dry run... No Action was taken...")