#!/usr/local/bin/python2.7
# encoding: utf-8
'''
 -- Converts FAIMS exported XML data into a format that can be ingested by Alveo


@author:     Michael Bauer

'''

import sys
import os, time
import json, csv
import pyalveo
import subprocess
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from lib2to3.pgen2.token import STAR

DEBUG = 1
TESTRUN = 0
PROFILE = 0

BASE_URL = 'https://staging.alveo.edu.au/'
DOWNSAMPLED_FORMAT = 'wav' #This is just default: Can also be 'mp3'
DOWNSAMPLED_BITRATE = '16000'
DOWNSAMPLED_SAMPLERATE = '16000'

DELETE = False

program_name = "FAIMS to Alveo Converter"

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def read_csv(input_dir):
    ''' Reads the primary exported .csv file and returns the results as a dict '''
    res = []
    with open(os.path.join(input_dir,'Entity-Interview.csv'),'r') as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            res.append(row)
    for i in res:
        print i
    return res

def process_data(input_dir=None,apiKey=None,collection=None, verbose=False,
                 skip_downsampled=False, include_backup=False, dont_keep_one=False):
    
    
    def add_document(relpath,fname,displaydoc=False,skip_downsampled=skip_downsampled):
        start_doc_time = time.time()
        path = os.path.join(input_dir,relpath)
        spl = os.path.basename(path).split('.')
        docid = None
        if skip_downsampled:
            name = spl[0]+"_"+fname+"."+spl[1]
            docid = client.add_document(item_uri, name, {"@id":name}, file=path, fileNameIsName=True)
        else:
            start_down_time = time.time()
            print("Starting Audio Downsample...")
            name = spl[0]+'_'+fname+"_downsampled."+DOWNSAMPLED_FORMAT
            if downsampleAudio(path, resultName=name, verbose=verbose):
                print("Finished Audio Downsample... "+str(int(time.time()-start_down_time))+"s")
                downsampledPath = path[:path.rfind(os.sep)]+os.sep+name
                docid = client.add_document(item_uri, name, {"@id":name}, file=downsampledPath,displaydoc=displaydoc, fileNameIsName=True)
            else:
                print("Failed to Downsample the Audio File... "+str(int(time.time()-start_down_time))+"s")
                return None
        print("Added "+fname+" File: "+docid+"  Processing Time: "+str(int(time.time()-start_doc_time))+"s")
        return docid
    
    client = pyalveo.Client(api_url=BASE_URL,api_key=apiKey,verifySSL=False)
    
    client.add_context("faims", "http://fedarch.org/schema/")
    # TODO: check to see if we're able to modify the given collection (and that it exists)
    
    collection_uri = BASE_URL+'catalog/'+collection
    speaker_data = read_csv(input_dir) #If we change the source (ie: xml, json, sqlite) make a new function
    
    if verbose:
        print("Found "+str(len(speaker_data))+" Speakers.")
    
    
    for speaker in speaker_data:
        try:
            start_time = time.time()
            if DELETE:
                r = client.delete_speaker("https://staging.alveo.edu.au/speakers/faims-test/"+speaker.get('uuid',''))
                print("Deleting Speaker - Result: "+str(r))
            if verbose:
                print("Handling Speaker: "+speaker.get('identifier','None'));
            
            #Add faims context in front of each element
            speaker_meta = {'faims:%s'% name[0].lower() + name[1:]:val for name,val in speaker.iteritems()}
            
            speaker_meta['dcterms:identifier'] = speaker_meta.get('faims:uuid','')
            speaker_meta['dcterms:created'] = speaker_meta.get('faims:createdAtGMT',None)[:10]
            
            speaker_meta['foaf:name'] = speaker_meta.get('faims:firstNameOfInterviewee','') + ' ' + speaker_meta.get('faims:lastNameOfInterviewee','')
            speaker_meta['foaf:gender'] = speaker_meta.get('faims:gender',None)
            speaker_meta['austalk:father_pob_town'] = speaker_meta.get('faims:whereWasYourFatherBornVillage','')
            speaker_meta['austalk:father_pob_state'] = speaker_meta.get('faims:whereWasYourFatherBornRegion','')
            
            speaker_meta.pop("faims:timeWhenInterviewEnded",None)
            speaker_meta.pop("faims:zoomH2nFiles",None)
            speaker_meta.pop("faims:zoomH6ExternalMic",None)
            speaker_meta.pop("faims:zoomH6PrimaryMic",None)
            speaker_meta.pop("faims:photoOfSignedConsentForm",None)
            
            #Remove metadata fields that have no data
            rem = []
            
            for i in speaker_meta:
                if not speaker_meta[i]:
                    rem.append(i)
            
            for i in rem:
                speaker_meta.pop(i,None)
            
            speaker_uri = BASE_URL+"speakers/"+collection+"/"+speaker['uuid']
            try:
                speaker_uri = client.add_speaker(collection, speaker_meta)
                print("Added Speaker: "+speaker_uri)
            except pyalveo.APIError, e:
                if e.http_status_code==412:
                    print("Skipping Add Speaker"+speaker.get('identifier','None')+": Speaker already exists. URI: "+speaker_uri)
                    continue
            #seems like each speaker only has one item. 
            #The item will have the documents listed below (in comments)
            #Item metadata located with audio files. They are identical except for 'SourceFile'
            h2nMetadataPath = os.path.join(input_dir,speaker['ZoomH2nFiles']+'.json')
            with open(h2nMetadataPath,'r') as item_metadata_file:
                item_metadata = json.load(item_metadata_file)
                
            if DELETE: #Get rid of 'and False' when we are able to delete
                try:
                    r = client.delete_item("https://staging.alveo.edu.au/catalog/faims-test/"+item_metadata['ImageID'])
                    print("Deleting Item - Result: "+str(r))
                except pyalveo.APIError, e:
                    if e.http_status_code==404:
                        pass
            
            item_metadata.pop("SourceFile",None)
            item_metadata['dcterms:title'] = item_metadata.pop('ImageDescription','')
            item_metadata['dcterms:creator'] = item_metadata.pop('XPAuthor','')
            item_metadata['dcterms:created'] = item_metadata['Keywords'][0] #Should be date in 1st position
            item_metadata['olac:speaker'] = speaker_uri
            item_metadata['dcterms:created'] = speaker.get('createdAtGMT',None)[:10]
            
            try:
                item_uri = client.add_item(collection_uri, item_metadata['ImageID'], item_metadata)
                print("Added Item: "+item_uri)
            except pyalveo.APIError, e:
                if e.http_status_code==412:
                    item_uri = BASE_URL+"catalog/"+collection+"/"+speaker['uuid']
                    print("Skipping Add Item"+item_metadata.get('dcterms:title','None')+": Item already exists. URI: "+item_uri)
                    continue
            
            #Get photo of concent form 'PhotoOfSignedConsentForm'
            add_document(speaker['PhotoOfSignedConsentForm'], "consentform", skip_downsampled=True)
            
            #Get H2n Audio file  'ZoomH2nFiles' (only one linked)
            add_document(speaker['ZoomH2nFiles'], "h2n")
            
            #Get H6Primay Audio File 'ZoomH6PrimaryMic'
            add_document(speaker['ZoomH6PrimaryMic'], "h6primary")
            
            #Get H6External Audio File 'ZoomH6ExternalMic'
            add_document(speaker['ZoomH6ExternalMic'], "h6external",displaydoc=True)
            
            #Get H6External Audio File and upload without downsampling
            if not dont_keep_one and not skip_downsampled:
                add_document(speaker['ZoomH6ExternalMic'], "h6external_original",skip_downsampled=True)
            
            #Get Backup Recordings 'BackupRecordings' (optional default:false)
            if include_backup:
                add_document(speaker['BackupRecordings'], "backup")
            
            print("Finished Handling Speaker: "+speaker.get('identifier','None')+"  Processing Time: "+str(int(time.time()-start_time))+"s");
            
        except pyalveo.APIError, e:
            if e.http_status_code==403:
                print("Unable to ingest data: You do not have write access to the collection: "+collection)
                return 1
            #elif e.http_status_code==412:
            #    print("Skipping Speaker"+speaker.get('identifier','None')+": Speaker or Item already exists.")
            #    continue
            else:
                print("An Error Occurred: Error "+str(e.http_status_code))
                raise
    
    print("All Data has successfully been added to Alveo.")
    return 0

def downsampleAudio(file,resultName="downsampled."+DOWNSAMPLED_FORMAT, verbose=False,samplerate=DOWNSAMPLED_SAMPLERATE,bitrate=DOWNSAMPLED_BITRATE,dstFormat=DOWNSAMPLED_FORMAT):
    ''' Will convent an audio file to mp3 and downsample it to 16bit. File must be the full directory. '''
    stdout = stderr = None
    if verbose:
        stdout = stderr = sys.stdout
        
    dir = file[:file.rfind(os.sep)]
    #convert file type
    process = subprocess.Popen('ffmpeg -i '+file+' -y '+' -ar '+samplerate+' -b:a '+bitrate+' '+dir+os.sep+resultName,stdout=stdout,stderr=stderr)
    
    poll = None
    while poll==None:
        poll = process.poll()
    if poll>0:
        #An error occured. Probably bad file directory, codec or ffmpeg isn't found (path not configured or ffmpeg not downloaded).
        print("Unable to downsample "+file+'''. This may have occured due to a bad path or filename, 
                the file not existing, a bad audio encoding or due to an error with ffmpeg, 
                such as an improperly configured PATH variable or you may not have ffmpeg installed.\n
                For more details about this error, please run the process again with the --verbose (-v) parameter.''')
        return False
    
    return True

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    try:
        # Setup argument parser
        parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument("-i", "--input", dest="input", help="The Root file to be Imported", metavar="path" )
        parser.add_argument("-k", "--apikey", dest="apikey", help="The API Key as Generated by Alveo. See https://app.alveo.edu.au/")
        parser.add_argument("-c", "--collection", dest="collection", help="The collection this data will be added to. You must be a data_owner in order to add documents to a collection.", metavar="path" )
        parser.add_argument("-d", "--skip-downsampled", dest="skipdownsampled", action="count", help="Skip generated a downsampled version of the audio files. This lower quality version allows researchers to sample the audio online without having to download the full sized file.\nUse This option if you don't have FFMPEG installed or setup properly." )
        parser.add_argument("-b", "--include-backup", dest="includebackup", action="count", help="Will include the backup audio files (if exists) when uploading items to Alveo." )
        parser.add_argument("-o", "--no-archive-file", dest="dontkeepone", action="count", help="Use if you don't want to upload one of the original files for archival purposes.\nHas no effect if --skip-downsampled is used." )  
        #Skip Backup file (default true)
        

        # Process arguments
        args = parser.parse_args()
        skip_downsampled = True if args.skipdownsampled else False
        include_backup = True if args.includebackup else False
        verbose = True if args.verbose else False
        dont_keep_one = True if args.dontkeepone else False
        
        return process_data(input_dir=args.input, 
                            apiKey=args.apikey, 
                            collection=args.collection, 
                            skip_downsampled = skip_downsampled,
                            include_backup = include_backup,
                            dont_keep_one = dont_keep_one, 
                            verbose=verbose)
    
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    #Uncomment for release version (so errors not displayed directly to users)
    #except Exception, e:
    #    if DEBUG or TESTRUN:
    #        raise(e)
    #    indent = len(program_name) * " "
    #    sys.stderr.write(program_name + ": " + repr(e) + "\n")
    #    sys.stderr.write(indent + "  for help use --help")
    #    return 2

if __name__ == "__main__":
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = '_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())