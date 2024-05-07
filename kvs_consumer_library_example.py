# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

'''
Example to demonstrate usage the AWS Kinesis Video Streams (KVS) Consumer Library for Python.
 '''
 
__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import os
import sys
import time
import boto3
import logging
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import KvsConsumerLibrary
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor

# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

# Update the desired region and KVS stream name.
REGION='[ENTER_REGION]'
KVS_STREAM01_NAME = '[ENTER_KVS_STREAM_NAME]'   # Stream must be in specified region


class KvsPythonConsumerExample:
    '''
    Example class to demonstrate usage the AWS Kinesis Video Streams KVS) Consumer Library for Python.
    '''

    def __init__(self):
        '''
        Initialize the KVS clients as needed. The KVS Comsumer Library intentionally does not abstract 
        the KVS clients or the various media API calls. These have individual authentication configuration and 
        a variety of other user defined settings so we keep them here in the users application logic for configurability.

        The KvsConsumerLibrary sits above these and parses responses from GetMedia and GetMediaForFragmentList 
        into MKV fragments and provides convenience functions to further process, save and extract individual frames.  
        '''

        # Create shared instance of KvsFragementProcessor
        self.kvs_fragment_processor = KvsFragementProcessor()

        # Variable to maintaun state of last good fragememt mostly for error and exception handling.
        self.last_good_fragment_tags = None

        # Init the KVS Service Client and get the accounts KVS service endpoint
        log.info('Initializing Amazon Kinesis Video client....')
        # Attach session specific configuration (such as the authentication pattern)
        self.session = boto3.Session(region_name=REGION)
        self.kvs_client = self.session.client("kinesisvideo")

    ####################################################
    # Main process loop
    def service_loop(self):
        
        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a Kinesis Video Stream

        # Get the KVS Endpoint for the GetMedia Call for this stream
        log.info(f'Getting KVS GetMedia Endpoint for stream: {KVS_STREAM01_NAME} ........') 
        get_media_endpoint = self._get_data_endpoint(KVS_STREAM01_NAME, 'GET_MEDIA')
        
        # Get the KVS Media client for the GetMedia API call
        log.info(f'Initializing KVS Media client for stream: {KVS_STREAM01_NAME}........') 
        kvs_media_client = self.session.client('kinesis-video-media', endpoint_url=get_media_endpoint)

        # Make a KVS GetMedia API call with the desired KVS stream and StartSelector type and time bounding.
        log.info(f'Requesting KVS GetMedia Response for stream: {KVS_STREAM01_NAME}........') 
        get_media_response = kvs_media_client.get_media(
            StreamName=KVS_STREAM01_NAME,
            StartSelector={
                'StartSelectorType': 'NOW'
            }
        )

        # Initialize an instance of the KvsConsumerLibrary, provide the GetMedia response and the required call-backs
        log.info(f'Starting KvsConsumerLibrary for stream: {KVS_STREAM01_NAME}........') 
        my_stream01_consumer = KvsConsumerLibrary(KVS_STREAM01_NAME, 
                                              get_media_response, 
                                              self.on_fragment_arrived, 
                                              self.on_stream_read_complete, 
                                              self.on_stream_read_exception
                                            )

        # Start the instance of KvsConsumerLibrary, any matching fragments will begin arriving in the on_fragment_arrived callback
        my_stream01_consumer.start()

        # Can create another instance of KvsConsumerLibrary on a different media stream or continue on to other application logic. 

        # Here can hold the process up by waiting for the KvsConsumerLibrary thread to finish (may never finish for live streaming fragments)
        #my_stream01_consumer.join()

        # Or 
    
        # Run a loop with the applications main functionality that holds the process open.
        # Can also use to monitor the completion of the KvsConsumerLibrary instance and trigger a required action on completion.
        while True:

            #Add Main process / application logic here while KvsConsumerLibrary instance runs as a thread
            log.info("Nothn to see, just doin main application stuff in a loop here!")
            time.sleep(5)
            
            # Call below to exit the streaming get_media() thread gracefully before reaching end of stream. 
            #my_stream01_consumer.stop_thread()


    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration):
        '''
        This is the callback for the KvsConsumerLibrary to send MKV fragments as they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw bytes and a DOM like structure containing the fragments meta data.

        With these parameters you can do a variety of post-processing including saving the fragment as a standalone MKV file
        to local disk, request individual frames as a numpy.ndarray for data science applications or as JPEG/PNG files to save to disk 
        or pass to computer vison solutions. Finally, you can also use the Fragment DOM to access Meta-Data such as the MKV tags as well
        as track ID and codec information. 

        In the below example we provide a demonstration of all of these described functions.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment. Can be save or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing searchable list of EBML elements and MetaData in the Fragment

            **fragment_receive_duration**: float
                The time in seconds that the fragment took for the streaming data to be received and processed. 
        
        '''
        
        try:
            # Log the arrival of a fragment. 
            # use stream_name to identify fragments where multiple instances of the KvsConsumerLibrary are running on different streams.
            log.info(f'\n\n##########################\nFragment Received on Stream: {stream_name}\n##########################')
            
            # Print the fragment receive and processing duration as measured by the KvsConsumerLibrary
            log.info('')
            log.info(f'####### Fragment Receive and Processing Duration: {fragment_receive_duration} Secs')

            # Get the fragment tags and save in local parameter.
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(fragment_dom)

            ##### Log Time Deltas:  local time Vs fragment SERVER and PRODUCER Timestamp:
            time_now = time.time()
            kvs_ms_behind_live = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_MILLIS_BEHIND_NOW'])
            producer_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP'])
            server_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_SERVER_TIMESTAMP'])
            
            log.info('')
            log.info('####### Timestamps and Delta: ')
            log.info(f'KVS Reported Time Behind Live {kvs_ms_behind_live} mS')
            log.info(f'Local Time Diff to Fragment Producer Timestamp: {round(((time_now - producer_timestamp)*1000), 3)} mS')
            log.info(f'Local Time Diff to Fragment Server Timestamp: {round(((time_now - server_timestamp)*1000), 3)} mS')

            ###########################################
            # 1) Extract and print the MKV Tags in the fragment
            ###########################################
            # Get the fragment MKV Tags (Meta-Data). KVS allows these to be set per fragment to convey some information 
            # about the attached frames such as location or Computer Vision labels. Here we just log them!
            log.info('')
            log.info('####### Fragment MKV Tags:')
            for key, value in self.last_good_fragment_tags.items():
                log.info(f'{key} : {value}')

            ###########################################
            # 2) Pretty Print the entire fragment DOM structure
            # ###########################################
            # Get and log the the pretty print string for entire fragment DOM structure from EBMLite parsing.
            log.info('')
            log.info('####### Pretty Print Fragment DOM: #######')
            pretty_frag_dom = self.kvs_fragment_processor.get_fragement_dom_pretty_string(fragment_dom)
            log.info(pretty_frag_dom)

            ###########################################
            # 3) Write the Fragment to disk as standalone MKV file
            ###########################################
            save_dir = 'ENTER_DIRECTORY_PATH_TO_SAVE_FRAGEMENTS'
            frag_file_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER'] + '.mkv' # Update as needed
            frag_file_path = os.path.join(save_dir, frag_file_name)
            # Uncomment below to enable this function - will take a significant amount of disk space if left running unchecked:
            #log.info('')
            #log.info(f'####### Saving fragment to local disk at: {frag_file_path}')
            #self.kvs_fragment_processor.save_fragment_as_local_mkv(fragment_bytes, frag_file_path)

            ###########################################
            # 4) Extract Frames from Fragment as ndarrays:
            ###########################################
            # Get a ratio of available frames in the fragment as a list of numpy.ndarray's
            # Here we just log the shape of each image array but ndarray lends itself to many powerful 
            # data science, computer vision and video analytic functions in particular.
            one_in_frames_ratio = 5
            log.info('')
            log.info(f'#######  Reading 1 in {one_in_frames_ratio} Frames from fragment as ndarray:')
            ndarray_frames = self.kvs_fragment_processor.get_frames_as_ndarray(fragment_bytes, one_in_frames_ratio)
            for i in range(len(ndarray_frames)):
                ndarray_frame = ndarray_frames[i]
                log.info(f'Frame-{i} Shape: {ndarray_frame.shape}')
            
            ###########################################
            # 5) Save Frames from Fragment to local disk as JPGs
            ###########################################
            # Get a ratio of available frames in the fragment and save as JPGs to local disk.
            # JPEGs could also be sent to other AWS services such as Amazon Rekognition and Amazon Sagemaker
            # for computer vision inference. 
            # Alternatively, these could be sent to Amazon S3 and used to create a timelapse set of images or 
            # further processed into timed thumbnails for the KVS media stream.
            one_in_frames_ratio = 5
            save_dir = 'ENTER_DIRECTORY_PATH_TO_SAVE_JPEG_FRAMES'
            jpg_file_base_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            jpg_file_base_path = os.path.join(save_dir, jpg_file_base_name)
            
            # Uncomment below to enable this function - will take a significant amount of disk space if left running unchecked:
            #log.info('')
            #log.info(f'####### Saving 1 in {one_in_frames_ratio} Frames from fragment as JPEG to base path: {jpg_file_base_path}')
            #jpeg_paths = self.kvs_fragment_processor.save_frames_as_jpeg(fragment_bytes, one_in_frames_ratio, jpg_file_base_path)
            #for i in range(len(jpeg_paths)):
            #    jpeg_path = jpeg_paths[i]
            #    print(f'Saved JPEG-{i} Path: {jpeg_path}')

            
            ###########################################
            # 6) Save Amazon Connect Frames from Fragment to local disk as WAVs
            ###########################################
            save_dir = 'ENTER_DIRECTORY_PATH_TO_SAVE_WAV_FRAMES'
            wav_file_base_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            wav_file_base_path = os.path.join(save_dir, wav_file_base_name)
            
            # Uncomment below to enable this function - will take a significant amount of disk space if left running unchecked:
            #log.info('')
            #log.info(f'####### Saving audio track "AUDIO_FROM_CUSTOMER" from Amazon Connect fragment as WAV to base path: {wav_file_base_path}')
            #self.kvs_fragment_processor.save_connect_fragment_audio_track_from_customer_as_wav(fragment_dom, wav_file_base_path)
            #log.info(f'####### Saving audio track "AUDIO_TO_CUSTOMER" from Amazon Connect fragment as WAV to base path: {wav_file_base_path}')
            #self.kvs_fragment_processor.save_connect_fragment_audio_track_to_customer_as_wav(fragment_dom, wav_file_base_path)


        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')
    
    def on_stream_read_complete(self, stream_name):
        '''
        This callback is triggered by the KvsConsumerLibrary when a stream has no more fragments available.
        This represents a graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the StreamSelector applied some 
        time or fragment bounding on the media request or if requesting a live steam and the producer 
        stopped sending more fragments. 

        Here you can choose to either restart reading the stream at a new time or just clean up any
        resources that were expecting to process any further fragments. 
        
        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.
        '''

        # Do something here to tell the application that reading from the stream ended gracefully.
        print(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')

    def on_stream_read_exception(self, stream_name, error):
        '''
        This callback is triggered by an exception in the KvsConsumerLibrary reading a stream. 
        
        For example, to process use the last good fragment number from self.last_good_fragment_tags to
        restart the stream from that point in time with the example stream selector provided below. 
        
        Alternatively, just handle the failed stream as per your application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **error**: err / exception
                The Exception obje tvthat was thrown to trigger this callback.

        '''

        # Can choose to restart the KvsConsumerLibrary thread at the last received fragment with below example StartSelector
        #StartSelector={
        #    'StartSelectorType': 'FRAGMENT_NUMBER',
        #    'AfterFragmentNumber': self.last_good_fragment_tags['AWS_KINESISVIDEO_CONTINUATION_TOKEN'],
        #}

        # Here we just log the error 
        print(f'####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{error}')

    ####################################################
    # KVS Helpers
    def _get_data_endpoint(self, stream_name, api_name):
        '''
        Convenience method to get the KVS client endpoint for specific API calls. 
        '''
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    '''
    Main method for example KvsConsumerLibrary
    '''
    
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()

